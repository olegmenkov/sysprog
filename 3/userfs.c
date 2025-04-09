#include "userfs.h"

#include <string.h>
#include <stdlib.h>

#define CAPACITY_MULTIPLIER 2
#define DESCRIPTOR_POOL_START_SIZE 10


enum {
	BLOCK_SIZE = 4096,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char *memory;
	/** How many bytes are occupied. */
	int occupied;
	/** Next block in the file. */
	struct block *next;
	/** Previous block in the file. */
	struct block *prev;

	/* PUT HERE OTHER MEMBERS */
};

struct file {
	/** Double-linked list of file blocks. */
	struct block *block_list;
	/**
	 * Last block in the list above for fast access to the end
	 * of file.
	 */
	struct block *last_block;
	/** How many file descriptors are opened on the file. */
	int refs;
	/** File name. */
	char *name;
	/** Files are stored in a double-linked list. */
	struct file *next;
	struct file *prev;

	int is_removed;
};

/** List of all files. */
static struct file *file_list = NULL;

struct filedesc {
	struct file *file;

	int curr_data_segment;
	enum open_flags flags;
	int byte_pos;
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static struct filedesc **file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

static enum ufs_error_code
fd_setup(void)
{
    file_descriptors = (struct filedesc **)malloc(DESCRIPTOR_POOL_START_SIZE * sizeof(void *));
    if (!file_descriptors) return UFS_ERR_NO_MEM;
    
    memset(file_descriptors, 0, DESCRIPTOR_POOL_START_SIZE * sizeof(void *));
    file_descriptor_count = 0;
    file_descriptor_capacity = DESCRIPTOR_POOL_START_SIZE;
    
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code
adjust_container_capacity()
{
    int target_capacity = file_descriptor_capacity;
    const int growth_threshold = file_descriptor_capacity;
    const int shrink_threshold = file_descriptor_capacity / CAPACITY_MULTIPLIER;

    if (file_descriptor_count >= growth_threshold) {
        target_capacity = file_descriptor_capacity * CAPACITY_MULTIPLIER;
    } else if (file_descriptor_count < shrink_threshold && 
              file_descriptor_capacity > DESCRIPTOR_POOL_START_SIZE) {
        target_capacity = file_descriptor_capacity / CAPACITY_MULTIPLIER;
    }

    if (target_capacity == file_descriptor_capacity) {
        return UFS_ERR_NO_ERR;
    }

    struct filedesc **new_container = (struct filedesc **)realloc(
        file_descriptors,
        target_capacity * sizeof(struct filedesc *)
    );

    if (!new_container) {
        return UFS_ERR_NO_MEM;
    }

    file_descriptors = new_container;
    
    if (target_capacity > file_descriptor_capacity) {
        for (int i = file_descriptor_capacity; i < target_capacity; ++i) {
            file_descriptors[i] = NULL;
        }
    }

    file_descriptor_capacity = target_capacity;
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code
expand_storage_unit(struct file *storage_entity) 
{
    struct block *new_data_cell = (struct block*)malloc(sizeof(struct block));
    if (new_data_cell == NULL) return UFS_ERR_NO_MEM;

    new_data_cell->memory = (char*)calloc(BLOCK_SIZE, sizeof(char));
    if (new_data_cell->memory == NULL) {
        free(new_data_cell);
        return UFS_ERR_NO_MEM;
    }

    new_data_cell->occupied = 0;
    new_data_cell->next = NULL;
    new_data_cell->prev = NULL;

    if (storage_entity->block_list == NULL) {
        storage_entity->block_list = new_data_cell;
        storage_entity->last_block = new_data_cell;
    } else {
        new_data_cell->prev = storage_entity->last_block;
        storage_entity->last_block->next = new_data_cell;
        storage_entity->last_block = new_data_cell;
    }

    return UFS_ERR_NO_ERR;
}

static void
release_memory_chain(struct block *memory_chain_head)
{
    struct block *current_segment = memory_chain_head;
    
    while (current_segment != NULL) {
        struct block *next_segment = current_segment->next;
        
        if (current_segment->memory != NULL) {
            free(current_segment->memory);
            current_segment->memory = NULL;
        }
        
        free(current_segment);
        current_segment = next_segment;
    }
}

static struct file *
mkfile(const char *fname)
{
    struct file *new_entry = (struct file*)malloc(sizeof(struct file));
    if (!new_entry) {
        return NULL;
    }
    
    memset(new_entry, 0, sizeof(struct file));

    new_entry->name = strdup(fname);
    if (!new_entry->name) {
        free(new_entry);
        return NULL;
    }

    if (expand_storage_unit(new_entry) != UFS_ERR_NO_ERR) {
        free(new_entry->name);
        free(new_entry);
        return NULL;
    }

    new_entry->next = file_list;
    if (file_list) {
        file_list->prev = new_entry;
    }

    file_list = new_entry;

    return new_entry;
}


static void
rm(struct file *file)
{
    if (!file) return;

    struct file *prev_file = file->prev;
    struct file *next_file = file->next;

    if (prev_file) {
        prev_file->next = next_file;
    }

    if (next_file) {
        next_file->prev = prev_file;
    }

    if (file_list == file) {
        file_list = next_file;
    }

    if (file->block_list) {
        release_memory_chain(file->block_list);
    }

    if (file->name) {
        free(file->name);
    }

    free(file);
}

static struct file *
find(const char *filename)
{
    if (!filename || !file_list) {
        return NULL;
    }

    struct file *current = file_list;
    size_t name_length = strlen(filename);

    while (current) {
        if (current->is_removed == 0 &&
            current->name &&
            strncmp(current->name, filename, name_length) == 0 &&
            current->name[name_length] == '\0') {
            return current;
        }
        current = current->next;
    }

    return NULL;
}

static struct filedesc *
create_descriptor(struct file *file, enum open_flags flags)
{
	if (file == NULL) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return NULL;
	}

	struct filedesc *fd = calloc(1, sizeof(struct filedesc));
	if (fd == NULL) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return NULL;
	}

	fd->file = file;
	fd->flags = flags;
	fd->curr_data_segment = 0;
	fd->byte_pos = 0;

	ufs_error_code = UFS_ERR_NO_ERR;
	return fd;
}

static struct filedesc *
locate_descriptor(int descriptor_index)
{
	if (descriptor_index >= 0 && descriptor_index < file_descriptor_count) {
		return *(file_descriptors + descriptor_index);
	}
	return NULL;
}

static int
smallest_fd()
{
	if (!file_descriptors) {
		return -1;
	}

	for (int fd = 0; fd < file_descriptor_capacity; ++fd) {
		if (!file_descriptors[fd]) {
			return fd;
		}
	}

	if (file_descriptor_capacity == file_descriptor_count) {
		if ((ufs_error_code = adjust_container_capacity()) != UFS_ERR_NO_ERR) {
			return -1;
		}
	}

	return file_descriptor_count;
}

static int
is_readable(struct filedesc *descriptor)
{
    if (!descriptor) {
        return 0;
    }

    int access_mode = descriptor->flags;

    switch (access_mode) {
        case 0:
        case UFS_CREATE:
        case UFS_READ_ONLY:
        case UFS_READ_WRITE:
            return 1;
        default:
            return 0;
    }
}

static int
is_writable(struct filedesc *desc)
{
    if (!desc) {
        return 0;
    }

    int mode = desc->flags;

    if (mode == 0) {
        return 1;
    }

    switch (mode) {
        case UFS_CREATE:
        case UFS_WRITE_ONLY:
        case UFS_READ_WRITE:
            return 1;
        default:
            return 0;
    }
}

int
ufs_open(const char *filename, int flags)
{
    if (!file_descriptors) {
        if ((ufs_error_code = fd_setup()) != UFS_ERR_NO_ERR) {
            return -1;
        }
    }

    struct file *target_file = find(filename);

    if (!target_file && !(flags & UFS_CREATE)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (!target_file) {
        target_file = mkfile(filename);
        if (!target_file) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
    }

    int free_fd = smallest_fd();
    for (int i = 0; i < file_descriptor_capacity; ++i) {
        if (!file_descriptors[i]) {
            free_fd = i;
            break;
        }
    }

    if (free_fd == -1) {
        if (adjust_container_capacity() != UFS_ERR_NO_ERR) {
            return -1;
        }
        free_fd = file_descriptor_count;  
    }

    struct filedesc *descriptor = create_descriptor(target_file, flags);
    if (!descriptor) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    file_descriptors[free_fd] = descriptor;
    ++target_file->refs;

    if (free_fd == file_descriptor_count) {
        ++file_descriptor_count;
    }

    ufs_error_code = UFS_ERR_NO_ERR;
    return free_fd;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	struct filedesc *descriptor = locate_descriptor(fd);
	if (!descriptor) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	if (!is_writable(descriptor)) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}

	struct file *file = descriptor->file;
	struct block *current_block = file->block_list;
	int segment_index = 0;
	while (segment_index < descriptor->curr_data_segment) {
		current_block = current_block->next;
		++segment_index;
	}

	size_t total_size = current_block->occupied + descriptor->curr_data_segment * BLOCK_SIZE;
	if (total_size + size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}

	ssize_t total_written = 0;
	while ((size_t) total_written < size) {
		if (descriptor->byte_pos == BLOCK_SIZE) {
			current_block = current_block->next;
			if (!current_block) {
				ufs_error_code = expand_storage_unit(file);
				if (ufs_error_code != UFS_ERR_NO_ERR) {
					return total_written;
				}

				current_block = file->last_block;
			}

			descriptor->byte_pos = 0;
			++descriptor->curr_data_segment;
		}

		size_t remaining_space = BLOCK_SIZE - descriptor->byte_pos;
		if (size - total_written < remaining_space) {
			remaining_space = size - total_written;
		}

		memcpy(current_block->memory + descriptor->byte_pos, buf + total_written, remaining_space);

		descriptor->byte_pos += remaining_space;
		total_written += remaining_space;

		if (descriptor->byte_pos > current_block->occupied) {
			current_block->occupied = descriptor->byte_pos;
		}
	}

	ufs_error_code = UFS_ERR_NO_ERR;
	return total_written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	struct filedesc *descriptor = locate_descriptor(fd);
	if (!descriptor) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	if (!is_readable(descriptor)) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}

	struct block *block = descriptor->file->block_list;
	int block_iter = 0;

	while (block_iter < descriptor->curr_data_segment) {
		block = block->next;
		++block_iter;
	}

	ssize_t read_block = 0;
	while ((size_t) read_block < size) {
		if (descriptor->byte_pos == BLOCK_SIZE) {
			block = block->next;
			if (!block) {
				ufs_error_code = UFS_ERR_NO_ERR;
				return read_block;
			}

			descriptor->byte_pos = 0;
			++descriptor->curr_data_segment;
		}

		size_t bytes = block->occupied - descriptor->byte_pos;
		if (size - read_block < bytes) {
			bytes = size - read_block;
		}

		if (bytes == 0) {
			ufs_error_code = UFS_ERR_NO_ERR;
			return read_block;
		}

		memcpy(buf + read_block, block->memory + descriptor->byte_pos, bytes);

		descriptor->byte_pos += bytes;
		read_block += bytes;
	}

	ufs_error_code = UFS_ERR_NO_ERR;
	return read_block;
}

int
ufs_close(int fd)
{
    if (fd < 0 || fd >= file_descriptor_count || file_descriptors[fd] == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *file = desc->file;

    if (--file->refs == 0 && file->is_removed) {
        struct file *previous = file->prev;
        struct file *next = file->next;

        if (previous) {
            previous->next = next;
        }
        if (next) {
            next->prev = previous;
        }
        if (file == file_list) {
            file_list = next;
        }

        if (file->block_list) {
            struct block *current_block = file->block_list;
            while (current_block != NULL) {
                struct block *next_block = current_block->next;
                free(current_block->memory);
                free(current_block);
                current_block = next_block;
            }
        }

        free(file->name);
        free(file);
    }

    free(desc);
    file_descriptors[fd] = NULL;

    if (fd == file_descriptor_count - 1) {
        while (file_descriptor_count > 0 && file_descriptors[file_descriptor_count - 1] == NULL) {
            --file_descriptor_count;
        }
    }

    adjust_container_capacity();

    ufs_error_code = UFS_ERR_NO_ERR;
    return 0;
}


int
ufs_delete(const char *filename)
{
	struct file *file_to_delete = find(filename);
	if (!file_to_delete) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	if (file_to_delete->refs == 0) {
		rm(file_to_delete);
	}
	else {
		file_to_delete->is_removed = 1;
	}

	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
    struct filedesc *fd_entry = locate_descriptor(fd);
    if (!fd_entry) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (!is_writable(fd_entry)) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    if (new_size > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    struct file *target_file = fd_entry->file;
    struct block *current_block = target_file->block_list;

    size_t current_total_size = 0;
    int block_counter = 0;

    while (current_block != NULL) {
        current_total_size += current_block->occupied;

        if (current_total_size >= new_size) {
            break;
        }

        current_block = current_block->next;
        ++block_counter;
    }

    if (current_total_size > new_size) {
        release_memory_chain(current_block->next);
        target_file->last_block = current_block;
        current_block->occupied = new_size - block_counter * BLOCK_SIZE;

        for (int i = 0; i < file_descriptor_count; ++i) {
            struct filedesc *fd = file_descriptors[i];
            if (fd && fd->file == target_file && fd->curr_data_segment >= block_counter) {
                fd->curr_data_segment = block_counter;

                if (fd->byte_pos > current_block->occupied) {
                    fd->byte_pos = current_block->occupied;
                }
            }
        }
    } else {
        current_total_size += BLOCK_SIZE - current_block->occupied;
        current_block->occupied = BLOCK_SIZE;

        while (current_total_size < new_size) {
            if (expand_storage_unit(target_file) != UFS_ERR_NO_ERR) {
                return -1;
            }

            target_file->last_block->occupied = BLOCK_SIZE;
            current_total_size += BLOCK_SIZE;
            ++block_counter;
        }

        target_file->last_block->occupied = new_size - block_counter * BLOCK_SIZE;
    }

    ufs_error_code = UFS_ERR_NO_ERR;
    return 0;
}


#endif

void
ufs_destroy(void)
{
	for (int i = 0; i < file_descriptor_count; ++i) {
		free(file_descriptors[i]);
	}

	free(file_descriptors);
	file_descriptors = NULL;

	while (file_list != NULL) {
		rm(file_list);
	}
}