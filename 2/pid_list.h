#pragma once

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/wait.h>

#define BG_PROC_ARR_INIT_SIZE 10
#define BG_PROC_ARR_GROW_COEFF 2

struct process_registry {
	size_t size;
	size_t capacity;
	pid_t *children;
};

static inline int
initialize_process_registry(struct process_registry *arr)
{
    if (arr == NULL) {
        return 1;
    }
    arr->size = 0;
    arr->capacity = 10;
    arr->children = (pid_t *)malloc(sizeof(pid_t) * arr->capacity);
    if (arr->children == NULL) {
        return 1;
    }

    return 0;
}

static inline void
release_process_registry(struct process_registry *arr)
{
    if (arr != NULL && arr->children != NULL) {
        size_t size = 0;
        while(arr->children[size] != 0 && size < arr->size){
            size++;
        }
        if(size > 0){
          free(arr->children);
          arr->children = NULL;
        }
    }
}

static inline int
adjust_process_registry_capacity(struct process_registry *arr)
{
    if (!arr) return 1;

    size_t target_capacity = 0;
    size_t threshold = BG_PROC_ARR_INIT_SIZE;
    size_t factor = BG_PROC_ARR_GROW_COEFF;

    if (arr->size * factor < arr->capacity && arr->size > threshold) {
        target_capacity = arr->capacity / factor;
    } else if (arr->size == arr->capacity) {
        target_capacity = arr->capacity * factor;
    }

    if (!target_capacity) return 0;

    pid_t *temp_children = (pid_t *)realloc(arr->children, sizeof(pid_t) * target_capacity);
    if (!temp_children) return 1;

    arr->children = temp_children;
    arr->capacity = target_capacity;
    return 0;
}

static inline int
check_completed_processes(struct process_registry *arr)
{
    if (!arr) return 1;

    size_t index = 0;
    while (index < arr->size) {
        if (waitpid(arr->children[index], NULL, WNOHANG) > 0) {
            arr->size--;

            if (index < arr->size) {
                memmove(&arr->children[index], &arr->children[index + 1], sizeof(pid_t) * (arr->size - index));
            }
        } else {
            index++;
        }
    }

    return adjust_process_registry_capacity(arr);
}

static inline int
pid_array_wait_and_free(struct process_registry *arr)
{
    if (!arr) return -1;

    int final_status = 0;
    size_t current_child = 0;

    while (current_child < arr->size) {
        int process_status;
        waitpid(arr->children[current_child], &process_status, 0);

        if (WIFEXITED(process_status)) {
            final_status = WEXITSTATUS(process_status);
        }
        current_child++;
    }

    release_process_registry(arr);
    return final_status;
}

static inline int
register_process(struct process_registry *arr, pid_t child)
{
    if (!arr) return 1;

    arr->children[arr->size] = child;
    arr->size++;

    return adjust_process_registry_capacity(arr);
}
