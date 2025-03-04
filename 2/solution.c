#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include "parser.h"


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


struct exec_result {
	int need_exit;
	int return_code;

	pid_t *bg_pids;
	size_t bg_count;
};

static struct exec_result
assemble_execution_outcome(int need_exit, int return_code, pid_t *bg_pids, size_t bg_count)
{
	struct exec_result res;
	res.need_exit = need_exit;
	res.return_code = return_code;
	res.bg_pids = bg_pids;
	res.bg_count = bg_count;
	return res;
}

static int
perform_directory_transition(const struct expr *expression)
{
    if (!expression) return 1;

    if (expression->cmd.arg_count != 1) return 1;

    const char *path = expression->cmd.args[0];
    if (!path) return 1;

    int result = chdir(path);
    return result;
}

static void
spawn_process_with_arguments(const struct expr *expression)
{
    if (!expression) return;

    size_t arg_count = expression->cmd.arg_count;
    char **arg_vector = (char **)calloc(arg_count + 2, sizeof(char *));

    if (!arg_vector) return;

    arg_vector[0] = expression->cmd.exe;
    memcpy(arg_vector + 1, expression->cmd.args, sizeof(char *) * arg_count);
    arg_vector[arg_count + 1] = NULL;

    execvp(expression->cmd.exe, arg_vector);

    free(arg_vector);
}

static int
determine_expression_is_operator(const struct expr *e)
{
    if (!e) return 0;
    return (e->type == EXPR_TYPE_AND || e->type == EXPR_TYPE_OR) ? 1 : 0;
}

static int determine_operator_expression_is_terminal(const struct expr *e)
{
    if (!e) return 0;
    return (e->next == NULL || (e->next->type == EXPR_TYPE_AND || e->next->type == EXPR_TYPE_OR)) ? 1 : 0;
}

static struct exec_result
execute_pipeline(struct expr *pipeline_start,
    const char *out_file, enum output_type out_type, int should_wait)
{
    if (!pipeline_start) {
        dprintf(STDERR_FILENO, "Invalid pipeline start\n");
        return assemble_execution_outcome(0, 1, NULL, 0);
    }

    struct process_registry process_ids;
    if (initialize_process_registry(&process_ids) != 0) {
        dprintf(STDERR_FILENO, "Memory allocation failed\n");
        return assemble_execution_outcome(0, 1, NULL, 0);
    }

    size_t command_index = 0;
    int io_descriptors[3] = {STDIN_FILENO, STDOUT_FILENO, -1};
    struct expr *current_expression = pipeline_start;

    while (current_expression && !determine_expression_is_operator(current_expression)) {
        if (current_expression->type != EXPR_TYPE_COMMAND) {
            current_expression = current_expression->next;
            continue;
        }

        if (!determine_operator_expression_is_terminal(current_expression)) {
            if (pipe(io_descriptors + 1) != 0) {
                dprintf(STDERR_FILENO, "Pipe creation error at command %zu\n", command_index);
                return assemble_execution_outcome(0, 0, NULL, 0);
            }

            int temp_descriptor = io_descriptors[1];
            io_descriptors[1] = io_descriptors[2];
            io_descriptors[2] = temp_descriptor;
        }

        if (strcmp("cd", current_expression->cmd.exe) == 0 && process_ids.size == 0 && determine_operator_expression_is_terminal(current_expression)) {
            if (perform_directory_transition(current_expression) != 0) {
                dprintf(STDERR_FILENO, "Change directory failed\n");
                release_process_registry(&process_ids);

                if (io_descriptors[0] != STDIN_FILENO) {
                    close(io_descriptors[0]);
                }

                if (io_descriptors[1] != STDOUT_FILENO) {
                    close(io_descriptors[1]);
                }

                return assemble_execution_outcome(0, -1, NULL, 0);
            }
        }
        else if (strcmp("exit", current_expression->cmd.exe) == 0) {
            if (!current_expression->next || determine_expression_is_operator(current_expression->next)) {
                int is_single_command = (process_ids.size == 0);
                pid_array_wait_and_free(&process_ids);

                if (io_descriptors[0] != STDIN_FILENO) {
                    close(io_descriptors[0]);
                }

                if (io_descriptors[1] != STDOUT_FILENO) {
                    close(io_descriptors[1]);
                }

                if (current_expression->cmd.arg_count != 0) {
                    char *parsing_end;
                    int exit_status = (int) strtol(current_expression->cmd.args[0], &parsing_end, 10);
                    return assemble_execution_outcome(is_single_command, exit_status, NULL, 0);
                }

                return assemble_execution_outcome(is_single_command, 0, NULL, 0);
            }
        }
        else {
            pid_t child_process_id = fork();

            if (child_process_id == -1) {
                dprintf(STDERR_FILENO, "Process creation failed\n");
                pid_array_wait_and_free(&process_ids);

                return assemble_execution_outcome(1, 1, NULL, 0);
            }

            if (child_process_id == 0) {
                release_process_registry(&process_ids);

                if (should_wait || process_ids.size > 0) {
                    if (dup2(io_descriptors[0], STDIN_FILENO) != STDIN_FILENO) {
                        dprintf(STDERR_FILENO, "Input redirection failed\n");
                        return assemble_execution_outcome(1, 0, NULL, 0);
                    }
                }
                else {
                    close(io_descriptors[0]);
                }

                int output_descriptor = io_descriptors[1];
                if (determine_operator_expression_is_terminal(current_expression)) {
                    if (output_descriptor != STDOUT_FILENO) {
                        close(output_descriptor);
                    }

                    if (out_type != OUTPUT_TYPE_STDOUT) {
                        output_descriptor = open(out_file,
                            O_CREAT | O_WRONLY | (out_type == OUTPUT_TYPE_FILE_NEW ? O_TRUNC : O_APPEND),
                            S_IRWXU | S_IRWXG | S_IRWXO
                        );
                        if (output_descriptor == -1) {
                            dprintf(STDERR_FILENO, "Output file error\n");
                            return assemble_execution_outcome(1, 0, NULL, 0);
                        }
                    }
                    else {
                        output_descriptor = STDOUT_FILENO;
                    }
                }

                if (dup2(output_descriptor, STDOUT_FILENO) != STDOUT_FILENO) {
                    dprintf(STDERR_FILENO, "Output redirection failed\n");
                    return assemble_execution_outcome(1, 0, NULL, 0);
                }

                if (io_descriptors[2] != -1) {
                    close(io_descriptors[2]);
                }

                spawn_process_with_arguments(current_expression);
                return assemble_execution_outcome(1, 0, NULL, 0);
            }

            if (register_process(&process_ids, child_process_id) != 0) {
                dprintf(STDERR_FILENO, "Failed to track process\n");
                break;
            }
        }

        if (io_descriptors[0] != STDIN_FILENO) {
            close(io_descriptors[0]);
        }

        if (io_descriptors[1] != STDOUT_FILENO) {
            close(io_descriptors[1]);
        }

        io_descriptors[0] = io_descriptors[2];
        current_expression = current_expression->next;
        command_index++;
    }

    if (io_descriptors[0] != STDIN_FILENO) {
        close(io_descriptors[0]);
    }

    if (should_wait) {
        return assemble_execution_outcome(0, pid_array_wait_and_free(&process_ids), NULL, 0);
    }

    return assemble_execution_outcome(0, 0, process_ids.children, process_ids.size);
}

static struct exec_result
execute_command_line(const struct command_line *line) {
    if (!line) return assemble_execution_outcome(1, 1, NULL, 0);

    struct expr *a = line->head;
    struct expr *b = a;

    while (a && !determine_expression_is_operator(a)) a = a->next;

    int c = (a == NULL);
    struct exec_result d = execute_pipeline(b, c ? line->out_file : NULL, c ? line->out_type : OUTPUT_TYPE_STDOUT, c ? !line->is_background : 1);

    if (d.need_exit) return d;

    while (a) {
        enum expr_type e = a->type;
        a = a->next;

        if ((e == EXPR_TYPE_AND && d.return_code == 0) || (e == EXPR_TYPE_OR && d.return_code != 0)) {
            b = a;
            while (a && !determine_expression_is_operator(a)) a = a->next;
            c = (a == NULL);
            d = execute_pipeline(b, c ? line->out_file : NULL, c ? line->out_type : OUTPUT_TYPE_STDOUT, c ? !line->is_background : 1);
            if (d.need_exit) return d;
        }
    }
    return d;
}

int
main(void)
{
    const size_t buffer_size = 1024;
    char data_buffer[buffer_size];
    ssize_t read_result;
    struct parser *parser_instance = parser_new();
    int final_return_code = 0;

    struct process_registry background_processes;
    if (initialize_process_registry(&background_processes) != 0) {
        dprintf(STDERR_FILENO, "Initialization failure\n");
        parser_delete(parser_instance);
        return 1;
    }

    while ((read_result = read(STDIN_FILENO, data_buffer, buffer_size)) > 0) {
        parser_feed(parser_instance, data_buffer, read_result);
        struct command_line *parsed_line = NULL;
        while (1) {
            enum parser_error parsing_error = parser_pop_next(parser_instance, &parsed_line);
            if (parsing_error == PARSER_ERR_NONE && parsed_line == NULL)
                break;
            if (parsing_error != PARSER_ERR_NONE) {
                printf("Error encountered: %d\n", (int)parsing_error);
                continue;
            }

            struct exec_result execution_result = execute_command_line(parsed_line);
            final_return_code = execution_result.return_code;
            command_line_delete(parsed_line);

            if (execution_result.bg_pids) {
                for (size_t process_index = 0; process_index < execution_result.bg_count; ++process_index) {
                    if (register_process(&background_processes, execution_result.bg_pids[process_index]) != 0) {
                        dprintf(STDERR_FILENO, "Background process tracking failed\n");
                        break;
                    }
                }

                free(execution_result.bg_pids);
            }

            if (check_completed_processes(&background_processes) != 0) {
                dprintf(STDERR_FILENO, "Background process cleanup failed\n");
            }

            if (execution_result.need_exit) {
                release_process_registry(&background_processes);
                parser_delete(parser_instance);
                return execution_result.return_code;
            }
        }
    }

    release_process_registry(&background_processes);
    parser_delete(parser_instance);
    return final_return_code;
}
