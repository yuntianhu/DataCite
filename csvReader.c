#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_BUFFER_SIZE 1024
#define MAX_THREADS 200

typedef struct {
    char id[100];
    char doi[100];
} Task;

void* process_row(void* arg) {
    Task* task = (Task*)arg;
    char json_string[MAX_BUFFER_SIZE];
    snprintf(json_string, sizeof(json_string), "{\"id\": \"%s\", \"doi\": \"%s\" }", task->id, task->doi);
    if(task->doi == NULL || strcmp(task->doi, "") == 0 || strcmp(task->doi, "\n") == 0){
      free(task);
      return NULL;
    }
    puts(json_string);
    free(task);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <csv_file>\n", argv[0]);
        return 1;
    }

    FILE *file = fopen(argv[1], "r");
    if (!file) {
        perror("Failed to open file");
        return 1;
    }

    char buffer[MAX_BUFFER_SIZE];
    pthread_t threads[MAX_THREADS];
    int thread_count = 0;

    while (fgets(buffer, MAX_BUFFER_SIZE, file)) {
        Task *task = malloc(sizeof(Task));
        if (sscanf(buffer, "%99[^,],%99[^\n]", task->id, task->doi) == 2) {

            if (pthread_create(&threads[thread_count], NULL, process_row, task) != 0) {
                perror("Failed to create thread");
                return 1;
            }
            thread_count++;
            if (thread_count >= MAX_THREADS) {
                for (int i = 0; i < thread_count; i++) {
                    pthread_join(threads[i], NULL);
                }
                thread_count = 0;
            }
        } else {
            free(task);
        }
    }


    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    fclose(file);
    return 0;
}
