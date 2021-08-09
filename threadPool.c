#include "threadPool.h"

// The function frees a ThreadPool struct
void freeThreadPool(ThreadPool *tp) {
    if (tp != NULL) {
        free(tp);
        exit(-1);
    }
}

// The function frees all the allocated memory on the heap of the ThreadPool
void freeAll(ThreadPool *tp) {
    if (tp != NULL) {
        void *data;
        // Free Tasks Queue
        if (tp->tasks != NULL) {
            // Free each task in the queue
            if (!osIsQueueEmpty(tp->tasks)) {
                data = osDequeue(tp->tasks);
                while (data != NULL) {
                    free(data);
                    data = osDequeue(tp->tasks);
                }
            }
            // Free the queue
            osDestroyQueue(tp->tasks);
        }

        // Free Threads Array
        if (tp->threadsArr != NULL)
            free(tp->threadsArr);

        // Free the ThreadPool
        free(tp);
    }
}

// The function destroys the mutex and the condition variable
void destroyConditionVariableAndMutex(pthread_mutex_t *tpLock, pthread_cond_t *cdv) {
    if (pthread_mutex_destroy(tpLock) != 0) {
        perror("Destroying mutex failed");
    }
    if (pthread_cond_destroy(cdv) != 0) {
        perror("Destroying condition variable failed");
    }
}

// The function run the tasks from the tasks queue of the ThreadPool
void *runTasks(void *tp) {
    // Convert the ThreadPool from void* to ThreadPool*
    ThreadPool *threadPool = (ThreadPool *) tp;

    int mutexFlag, cdvFlag;
    taskInfo *currentTask;

    while (1) {
        // Lock the mutex - critical section
        mutexFlag = pthread_mutex_lock(&(threadPool->tpLock));

        // Case lock failed
        if (mutexFlag != 0) {
            //Destroy condition variable and mutex
            destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
            // print error
            perror("Mutex lock failed");
            freeAll(threadPool);
            exit(-1);
        }

        // Continue waiting as long as the queue is empty and destoryFlag is 0
        while (osIsQueueEmpty(threadPool->tasks) && (!threadPool->destroyFlag)) {
            cdvFlag = pthread_cond_wait(&(threadPool->cdv), &(threadPool->tpLock));

            if (cdvFlag != 0) {
                //Destroy condition variable and mutex
                destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
                perror("Condition variable wait failed");
                freeAll(threadPool);
                exit(-1);
            }
        }

        // Wait only for the current running tasks
        if (threadPool->shouldWait == 0 && threadPool->destroyFlag) {
            // Unlock the mutex
            mutexFlag = pthread_mutex_unlock(&(threadPool->tpLock));
            if (mutexFlag != 0) {
                //Destroy condition variable and mutex
                destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
                // Case mutex unlock failed
                perror("Mutex unlock failed");
                freeAll(threadPool);
                exit(-1);
            }

            // Break in order to stop extracting tasks from the queue
            break;
        }

        // Case no tasks to run
        if (threadPool->shouldWait == 1 && osIsQueueEmpty(threadPool->tasks)) {
            // Unlock the mutex
            mutexFlag = pthread_mutex_unlock(&(threadPool->tpLock));
            if (mutexFlag != 0) {
                //Destroy condition variable and mutex
                destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
                // Case mutex unlock failed
                perror("Mutex unlock failed");
                freeAll(threadPool);
                exit(-1);
            }
            break;
        }

        //Take out task from the queue
        currentTask = (taskInfo *) osDequeue(threadPool->tasks);

        // Unlock the mutex
        mutexFlag = pthread_mutex_unlock(&(threadPool->tpLock));
        if (mutexFlag != 0) {
            //Destroy condition variable and mutex
            destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
            // Case mutex unlock failed
            perror("Mutex unlock failed");
            freeAll(threadPool);
            exit(-1);
        }

        // Run the current task
        if (currentTask->function != NULL)
            currentTask->function(currentTask->argument);

        // Free the current task after executing it
        free(currentTask);
    }
    return NULL;
}

// The functions creates a ThreadPool, allocating memory on the heap and returns a pointer to the ThreadPool
ThreadPool *tpCreate(int numOfThreads) {
    // Case no threads
    if (numOfThreads <= 0){
        perror("ThreadPool can only be initialized with at least one thread");
        exit(-1);
    }

    // Allocating a threadPool struct on the heap, and allocating the queue on the heap
    ThreadPool *tp = (ThreadPool *) malloc(sizeof(ThreadPool));

    // Case ThreadPool allocation failed
    if (tp == NULL) {
        perror("Allocation of ThreadPool failed");
        exit(-1);
    }

    // Initialize struct values
    tp->numOfThreads = numOfThreads;
    tp->destroyFlag = 0;

    // Initialize tasks queue
    tp->tasks = osCreateQueue();

    // Case tasks queue allocation failed
    if (tp->tasks == NULL) {
        perror("Allocation of Tasks Queue failed");
        freeThreadPool(tp);
    }

    // Allocate an array of N threads
    tp->threadsArr = (pthread_t *) malloc(sizeof(pthread_t) * numOfThreads);

    // Case threads array allocation failed
    if (tp->threadsArr == NULL) {
        perror("Allocation of Threads Array failed");
        // Free Tasks Queue
        osDestroyQueue(tp->tasks);
        // Free ThreadPool
        freeThreadPool(tp);
    }

    // Initialize mutex
    int mutexFlag = pthread_mutex_init(&(tp->tpLock), NULL);
    if (mutexFlag != 0) {
        perror("Mutex initialization failed");
        freeAll(tp);
        exit(-1);
    }

    // Initialize condition variable
    int cdvFlag = pthread_cond_init(&(tp->cdv), NULL);
    if (cdvFlag != 0) {
        if (pthread_mutex_destroy(&(tp->tpLock)) != 0) {
            perror("Destroying mutex failed");
        }
        perror("Condition variable initialization failed");
        freeAll(tp);
        exit(-1);
    }

    int i = 0;
    // Run tasks on threads
    for (; i < numOfThreads; i++) {
        int creationFlag = pthread_create(&(tp->threadsArr[i]), NULL, runTasks, (void *) tp);

        // An error occurred
        if (creationFlag != 0) {
            //Destroy condition variable and mutex
            destroyConditionVariableAndMutex(&(tp->tpLock), &(tp->cdv));

            //Destroy the ThreadPool
            perror("pthread_create failed");
            freeAll(tp);
            exit(-1);
        }
    }
    return tp;
}

// The function destroys and frees all the memory allocated on the heap (frees the ThreadPool)
void tpDestroy(ThreadPool *threadPool, int shouldWaitForTasks) {
    // tpDestroy has already been activated
    if (threadPool->destroyFlag) {
        return;
    }

    // Lock the mutex
    if (pthread_mutex_lock(&(threadPool->tpLock)) != 0) {
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        // print error
        perror("Mutex lock failed");
        freeAll(threadPool);
        exit(-1);
    }

    // Raise destroyFlag
    threadPool->destroyFlag = 1;
    threadPool->shouldWait = shouldWaitForTasks;

    // Wake all the threads that may be waiting on the condition variable
    if (pthread_cond_broadcast(&(threadPool->cdv)) != 0) {
        // Unlock the mutex
        if (pthread_mutex_unlock(&(threadPool->tpLock)) != 0) {
            //Destroy condition variable and mutex
            destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
            // Case mutex unlock failed
            perror("Mutex unlock failed");
            freeAll(threadPool);
            exit(-1);
        }
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        // print error
        perror("join threads failed");
        freeAll(threadPool);
        exit(-1);
    }

    // Unlock the mutex
    if (pthread_mutex_unlock(&(threadPool->tpLock)) != 0) {
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        // Case mutex unlock failed
        perror("Mutex unlock failed");
        freeAll(threadPool);
        exit(-1);
    }

    // Wait for all the other threads to finish
    int i = 0;
    for (; i < threadPool->numOfThreads; i++) {
        /*
         * Avoid deadlock
         * This can occur if the target is directly or indirectly joined to the current thread.
         */
        if(pthread_equal(pthread_self(), threadPool->threadsArr[i]) != 0){
            continue;
        }

        // Wait for all the other threads to finish
        if (pthread_join(threadPool->threadsArr[i], NULL) != 0) {
            //Destroy condition variable and mutex
            destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
            // print error
            perror("join threads failed");
            freeAll(threadPool);
            exit(-1);
        }
    }

    //Destroy condition variable and mutex
    destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
    // Free the ThreadPool
    freeAll(threadPool);
}

// The function inserts a new task to the task queue of the ThreadPool
int tpInsertTask(ThreadPool *threadPool, void (*computeFunc)(void *), void *param) {
    // Check validation of threadPool and the task
    if (threadPool == NULL || computeFunc == NULL)
        return -1;

    // Lock the mutex
    int mutexFlag = pthread_mutex_lock(&(threadPool->tpLock));

    // Case lock failed
    if (mutexFlag != 0) {
        perror("Mutex lock failed");
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        freeAll(threadPool);
        exit(-1);
    }

    // Case some thread called 'tpDestroy' - don't insert any other tasks to the queue
    if (threadPool->destroyFlag == 1) {
        return -1;
    }

    // Otherwise - lock succeed, and allocate new taskInfo
    taskInfo *task = (taskInfo *) malloc(sizeof(taskInfo));
    if (task == NULL) {
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        perror("Allocation of a new task failed");
        freeAll(threadPool);
        exit(-1);
    }

    // Add the task to the queue
    task->function = computeFunc;
    task->argument = param;
    osEnqueue(threadPool->tasks, task);

    // Signal that a new task was added to the queue
    int signalFlag = pthread_cond_signal(&(threadPool->cdv));
    if (signalFlag != 0) {
        if (task != NULL)
            free(task);
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        perror("Condition variable signal failed");
        freeAll(threadPool);
        exit(-1);
    }

    // Unlock the mutex
    mutexFlag = pthread_mutex_unlock(&(threadPool->tpLock));
    if (mutexFlag != 0) {
        if (task != NULL)
            free(task);
        //Destroy condition variable and mutex
        destroyConditionVariableAndMutex(&(threadPool->tpLock), &(threadPool->cdv));
        // Case mutex unlock failed
        perror("Mutex unlock failed");
        freeAll(threadPool);
        exit(-1);
    }

    return 0;
}


