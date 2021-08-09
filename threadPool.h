#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include "osqueue.h"
#include "stdlib.h"
#include <pthread.h>
#include <stdio.h>

typedef struct thread_pool
{
    OSQueue* tasks; // A queue of tasks
    int numOfThreads; // The number of threads
    pthread_t* threadsArr; // An array of threads
    pthread_mutex_t tpLock; // A mutex
    pthread_cond_t cdv; // A condition variable
    int destroyFlag; // A flag that indicates if 'tpDestroy' was called
    int shouldWait; // A flag that indicates if the ThreadPool should wait for the tasks in the queue to finish after 'tpDestroy'
}ThreadPool;

// A struct that defines a task - a pointer to a function and a pointer to an argument to this function
typedef struct{
    void (*function)(void*);
    void* argument;
} taskInfo;

ThreadPool* tpCreate(int numOfThreads);

void tpDestroy(ThreadPool* threadPool, int shouldWaitForTasks);

int tpInsertTask(ThreadPool* threadPool, void (*computeFunc) (void *), void* param);

#endif
