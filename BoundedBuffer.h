#ifndef BoundedBuffer_h
#define BoundedBuffer_h

#include <stdio.h>
#include <queue>
#include <string>
#include <pthread.h>
#include "common.h"
#include <iostream>

using namespace std;

class BoundedBuffer
{
private:
  pthread_mutex_t tm, out, in;
  pthread_cond_t pat_done, work_done;
  // have a buffer for both filemsgs and datamsg
  queue<vector<char>> buffer;
  int cap;
  MESSAGE_TYPE config;

public:
    bool done = false;
    int producers_remaining;
	BoundedBuffer(int _cap){
        cap = _cap;
        //buffer = string[][];
        pthread_mutex_init(&tm, NULL);
        pthread_cond_init(&pat_done, NULL);
        pthread_cond_init(&work_done, NULL);
    }

	~BoundedBuffer(){
        pthread_mutex_destroy(&tm);
        pthread_cond_destroy(&pat_done);
        pthread_cond_destroy(&work_done);
	}

	void push(void* _push){
        pthread_mutex_lock(&tm);
        while (buffer.size() == cap) {
            pthread_cond_wait(&work_done, &tm);
        }

        //push to the buffer that array, using terrible iterator tricksies.
        buffer.push(vector<char>((char*) _push, (char*) _push + 24)); //filemsg and datamsg are both 24 bytes
        //cout << "Pushed: " << buffer.size() << endl;

        if (buffer.size() > 3) {
            pthread_cond_signal(&pat_done);
        }

        //send signal to whoever.
        pthread_mutex_unlock(&tm);
	}

	void* pop(){
        pthread_mutex_lock(&tm);
        while (buffer.size() == 0 && producers_remaining > 0) {
            pthread_cond_wait(&pat_done, &tm);
        }

        if (done) {
            return nullptr;
        }

        //pop that stuff right on out. Using front() cuz pop() doesn't actually return anything.
        vector<char> msg = buffer.front();
        char* out = new char[32];
        memcpy(out, msg.data(), 32);

        buffer.pop();
        //cout << "Popped: " << buffer.size()  << endl;

        pthread_mutex_unlock(&tm);
        if (buffer.size() == 0 && producers_remaining == 0) {
            done = true;
            pthread_cond_broadcast(&pat_done);
        }

        if (buffer.size() < cap - 3 || buffer.size() < 3) {
            pthread_cond_signal(&work_done);
        }
        return (void*) out;
	}

    int size() {
        int number = buffer.size();
        return number;
    }

    MESSAGE_TYPE type() {
        return config;
    }

    void trigger_producers() {
        pthread_cond_signal(&work_done);
    }

    void trigger_workers() {
        pthread_cond_signal(&pat_done);
    }

    void trigger_all_workers() {
        pthread_cond_broadcast(&pat_done);
    }

    void producer_finish() {
        pthread_mutex_lock(&tm);
        producers_remaining -= 1;
        pthread_mutex_unlock(&tm);
    }
};

#endif /* BoundedBuffer_ */
