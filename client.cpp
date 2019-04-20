#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "HistogramCollection.h"
#include "FIFOreqchannel.h"

#include <unistd.h>
#include <iostream>
#include <string>
#include <pthread.h>
#include <exception>
#include <thread>
#include <chrono>
#include <mutex>
using namespace std;

//TODO:
// add checking for wrong filename in the data server?

class patient_thread_args{
public:
   /*put all the arguments for the patient threads */
   int num;
   int patient_num;
   BoundedBuffer* request_buffer;
   string filename;
   FIFORequestChannel* remote;

   patient_thread_args(int n = 0, int p = 0, BoundedBuffer* req = nullptr) : num(n), patient_num(p), request_buffer(req){};
};

//worker thread's info
class worker_thread_args{
public:
    BoundedBuffer* request_buffer;
    FIFORequestChannel* workerChannel;
    vector<Histogram>* hists;
    string filename;

    //hello and welcome to the world's worst constructor ever, jesus christ.
    worker_thread_args(FIFORequestChannel* _chan = nullptr, BoundedBuffer* _req = nullptr, vector<Histogram>* _hists = nullptr, string _file = "") :
    workerChannel(_chan),
    request_buffer(_req),
    hists(_hists),
    filename(_file) {
    };
};

void* data_patient_function(void* arg) {
    patient_thread_args* args = (patient_thread_args*) arg;

    //thread stuff here
    datamsg temp; //I remember reading that declaring your variables pre-loop was faster.
    double time = 0;
    //generate a datamsg for each half of that point in time in the .csv, push both to buffer.
    for (double i = 0; i < args->num; i++) {
        time = i * .004;
        temp = datamsg(args->patient_num, time, 1);
        args->request_buffer->push(&temp);
        temp = datamsg(args->patient_num, time, 2);
        args->request_buffer->push(&temp);
    }
    args->request_buffer->producer_finish(); //decrease the tracked producer count.
}

void* file_patient_function(void* arg) {
    patient_thread_args* args = (patient_thread_args*) arg; //worker version has a string in it

    //make room in the msg for the filename and null char

    //make a new filemsg
    string filename = args->filename;
    char* buffer = new char [sizeof(filemsg) + 1 + filename.length()];
    filemsg* file_req = (filemsg*) buffer;
    *file_req = filemsg(0,0); //initialize a buffer with an opening filemsg in it.

    //put filename after that and nullchar
    strcpy(buffer+sizeof(filemsg), filename.c_str());
    //write to server
    args->remote->cwrite(buffer, sizeof(filemsg) + 1 + filename.length());
    //read response
    int read_size = sizeof(__int64_t);
    char* read_out = args->remote->cread(&read_size);
    __int64_t file_size = *(__int64_t*) read_out;

    //calculate # of passes needed and amt needed on last pass.
    int leftover = file_size % args->num;
    file_size -= leftover;
    __int64_t iterations = file_size / args->num;

    __int64_t total_read = 0;
    for (; total_read < file_size; total_read += args->num) {
      //generate file messages in here.
        filemsg file_req = filemsg(total_read, args->num);
        args->request_buffer->push(&file_req);
    }

    file_req = new filemsg(total_read, args->num);
    file_req->length = leftover;
    file_req->offset = total_read;
    args->request_buffer->push(file_req);
    args->request_buffer->producer_finish(); //decrease the tracked producer count.
}

void* data_worker_function(void* arg) {
    //pull from buffer, send to server, put response in a file or something
    //update histogram? idk
    worker_thread_args* args = (worker_thread_args*) arg;

    //create new channel for this thread
    char* read_out;
    int read_size = 30;
    MESSAGE_TYPE type = NEWCHANNEL_MSG;
    args->workerChannel->cwrite((char*) &type, sizeof(int));
    read_out = args->workerChannel->cread(&read_size);
    string channel_name = read_out;
    FIFORequestChannel request_zone = FIFORequestChannel(channel_name, FIFORequestChannel::CLIENT_SIDE);

    //make all of our requests
    read_size = sizeof(double);
    int write_size = sizeof(datamsg);
    char* read_buffer;
    datamsg* data_request;
    while (true) {
        //if no more producers remain and the buffer is empty, stop.
        if (args->request_buffer->done) {
            break;
        }

        //get datamsg
        void* temp = (args->request_buffer->pop());

        if (temp == nullptr) {
            //this occurs if no producers remain and buffer is empty.
            //or in case of catostrophic error, I guess. That's not intended, though.
            break;
        }
        data_request = (datamsg*) temp;
        if (data_request->person == 0) {
            cout << "Requested incorrect patient: message corruption likely" << endl;
            break;
        }
        //write to server and read from it.
        request_zone.cwrite((char*) data_request, write_size);
        read_buffer = request_zone.cread(&read_size); //this might also cause problems? I couldn't say.
        args->hists->at(data_request->person-1).update(*(double*)read_buffer);
        //delete our datamsg, because I allocated them dynamically.
    }
    MESSAGE_TYPE q = QUIT_MSG;
    request_zone.cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
}

void* file_worker_function(void* arg) {
    //pull from buffer, send to server, put response in a file or something
    worker_thread_args* args = (worker_thread_args*) arg;

    //create new channel
    char* read_out;
    int read_size = 30;
    MESSAGE_TYPE type = NEWCHANNEL_MSG;
    args->workerChannel->cwrite((char*) &type, sizeof(int));
    read_out = args->workerChannel->cread(&read_size);
    string channel_name = read_out;
    FIFORequestChannel request_zone = FIFORequestChannel(channel_name, FIFORequestChannel::CLIENT_SIDE);

    // Prepare write buffer
    string filename = args->filename;
    char* buffer = new char [sizeof(filemsg) + 1 + args->filename.length()];
    filemsg* msg = (filemsg*) buffer;
    int length = sizeof(filemsg) + args->filename.length() + 1;

    //prepare read buffer
    int read_buffer_size = 256;
    read_out = new char[read_buffer_size];
    //do tasks repetitively
    while (true) {

        if (args->request_buffer->done) {
            break;
        }
        //open file; if fails, all fails
        string temp = "out/" + args->filename;
        FILE* binary_out = fopen(temp.c_str(), "rb+");
        if (binary_out == nullptr) {
            throw exception();
        }

        //set up filemsg, and buffer also.
        filemsg* tempmsg = (filemsg*) args->request_buffer->pop();
        if (tempmsg == nullptr) {
            break;
        }

        *msg = *tempmsg;
        delete tempmsg;
        strcpy(buffer+sizeof(filemsg), args->filename.c_str()); //really shouldn't be needed every time. it is though.
        fseek(binary_out, msg->offset, SEEK_SET);

        //time to exit the loop
        if (msg->length < 0) {
            //having negative length is illegal and also a sign to break
            break;
        }

        //if length requested is bigger than buffer, increase buffer size
        if (msg->length > read_buffer_size) {
            delete read_out;
            read_buffer_size = msg->length;
            read_out = new char[read_buffer_size];
        }

        //communicate with server, write locally.
        request_zone.cwrite(buffer, msg->length);
        read_out = request_zone.cread(&msg->length);
        fwrite(read_out, sizeof(char), msg->length, binary_out);

        //delete things so that we don't have memory leaks.
        fclose(binary_out);
    }
    //close request channel
    MESSAGE_TYPE q = QUIT_MSG;
    request_zone.cwrite ((char *) &q, sizeof (MESSAGE_TYPE));

    //delete heap memory
    delete buffer;
    delete read_out;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//main, input handling and stuff
int main(int argc, char *argv[])
{
    int n = 100;    //default number of requests per "patient"
    int p = 1;     // number of patients [1,15]
    int w = 1;    //default number of worker threads
    int b = 20; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the file buffer
    string f;
    srand(time_t(NULL));


    int opt;
    bool f_provided = false;

    while((opt = getopt(argc, argv, "n:p:w:b:m:f:")) != -1)
    {
        switch(opt)
        {
            case 'n':
                n = atoi(optarg);
                break;
            case 'p':
                p = atoi(optarg);
                break;
            case 'w':
                w = atoi(optarg);
                break;
            case 'b':
                b = atoi(optarg);
                break;
            case 'm':
                m = atoi(optarg);
                break;
            case 'f':
                f_provided = true;
                f = optarg;
                break;
            case '?':
                printf("unknown option: %c\n", optopt);
                break;
        }
    }
////////////////////////////////////////////////////////////////////////////////////////////////////
//Actual operations start now
    string buffer_size = to_string(m);
    int pid = fork();
    if (pid == 0){
		// modify this to pass along m
        execl ("./dataserver", buffer_size.c_str());
    } else {

    	FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);

        HistogramCollection hc;
        vector<Histogram> hists;

        //initialize histograms and histogram collectionz
        for (int i = 0; i < p; i++) {
            Histogram hist(20, -2, 2);
            hists.push_back(hist);
        }

        struct timeval start, end;
        gettimeofday (&start, 0);

        pthread_t works[w];
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //File copy
        if (f_provided) {
            //read file

            //create the file. This overwrites the old one!
            string temp = "out/" + f;
            fopen(temp.c_str(), "w");

            //create the producer thread, and give it all needed to create its requests.
            BoundedBuffer request_buffer(b);
            request_buffer.producers_remaining = 1;
            pthread_t prod;
            struct patient_thread_args pargs(m, 0, &request_buffer);
            pargs.filename = f;
            pargs.remote = chan;
            pthread_create(&prod, NULL, file_patient_function, &pargs);

            this_thread::sleep_for(std::chrono::seconds(1));
            //create worker threads.
            MESSAGE_TYPE new_channel{NEWCHANNEL_MSG};
            char* read_out;
            int read_size = 30; //maximum size for name in dataserver
            string channel_name;
            worker_thread_args work_args_array[w];
            for (int i = 0; i < w; i++) {
                //get new channel name, prepare server for new channel
                work_args_array[i] = worker_thread_args(chan, &request_buffer, &hists, f);
                pthread_create(&works[i], NULL, file_worker_function, &work_args_array[i]);
            }

            //trigger producers.
            for (int i = 0; i < p; i++) {
                request_buffer.trigger_producers();
            }

            cout << "Now attempting to join producer" << endl;
                pthread_join(prod, NULL);
            cout << "Producer joined" << endl;

            for (int i = 0; i < w; i++) {
                request_buffer.trigger_workers();
            }

            //like in data requests, we just wait a little bit after the producers are done. waiting -> getting stuck.
            this_thread::sleep_for(std::chrono::seconds(1));

            /*request_buffer.trigger_all_workers();
            cout << "Attempting to join workers" << endl;
            for (int i = 0; i < w; i++) {
                request_buffer.trigger_workers();
                pthread_join(works[i], NULL);
            }
            cout << "Worker threads joined." << endl;*/

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //Data copy
        } else {
            //transfer data points.
            BoundedBuffer request_buffer(b);
            request_buffer.producers_remaining = p;

            //initialize and start worker threads: make new channels, and all that too
            //args have to be in an array, so they can be passed in as an array without going out of scope. I think, anyway. lol
            pthread_t works[w];
            worker_thread_args work_args_array[w];
            char* read_out;
            int read_size = 30; //maximum size for name in dataserver
            string channel_name;
            FIFORequestChannel* temp;
            for (int i = 0; i < w; i++) {
                //create argument array, create thread! yay ^_^
                work_args_array[i] = worker_thread_args(chan, &request_buffer, &hists, "");
                pthread_create(&works[i], NULL, data_worker_function, &work_args_array[i]);
            }

            //initialize and start producer threads
            pthread_t prods[p];
            struct patient_thread_args pat_args_array[p]; //creating array so these don't get lost in scope
            for (int i = 0; i < p; i++) {
                pat_args_array[i] = patient_thread_args(n, i+1, &request_buffer);
                pthread_create(&prods[i], NULL, data_patient_function, &pat_args_array[i]);
            }

            cout << "Now attempting to join producers" << endl;
            for (int i = 0; i < p; i++) {
                pthread_join(prods[i], NULL);
            }
            cout << "Producers joined" << endl;

            for (int i = 0; i < p; i++) {
                request_buffer.trigger_producers();
            }

            //instead of trying to wait for all worker threads (which gets stuck often)
            //we're gonna wait for a bit after all the threads are exited (which don't get stuck much)
            //there's only buffer_size possible jobs remaining, so this.... SHOULD work okay.
            this_thread::sleep_for(std::chrono::seconds(2));

            /*request_buffer.trigger_all_workers();
            cout << "Attempting to join workers" << endl;
            for (int i = 0; i < w; i++) {
                request_buffer.trigger_workers();
                pthread_join(works[i], NULL);
                cout << "worker " << i << " joined." << endl;
            }
            cout << "Workers joined" << endl;

            */
            for (int i = 0; i < p; i++) {
                hc.add(&hists.at(i));
            }

            hc.print();
        }

        gettimeofday (&end, 0);
        int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
        int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
        cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

        MESSAGE_TYPE q = QUIT_MSG;
        chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
        cout << "All Done!!!" << endl;
        delete chan;
    }
}
