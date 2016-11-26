#define NBR_CLIENTS 20
#define NBR_WORKERS 10
#include <string>
#include <zmq.hpp>
#include <thread>
#include <iostream>

/// \author Sven Turowski
/// \date 26.11.2016
/// \details This class automatically receives/sends all parts of a zmq message.
/// It is possible to get all messages with the [] operator.
class ZmqMessage {
public:
    ZmqMessage() : has_error_(false) {}
    /// \param socket We need the socket to receive from.
    /// \return If the receive fails it return false. If it receives successfully return is true.
    /// \throw This function throws nothing. It's possible to get the exception with the function get_error().
    bool recv(zmq::socket_t& socket) {
        try {
            zmq::message_t msg;
            int rc = socket.recv(&msg);
            if(!rc)
                return false;
            messages_.push_back(zmq::message_t(msg.data(), msg.size()));
            // We have to check if there are more messages to receive.
            // It's important to not receive to more because we can hunger.
            while (msg.more()) {
                msg.rebuild();
                rc = socket.recv(&msg);
                if(!rc)
                    return false;
                messages_.push_back(zmq::message_t(msg.data(), msg.size()));
            }
            return true;
        }
        catch (std::exception e)
        {
            // We don't return the exception. Because not every time we got one, but receives fails and we need to
            // return a false, to know something went wrong.
            // We can get the exception with the function get_error.
            error_ = e;
            has_error_ = true;
            return false;
        }
    }
    /// \details Simple function to clean the message.
    void clear() {
        messages_.clear();
        has_error_ = false;
    }
    /// \param data The data in std::string format.
    /// \details Adds a zmq::message_t element at the beginning.
    void push_front(const std::string data) {
        messages_.insert(messages_.begin(), zmq::message_t(data.c_str(), data.size()));
    }
    /// \param data The data in std::string format.
    /// \details Adds a zmq::message_t element at the end.
    void push_back(const std::string data) {
        messages_.push_back(zmq::message_t(data.c_str(), data.size()));
    }
    /// \param id The identity in std::string format.
    /// \param with_delimiter If false no delimiter is added, default is true.
    /// \details Adds the identity and a delimiter.
    void add_envelope(const std::string id, const bool with_delimiter=true) {
        if(with_delimiter)
            push_front(""); // add delimiter
        push_front(id); // add worker id
    }
    /// \details Returns the first element. If there are no elements it causes undefined behavior.
    /// \return A zmq::message_t& element.
    zmq::message_t& front() {
        return messages_.front();
    }
    /// \return Returns false if there are 2 or less elements. Return true if successful.
    /// \details Removes the first two elements. Identity and delimiter.
    bool remove_envelope() {
        if(messages_.size() <= 2) // If we have no envelope, we only have data
            return false;
        messages_.erase(messages_.begin(), messages_.begin()+2); // erase the first two elements, identity and delimiter
        return true;
    }
    /// \param socket We need the socket to send the message.
    /// \return If send fails it return false. If it sends successfully return is true.
    /// \throw This function throws nothing. It's possible to get the exception with the function get_error().
    bool send(zmq::socket_t& socket) {
        try {
            for(int i = 0; i < messages_.size(); i++) {
                // We have to check if the message is NOT the last one, then we need to set the zmq flag ZMQ_SNDMORE.
                if(!socket.send(messages_[i], ((i+1) < messages_.size())?ZMQ_SNDMORE:0))
                    return false;
            }
            return true;
        }
        catch (std::exception e) {
            // We don't return the exception. Because not every time we got one, but receives fails and we need to
            // return a false, to know something went wrong.
            // We can get the exception with the function get_error.
            error_ = e;
            has_error_ = true;
            return false;
        }
    }
    /// \param idx Index of the element we want to get.
    /// \return A reference to the element in the vector.
    zmq::message_t& operator[] (std::size_t idx) {
        return messages_[idx];
    }
    /// \param idx Index of the element we want to get.
    /// \return A reference to the element in the vector.
    const zmq::message_t& operator[] (std::size_t idx) const{
        return messages_[idx];
    }
    /// \return The count of all messages.
    int size() {
        return static_cast<int>(messages_.size());
    }
    /// \return The real message not identities and delimiters.
    std::string get_data() {
        std::vector<zmq::message_t>::iterator iter = messages_.begin();
        std::string data;
        while(iter != messages_.end())
        {
            zmq::message_t& msg = *iter;
            if(msg.size()==0) // if we are directly on a delimiter
                iter++;
            else if(msg.size() > 0 && (iter+1)!=messages_.end() && (iter+1)->size()==0){
                // if our msg is not a delimiter but followed by one then we have an id.
                iter+=2;
            }
            else {
                data += std::string(static_cast<char*>(msg.data()), msg.size());
                iter++;
            }
        }
        return  data;
    }
    /// \return Returns the latest exception getting by the socket.
    std::exception get_error() {
        return error_;
    }
    /// \return Returns true if an exception occurs in the progress.
    bool has_error() {
        return has_error_;
    }

private:
    std::vector<zmq::message_t> messages_;
    std::exception error_;
    bool has_error_;
};


static std::string self;

void client_task()
{
    std::cout << "client thread gestart" << std::endl;
    zmq::context_t context (1);
    zmq::socket_t client (context, ZMQ_REQ);
    client.connect("ipc://"+self+"-localfe.ipc");

    while(true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(rand()%2000));
        std::string task_id = std::to_string(rand()%10000);
        //  Send request with random hex ID

        client.send(task_id.c_str(), task_id.length());

        zmq::message_t message;
        std::vector<zmq::pollitem_t> items= {
                {static_cast<void*>(client), 0, ZMQ_POLLIN, 0}
        };
        int rc = zmq::poll (&items [0], 1, 10 * 1000);

        if (rc == 0) {
            std::cout << "client raus" << std::endl;
            break;          //  Interrupted
        }

        if (items [0].revents & ZMQ_POLLIN) {
            client.recv(&message);
            //  Process task
            if (message.size() == 0)
                break;              //  Interrupted

        }else {
            return;
        }
    }
}

void worker_task (int id)
{
    std::cout << "worker thread gestart" << std::endl;
    zmq::context_t context (1);
    zmq::socket_t worker (context, ZMQ_REQ);
    std::string ident = "worker_" + self + "_" + std::to_string(id);
    worker.setsockopt( ZMQ_IDENTITY, ident.c_str(), ident.length());
    worker.connect("ipc://"+self+"-localbe.ipc");

    zmq::message_t rdy("GO", 2);
    worker.send(rdy,0);

    while(true) {
        ZmqMessage msg;
        msg.recv(worker);
        std::this_thread::sleep_for(std::chrono::milliseconds(rand()%2000));
        msg.send(worker);
        if(msg.has_error())
            break;
    }
}

int main (int argc, char *argv [])
{
    //  First argument is this broker's name
    //  Other arguments are our peers' names
    if (argc < 2) {
        std::cout << "syntax: peering3 me {you}…\n";
        return 0;
    }
    std::vector<std::thread*> client_threads;
    std::vector<std::thread*> worker_threads;

    self = argv [1];
    std::cout << "I: preparing broker at "+self+"…\n";

    //  Prepare local frontend and backend
    zmq::context_t context (1);

    zmq::socket_t localfe (context, ZMQ_ROUTER);
    localfe.bind("ipc://"+self+"-localfe.ipc");

    zmq::socket_t localbe (context, ZMQ_ROUTER);
    localbe.bind("ipc://"+self+"-localbe.ipc");

    //  Bind cloud frontend to endpoint
    zmq::socket_t cloudfe (context, ZMQ_ROUTER);
    cloudfe.setsockopt(ZMQ_IDENTITY, self.c_str(), self.length());
    cloudfe.bind("ipc://"+self+"-cloud.ipc");

    //  Connect cloud backend to all peers
    zmq::socket_t cloudbe (context, ZMQ_ROUTER);
    cloudbe.setsockopt(ZMQ_IDENTITY, self.c_str(), self.length());
    std::string peer;
    for (int argn = 2;  argn < argc; argn++) {
        peer = argv [argn];
        std::cout << "I: connecting to cloud frontend at '"+peer+"'\n";
        cloudbe.connect("ipc://"+peer+"-cloud.ipc");
    }

    //  Bind state backend to endpoint
    zmq::socket_t statebe (context, ZMQ_PUB);
    statebe.bind("ipc://"+self+"-state.ipc");

    //  Connect state frontend to all peers
    zmq::socket_t statefe (context, ZMQ_SUB);
    statefe.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    for (int argn = 2;  argn < argc; argn++) {
        peer = argv [argn];
        std::cout << "I: connecting to state backend at '"+peer+"'\n";
        statefe.connect("ipc://"+peer+"-state.ipc");
    }

    //  After binding and connecting all our sockets, we start our child
    //  tasks - workers and clients:
    int worker_nbr;
    for (worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++) {
        std::thread* worker = new std::thread(worker_task, worker_nbr);
        worker_threads.push_back(worker);
    }

    //  Start local clients
    int client_nbr;
    for (client_nbr = 0; client_nbr < NBR_CLIENTS; client_nbr++) {
        std::thread* client = new std::thread(client_task);
        client_threads.push_back(client);
    }

    //  Queue of available workers
    int local_capacity = 0;
    int cloud_capacity = 0;
    std::vector<std::string> workers;


    while (true) {
        std::vector<zmq::pollitem_t> items= {
                { static_cast<void *>(localbe), 0, ZMQ_POLLIN, 0 },
                { static_cast<void *>(statefe), 0, ZMQ_POLLIN, 0 },
                { static_cast<void *>(cloudbe), 0, ZMQ_POLLIN, 0 }
        };
        //  If we have no workers ready, wait indefinitely
        int rc = zmq::poll (&items[0], 3, local_capacity? 1 * 1000: -1);
        if (rc == -1)
            break;              //  Interrupted

        //  Track if capacity changes during this iteration
        int previous = local_capacity;
        ZmqMessage msg;

        if (items [0].revents & ZMQ_POLLIN) {
            msg.recv(localbe);
            // keep workers up to date
            workers.push_back(std::string(static_cast<char*>(msg.front().data()), msg.front().size()));
            local_capacity++;
        }
        else if (items [1].revents & ZMQ_POLLIN) {
            msg.recv(statefe);
            cloud_capacity = atoi (msg.get_data().c_str());
        }
        else if (items [2].revents & ZMQ_POLLIN) {
            msg.recv(cloudbe);
        }

        if(msg.size() > 3) {
            bool local_msg = true;
            msg.remove_envelope(); // we don't need the worker/peer envelope

            for (int argn = 2; argn < argc; argn++) {
                std::string id(static_cast<char*>(msg.front().data()), msg.front().size());
                std::string peer_id (argv [argn]);

                if (id == peer_id) {
                    msg.send(cloudfe);
                    local_msg = false;
                }
            }

            if(local_msg) { // we have a message for our local client
                msg.send(localfe);
            }
        }
        //  Route reply to cloud if it's addressed to a broker




        while (local_capacity + cloud_capacity) {
            std::vector<zmq::pollitem_t> secondary= {
                    {static_cast<void*>(localfe), 0, ZMQ_POLLIN, 0},
                    {static_cast<void*>(cloudfe), 0, ZMQ_POLLIN, 0}
            };
            if (local_capacity)
                zmq::poll (&secondary[0], 2, 0);
            else
                zmq::poll (&secondary[0], 1, 0);

            if (secondary [0].revents & ZMQ_POLLIN) {
                msg.clear(); // First clear the message
                msg.recv(localfe); // receive all parts from the socket
            } else if (secondary [1].revents & ZMQ_POLLIN) {
                msg.clear(); // First clear the message
                msg.recv(cloudfe); // receive all parts from the socket
            }
            else
                break;      //  No work, go back to primary

            if (local_capacity) {
                std::string id = *workers.begin();
                workers.erase(workers.begin());
                msg.add_envelope(id);
                msg.send(localbe);
                local_capacity--;
            }
            else {
                //  Route to random broker peer
                msg.add_envelope(argv[2]);
                msg.send(cloudbe);
            }
        }

        if (local_capacity != previous) {
            msg.clear();
            msg.push_front(self); // We stick our own identity onto the envelope
            msg.push_front(std::to_string(local_capacity)); // Broadcast new capacity
            msg.send(statebe);
        }
    }
    return 0;
}
