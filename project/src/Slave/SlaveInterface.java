package project.src.Slave;

import project.src.Slave.Slave.SlaveStatus;
import project.src.Slave.Slave.WordCount;

public interface SlaveInterface {
    /* Attributes */
    public int STATUS_PORT = 8889;
    public int SHUFFLE_PORT = 8888;

    /* Methods */

    // Set status
    void setStatus(SlaveStatus status);

    // Handle received signals from Master
    void startCommandListeningThread(); // Start a thread that listens on port STATUS_PORT and store them in a queue
    void stopCommandListeningThread(); // Stopping the listening for new signal on STATUS_PORT
    void startCommandHandlingThread(); // Dequeue the commands one by one
    void handleCommand(Object command); // Actually handle the command
    void findSlavesIP(); // Finding the ip addresses of slaves
    void sendStatus(); // Send the status of the slave to the master via a signal

    // Handle received signals from other Slaves
    void startWordListeningThread(); // Start a thread that listens on port SHUFFLE_PORT and handle the signal
    void stopWordListeningThread(); // Stopping the listening for new signal on SHUFFLE_PORT

    // Handle map-reduce
    void startMapingThread(); // Start the Thread maping that split the words on the split, and store them in a queue
    void startShufflingThread(); // Start the thread that will dequeue the words and send them to another slave
    void startReducingThread(); // Start the thread that will dequeue the words received by other slaves
    void startSendingResult(); // Start the thread that will send the wordCounts to master
    void sendWord(String word); // Send a word to another Slave
    void sendWordCount(WordCount word); // Send the word count to the master via a result
}
