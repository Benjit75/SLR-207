package project.src.Master;


import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;

import project.src.Slave.Slave.WordCount;
import project.src.Slave.Slave.SlaveCommand;
import project.src.Slave.Slave.SlaveStatus;

public interface MasterInterface {
    /* Attributes */
    final int STATUS_PORT = 9999; // Port used to receive slave statuses
    final int RESULT_PORT = 8888; // Port used to receive slave results
    final int MAX_COMMANDS_PER_MINUTE = 10; // Maximum number of commands allowed per minute
    final long TIME_WINDOW = 61000; // Time window in milliseconds (1 minute)


    /* Methods */

    // Inside master
    void loadComputers(); // Store all reachable machines from computersFilename into map slaveStatuses, with idle status
    void deleteExistingSplits(); // Delete splits in ./splits/
    void splitInputData(); // Split the input data into max computers.size() splits (parcouring one line after another and write it in one split text file)

    // Deploy
    int executeCommand(ProcessBuilder processBuilder) throws IOException, InterruptedException; // Executes a cmd, is used for scp and ssh
    boolean isMachineReachable(String remoteMachine, String actionTried); // Check the SSH reachability of a machine (standard timeout)
    void cleanSlavesFolders(); // Check if the folder ../$user/splits exists, if so, delete all its content; if not, create it (eventually checking if ../$user/ folder exists)
    void sendSplits(); // Send the splits, each to one different machine, using SCP (checking for failures)
    void sendSlaves(); // Send the Slave.java to all used machines, using SCP (checking for failures)
    void compileSlaves(); // Compile the Slaves.java
    void launchSlaves(); // Launch the Slave.class on each used machine (checking for failures), they will open a listening thread on port 9999 to handle master signals

    // Handling statuses of slaves
    void startSignalListeningThread(); // Start a thread that listens on port STATUS_PORT and enqueues signals received in signalsReceived
    void stopSignalListeningThread(); // Stopping the listening for new signal on STATUS_PORT
    void startSignalHandlingThread(); // Start a thread that keeps checking if signalsReceived is not empty, and if not, handle the first signal
    void handleSignal(Object signal); // Handle the signal and dequeue it; it should be a status from a slave
    void waitForGlobalStatus(SlaveStatus status); // Wait for all statuses of slaves to be at least the status in the argument
    void inhibitsGlobalStatusWaitingThread(); // Inhibits any future wait for status

    // Handling results of slaves
    void startResultListeningThread(); // Start a thread that listens on port RESULT_PORT and enqueues signals received in signalsReceived
    void stopResultListeningThread(); // Stopping the listening for new signal on RESULT_PORT
    void startResultHandlingThread(); // Start a thread that keeps checking if wordsReceived is not empty, and if not, handle the first signal
    void handleResult(Object signal, BufferedWriter resultsWriter); // Handle the signal and dequeue it; it should be a word followed by its count. Add it to the result map (key=word, value=count)
    void writeResult(WordCount wordCount, BufferedWriter resultsWriter); // Write the word with its count into the output file
    void waitForTermination(); // Wait for the handling to terminate

    // Setup connections
    InetAddress getOwnAddress(); // Get the own adress to be able to send it to slaves
    InetAddress resolveSlaveHostname(String slave); // Retrieve the ip adress from a string
    void waitForSocketOpen(String slave); // wait for a server to be open
    void sendCommand(String slave, SlaveCommand command);
    void setUpConnections(); // Send to each slave the address and port of the master, the address of the slave, and the list of used slaves
    void sendMasterInfo(String slave); // Send the info of the master (address + STATUS_PORT) to the slave
    void sendSlaveInfo(String slave); // Send the info (address + boolean splitter) of the slave to them (splitter is true if the slave has been assigned to a split, false otherwise)
    void sendSlavesList(String slave); // Send the list of slaves (addresses that are keys in slavesStatuses) to the slave
    void interconnectSlaves(); // Send the command "interconnect" to all slaves; they will handle it and create a fully connected network between all of them on another port (8888) by starting a thread that can enqueue received words

    // Steps of the map reduce
    void beginShuffleThread(); // Send the command "shuffleOn" to all slaves that have a split; they will handle it and start a thread that dequeues the words found, computes a hashcode on it, and sends it to the correct slave machine according to the hash (note that the hash has to be something quite uniform, and hashCode is not considering only small words) 
    void beginMap(); // Send to each slave that has a split the command "map"; they will handle it, detect all words in their split, and put them one by one in the words queue, ready to be dequeued by the shuffleThread
    void beginReduce(); // Send the command "reduce" to each slave; they will start another thread that dequeues the words and stores them in a map (for each word, it stores the number of occurrences as the value)
    void requestResults(); // Send the command "sendResults - RESULT_PORT" to all slaves; they will handle it and send all the words they counted one by one. After it is over, terminate itself
}
