package project.src.Master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import project.src.Slave.Slave;
import project.src.Slave.Slave.SlaveCommand;
import project.src.Slave.Slave.SlaveStatus;
import project.src.Slave.Slave.WordCount;

public class Master implements MasterInterface {
    /* NOTE: Attributes */

    private volatile boolean statusGlobalRunning = true;
    private volatile boolean signalHandlingRunning = true;
    private volatile boolean resultHandlingRunning = true;
    private volatile boolean termination = false;

    private String inputDataFilename; // text over which we want to count the words
    private String outputResultsFilename; // text over which we want to store the results
    private String computersFilename; // list of possible computers
    private String user; // user (id) for SSH connections
    private String domain; // domain for SSH connections

    private ArrayList<String> slaves = new ArrayList<>();
    
    private HashMap<String, Boolean> slavesSplitter = new HashMap<>();
    private HashMap<String, SlaveStatus> slavesStatuses = new HashMap<>(); // map of available computers and their execution status
    private HashMap<String, Integer> result = new HashMap<>(); // map of words and their occurrences
    
    private int splitsUsed; // actual number of splits done
    private int maxMachineUsed; // max number of machines used,  actual number of machines used, >= splitsUsed, with = if only the file has enough lines in it

    private Queue<Long> commandTimestamps = new LinkedList<>(); // Queue to store command timestamps 

    private ConcurrentLinkedQueue<Object> signalsReceived = new ConcurrentLinkedQueue<>(); // queue used to store the incoming signals, to handle them one by one
    private ConcurrentLinkedQueue<Object> wordsReceived = new ConcurrentLinkedQueue<>(); // queue used to store the incoming words, to handle them one by one

    // NOTE: Constructor
    public Master(String inputDataFilename, String outputStringFilename, String computersFilename, String user, String domain, int maxMachineUsed) {
        this.inputDataFilename = inputDataFilename;
        this.outputResultsFilename = outputStringFilename;
        this.computersFilename = computersFilename;
        this.user = user;
        this.domain = domain;
        this.maxMachineUsed = maxMachineUsed;
    }

    /* NOTE: Methods */

    // NOTE: Pre-process methods

    @Override
    public void loadComputers() {
        // Implementation of loadComputers method

        System.out.println("Checking machine availability...\n");

        try (BufferedReader reader = new BufferedReader(new FileReader(this.computersFilename))) {
            String line;
            int machineCount = 0;
            while ((line = reader.readLine()) != null && machineCount < this.maxMachineUsed) {
                // Check SSH availability
                String remoteMachine = user + "@" + line + domain;
                boolean isMachineReachable = isMachineReachable(remoteMachine, "Availability");
                if (isMachineReachable) {
                    this.slavesStatuses.put(line, SlaveStatus.IDLE);
                    this.slavesSplitter.put(line, false);
                    this.slaves.add(line);
                    machineCount++;
                    System.out.println("Machine " + line + " - OK");
                } else {
                    System.out.println("Skipping " + remoteMachine + " - SSH not available");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("\nAvailable slaves selected:");
        for (String slave : this.slaves) {
            System.out.println(slave);
        }
    }

    @Override
    public boolean isMachineReachable(String remoteMachine, String actionTried) {
        //Implementation of isMachineReachable method

        long currentTime = System.currentTimeMillis();

        // Remove timestamps that are older than the time window
        while (!commandTimestamps.isEmpty() && commandTimestamps.peek() <= currentTime - TIME_WINDOW) {
            commandTimestamps.poll();
        }

        // Check if the number of commands executed within the time window exceeds the limit
        if (commandTimestamps.size() >= MAX_COMMANDS_PER_MINUTE) {
            // Calculate the amount of time to wait before executing the next command
            currentTime = System.currentTimeMillis();
            long waitTime = commandTimestamps.poll() + TIME_WINDOW - currentTime;

            if (waitTime > 0) {
                System.out.println("Command execution limit reached. Waiting for " + waitTime + " milliseconds before executing the next command.");
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        commandTimestamps.add(System.currentTimeMillis());

        String[] parts = remoteMachine.split("@");
        String hostname = parts[1];
    
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(hostname, 22), 5000);
            socket.close();
            return true;
        } catch (SocketTimeoutException e) {
            // Handle the socket timeout exception
            System.out.println("Connection to " + remoteMachine + " timed out. " + actionTried + " impossible.");
            return false;
        } catch (IOException e) {
            // Handle other IO exceptions
            System.out.println("Failed to connect to " + remoteMachine + ". " + actionTried + " impossible.");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void deleteExistingSplits() {
        // Implementation of deleteExistingSplits method
        
        File splitsFolder = new File("project/splits/");
        File[] files = splitsFolder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
        System.out.println("\nLocal folder splits/ cleaned.\n");
    }

    @Override
    public void splitInputData() {
        // Implementation of splitInputData method

        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(this.inputDataFilename))) {
                ArrayList<BufferedWriter> writers = new ArrayList<>();
                String line;
                int currWriter = 0;
                while ((line = reader.readLine()) != null) {
                    if (currWriter < this.slaves.size()) {
                        writers.add(new BufferedWriter(new FileWriter("project/splits/S" + currWriter + ".txt")));
                    }
                    BufferedWriter writer = writers.get(currWriter % this.slaves.size());
                    writer.write(line);
                    currWriter += 1;
                }

                for (BufferedWriter writer : writers) {
                    writer.close();
                }

                this.splitsUsed = writers.size();
                System.out.println(this.splitsUsed + " splits used.\n");
            }
        } catch (IOException e) {
            // Handle any exceptions that occur during file reading or writing
            System.err.println("An error occurred during file processing: " + e.getMessage());
        }
    }

    // NOTE: Deploy methods

    @Override
    public int executeCommand(ProcessBuilder processBuilder) throws IOException, InterruptedException {
        long currentTime = System.currentTimeMillis();

        // Remove timestamps that are older than the time window
        while (!commandTimestamps.isEmpty() && commandTimestamps.peek() <= currentTime - TIME_WINDOW) {
            commandTimestamps.poll();
        }

        // Check if the number of commands executed within the time window exceeds the limit
        if (commandTimestamps.size() >= MAX_COMMANDS_PER_MINUTE) {
            // Calculate the amount of time to wait before executing the next command
            currentTime = System.currentTimeMillis();
            long waitTime = commandTimestamps.poll() + TIME_WINDOW - currentTime;

            if (waitTime > 0) {
                System.out.println("Command execution limit reached. Waiting for " + waitTime + " milliseconds before executing the next command.");
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        commandTimestamps.add(System.currentTimeMillis());
        int exitCode = -1;
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        exitCode = process.waitFor();
        return exitCode;
    }

    @Override
    public void cleanSlavesFolders() {
        // Implementation of cleanSlavesFolders method

        boolean allCleanSucceeded = true;
        System.out.println("Cleaning folders /tmp/" + user + "/ on slaves...");

        for (String slave : slaves) {
            String remoteMachine = user + "@" + slave + domain;
            try {
                // Check if the folder exists on the remote machine
                ProcessBuilder checkFolderCommand = new ProcessBuilder(
                    "ssh", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine,
                    "test", "-d", "/tmp/" + user + "/"
                );
                int checkFolderExitCode = executeCommand(checkFolderCommand);

                if (checkFolderExitCode == 0) {
                    System.out.println("Folder /tmp/" + user + "/ exists on machine " + slave + " . Cleaning it...");
                    // Folder exists on the remote machine, delete its contents
                    ProcessBuilder deleteContentsCommand = new ProcessBuilder(
                        "ssh", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine,
                        "rm", "-rf", "/tmp/" + user + "/"
                    );
                    executeCommand(deleteContentsCommand);
                    System.out.println("Folder /tmp/" + user + "/ machine " + slave + " cleaned.");
                    
                } else {
                    System.out.println("Folder /tmp/" + user + "/ does not exist on machine " + slave + " . Creating it...");
                }
                ProcessBuilder createFolderCommand = new ProcessBuilder(
                        "ssh", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine,
                        "mkdir", "-p", "/tmp/" + user + "/project/splits/", "/tmp/" + user + "/project/src/Slave/"
                );
                executeCommand(createFolderCommand);
                System.out.println("Folder /tmp/" + user + "/splits/ machine " + slave + " created.");
            } catch (IOException | SecurityException | InterruptedException e) {
                System.out.println("Failed to execute clean commands on " + remoteMachine + "\n");
                e.printStackTrace();
                allCleanSucceeded = false;
                break;
            }
        }

        if (allCleanSucceeded) {
            System.out.println("Clean operation completed successfully.\n");
        }
    }

    @Override
    public void sendSplits() {
        // Implementation of sendSplits method

        System.out.println("Sending splits to slaves...");

        boolean allSendsSucceeded = true;

        int countSplits = 0;

        for (String slave : this.slaves) {
            String remoteMachine = user + "@" + slave + domain;
            String scpCommand = "scp -o \"StrictHostKeyChecking=no\" project/splits/S" + countSplits + ".txt " + remoteMachine + ":/tmp/" + user + "/project/splits/";
            ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", scpCommand);

            try {
                int sendExitCode = executeCommand(processBuilder);

                if (sendExitCode != 0) {
                    System.out.println("Failed to send split " + countSplits + " to " + slave);
                    allSendsSucceeded = false;
                } else {
                    System.out.println("Split " + countSplits + " sent to " + slave);
                }

                this.slavesSplitter.put(remoteMachine, true); // This slave needs to process a split

            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to execute the SCP command to send split " + countSplits + " to " + remoteMachine);
                e.printStackTrace();
                allSendsSucceeded = false;
                break;
            }

            if (countSplits++ == this.splitsUsed) {
                break;
            }
        }

        if (allSendsSucceeded) {
            System.out.println("Send splits operation completed successfully.\n");
        }
    }

    @Override
    public void sendSlaves() {
        // Implementation of sendSlaves method

        boolean allSendsSucceeded = true;

        for (String slave : this.slaves) {
            String remoteMachine = user + "@" + slave + domain;
            String scpCommand = "scp -o \"StrictHostKeyChecking=no\" project/src/Slave/Slave.java " + remoteMachine + ":/tmp/" + user + "/project/src/Slave/";
            ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", scpCommand);

            try {
                int sendExitCode = executeCommand(processBuilder);

                if (sendExitCode != 0) {
                    System.out.println("Failed to send Slave to " + remoteMachine);
                    allSendsSucceeded = false;
                } else {
                    System.out.println("Slave sent to " + remoteMachine);
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to execute the SCP command to send Slave to " + remoteMachine);
                e.printStackTrace();
                allSendsSucceeded = false;
                break;
            }
        }

        if (allSendsSucceeded) {
            System.out.println("Send Slaves operation completed successfully.\n");
        }
    }

    @Override
    public void compileSlaves() {
        // Implementation of compileSlaves method

        boolean allLaunchesSucceeded = true;

        for (String slave : this.slaves) {
            String remoteMachine = user + "@" + slave + domain;

            try {
                // Check connection availability
                boolean isMachineReachable = isMachineReachable(remoteMachine, "Compile");
                allLaunchesSucceeded &= isMachineReachable;

                if (!allLaunchesSucceeded) {
                    break;
                }

                // Compile Slave program on the remote machine
                ProcessBuilder compileCommand = new ProcessBuilder("ssh", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine, "javac", "/tmp/" + user + "project/src/Slave/Slave.java");
                executeCommand(compileCommand);

                System.out.println("Slave program compiled on " + remoteMachine);

            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to compile Slave.java on " + remoteMachine);
                e.printStackTrace();
                allLaunchesSucceeded = false;
                break;
            }
        }

        if (allLaunchesSucceeded) {
            System.out.println("All compiled completed successfully.\n");
        }
    }

    @Override
    public void launchSlaves() {
        // Implementation of launchSlaves method

        boolean allLaunchesSucceeded = true;

        for (String slave : this.slaves) {
            String remoteMachine = user + "@" + slave + domain;

            try {
                // Check connection availability
                boolean isMachineReachable = isMachineReachable(remoteMachine, "Launch");
                allLaunchesSucceeded &= isMachineReachable;

                if (!allLaunchesSucceeded) {
                    break;
                }

                // Launch Slave program on the remote machine
                ProcessBuilder killCommand = new ProcessBuilder("ssh", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine, "lsof", "-ti | xargs kill -9");
                executeCommand(killCommand);
                ProcessBuilder launchCommand = new ProcessBuilder("ssh", "-t", "-o", "\"StrictHostKeyChecking=no\"", remoteMachine, "\"cd /tmp/" + user + "/ ; java project/src/Slave/Slave\"");
                executeCommand(launchCommand);

                System.out.println("Slave program launched on " + remoteMachine);

            } catch (IOException | InterruptedException e) {
                System.out.println("Failed to launch Slave.java on " + remoteMachine);
                e.printStackTrace();
                allLaunchesSucceeded = false;
                break;
            }
        }

        if (allLaunchesSucceeded) {
            System.out.println("All launches completed successfully.\n");
        }
    }

    // NOTE: Handling statuses of slaves
    @Override
    public void startSignalListeningThread() {
        //Implementation of startSignalListeningThread method

        Thread listeningThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(STATUS_PORT)) {
                while (this.signalHandlingRunning) {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    try {
                        Object signal = objectInputStream.readObject();
                        this.signalsReceived.add(signal);
                    } catch (ClassNotFoundException e) {
                        System.out.println("Unknown object received.");
                        e.printStackTrace();
                    }
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        listeningThread.start();
    }

    @Override
    public void stopSignalListeningThread() {
        this.signalHandlingRunning = false;
    }

    @Override
    public void startSignalHandlingThread() {
        //Implementation of startSignalHandlingThread method

        Thread signalHandlingThread = new Thread(() -> {
            while (this.signalHandlingRunning | !this.signalsReceived.isEmpty()) {
                if (!this.signalsReceived.isEmpty()) {
                    Object signal = this.signalsReceived.poll();
                    handleSignal(signal);
                }
            }
        });
        signalHandlingThread.start();
    }

    @Override
    public void handleSignal(Object signal) {
        // Implementation of handleSignal method

        if (signal instanceof SlaveStatus) {
            SlaveStatus statusSignal = (SlaveStatus) signal;

            // Update the status of the sender in the slavesStatuses hashtable
            this.slavesStatuses.put(statusSignal.getSender(), statusSignal);
        } else {
            System.out.println("Received signal is not a StatusSignal.");
        }
    }

    @Override
    public void waitForGlobalStatus(SlaveStatus status) {
        // Implementation of waitForGlobalStatus method

        while (this.statusGlobalRunning) {
            boolean allSlavesReachedStatus = true;
            for (SlaveStatus slaveStatus : this.slavesStatuses.values()) {
                if (slaveStatus.getOrder() < status.getOrder()) {
                    allSlavesReachedStatus = false;
                    break;
                }
            }
            if (allSlavesReachedStatus) {
                System.out.println("All slaves are at least at status: " + status.toString());
                break;
            }
        }
    }

    @Override
    public void inhibitsGlobalStatusWaitingThread() {
        this.statusGlobalRunning = false;
    }

    // NOTE: Handling results of slaves

    @Override
    public void startResultListeningThread() {
        // Implementation of startResultListeningThread method

        Thread resultThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(RESULT_PORT)) {
                while (this.resultHandlingRunning) {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    try {
                        Object word = objectInputStream.readObject();
                        this.wordsReceived.add(word);
                    } catch (ClassNotFoundException e) {
                        System.out.println("Unknown object received.");
                        e.printStackTrace();
                    }
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        resultThread.start();
    }

    @Override
    public void stopResultListeningThread() {
        this.resultHandlingRunning = false;
    }

    @Override
    public void startResultHandlingThread() {
        // Implementation of startResultHandlingThread method

        Thread resultHandlingThread = new Thread(() -> {
            try (BufferedWriter resultsWriter = new BufferedWriter(new FileWriter(this.outputResultsFilename))){
                while (this.resultHandlingRunning | !this.wordsReceived.isEmpty()) {
                    if (!this.wordsReceived.isEmpty()) {
                        Object result = this.wordsReceived.poll();
                        handleResult(result, resultsWriter);
                    }
                }
                this.termination = true;
            } catch (IOException e){
                e.printStackTrace();
            }
        });
        resultHandlingThread.start();
    }

    @Override
    public void handleResult(Object signal, BufferedWriter resultsWriter) {
        // Implementation of handleResult method

        if (signal instanceof WordCount) {
            WordCount wordCount = (WordCount) signal;

            // Add the word to the results map
            result.put(wordCount.getWord(), wordCount.getCount());
            writeResult(wordCount, resultsWriter);
        } else {
            System.out.println("Received signal is not a WordCount.");
        }
    }

    @Override
    public void writeResult(WordCount wordCount, BufferedWriter resultWriter) {
        // Implementation of writeResult method

        try {
            resultWriter.write(wordCount.getWord() + " : " + wordCount.getCount());
            resultWriter.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void waitForTermination() {
        // Implementation of waitForTermination method

        while (!this.termination) {
        }
    }

    // NOTE: Setting-up the connection with all Slaves

    @Override
    public void setUpConnections() {
        // Implementing the setUpConnections method

        for (String slave : this.slaves){
            System.out.println("Connecting with " + slave);
            waitForSocketOpen(slave);
        }

        for (String slave : this.slaves){
            sendMasterInfo(slave);
        }
        waitForGlobalStatus(SlaveStatus.MASTER_INFO_RECEIVED);

        for (String slave : this.slaves){
            sendSlaveInfo(slave);
        }
        waitForGlobalStatus(SlaveStatus.MY_INFO_RECEIVED);

        for (String slave : this.slaves){
            sendSlavesList(slave);
        }
        waitForGlobalStatus(SlaveStatus.SLAVES_INFO_RECEIVED);
    }

    @Override
    public InetAddress getOwnAddress() {
        // Implementation of getOwnAddress method

        try {
            InetAddress localhost = InetAddress.getLocalHost();
            return localhost;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InetAddress resolveSlaveHostname(String slave) {
        // Implementation of resolveHostname method
        
        try {
            InetAddress ipAddress = InetAddress.getByName(slave + this.domain);
            return ipAddress;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void waitForSocketOpen(String slave){
        boolean socketOpen = false;
        InetAddress host = resolveSlaveHostname(slave);

        while (!socketOpen) {
            try {
                Socket socket = new Socket(host.getHostAddress(), Slave.STATUS_PORT);
                socketOpen = true;
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void sendMasterInfo(String slave) {
        // Implementation of sendMasterInfo method

        SlaveCommand command = SlaveCommand.MASTER_INFO;
        command.setParameters(getOwnAddress(), STATUS_PORT);

        sendCommand(slave, command);
    }

    @Override
    public void sendSlaveInfo(String slave) {
        // Implementation of sendSlaveInfo method

        SlaveCommand command = SlaveCommand.YOUR_INFO;
        command.setParameters(slave, this.slavesSplitter.get(slave), domain);

        sendCommand(slave, command);
    }

    @Override
    public void sendSlavesList(String slave) {
        // Implementation of sendSlavesList method

        SlaveCommand command = SlaveCommand.SLAVES_LIST;
        command.setParameters(slaves);

        sendCommand(slave, command);
    }

    @Override
    public void sendCommand(String slave, SlaveCommand command) {
        // Implementation of sendCommand method

        InetAddress ipAddress = resolveSlaveHostname(slave);
        System.out.println(ipAddress.getHostAddress());
        try {
            Socket socket = new Socket(ipAddress.getHostAddress(), Slave.STATUS_PORT);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(command);
            outputStream.flush();

            // Close the output stream and socket connection
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            // Handle any IO exceptions
            System.out.println("Connection with slave " + slave + "(IP: " + ipAddress + ") refused on port " + Slave.STATUS_PORT);
            e.printStackTrace();
        }
    }

    @Override
    public void interconnectSlaves() {
        // Implementation of interconnectSlaves method

        for (String slave : this.slaves){
            sendCommand(slave, SlaveCommand.INTERCONNECT);
        }
        waitForGlobalStatus(SlaveStatus.INTERCONNECTED);
    }

    // NOTE: Map-Reduce steps

    @Override
    public void beginShuffleThread() {
        // Implementation of beginShuffleThread method

        for (String slave : this.slavesSplitter.keySet()){
            if (slavesSplitter.get(slave)){
                sendCommand(slave, SlaveCommand.SHUFFLE_ON);
            }
        }
        waitForGlobalStatus(SlaveStatus.SHUFFLE_ON);
    }

    @Override
    public void beginMap() {
        // Implementation of beginMap method

        for (String slave : this.slaves){
            sendCommand(slave, SlaveCommand.MAP);
        }
        waitForGlobalStatus(SlaveStatus.WAITING_REDUCE);
    }

    @Override
    public void beginReduce() {
         // Implementation of beginReduce method

        for (String slave : this.slaves){
            sendCommand(slave, SlaveCommand.REDUCE);
        }
        waitForGlobalStatus(SlaveStatus.REDUCE_DONE);
    }

    @Override
    public void requestResults() {
        // Implementation of requestResults method

        for (String slave : this.slaves){
            sendCommand(slave, SlaveCommand.SEND_RESULTS);
        }
        waitForGlobalStatus(SlaveStatus.TERMINATED);
    }

    // NOTE: MAIN PROGRAM

    public static void main(String[] args) {
        // Entry point of the program
        String inputDataFilename = "project/input.txt";
        String outputResultsFilename = "project/results.txt";
        String computersFilename = "project/computers.txt";
        String user = "bternot-21";
        String domain = ".enst.fr";
        int maxMachineUsed = 3;

        Master master = new Master(inputDataFilename, outputResultsFilename, computersFilename, user, domain, maxMachineUsed);
        
        // Invoke the pre-processing
        master.loadComputers();
        master.deleteExistingSplits();
        master.splitInputData();
        
        // Invoke the deploy application
        master.cleanSlavesFolders();
        master.sendSplits();
        master.sendSlaves();
        master.compileSlaves();
        
        // Start the status threads handlers
        master.startSignalListeningThread();
        master.startSignalHandlingThread();

        // Launch the slaves
        master.launchSlaves();

        // Invoke the setting-up of connections //HACK: use waitforGlobalStatus inside the methods
        master.setUpConnections();
        master.interconnectSlaves();

        // Invoke the launch of computation
        master.beginShuffleThread();
        master.beginMap();
        master.beginReduce();

        // Invoke the start of results threads handlers and then request the results
        master.startResultListeningThread();
        master.startResultHandlingThread();
        master.requestResults();

        // Invoke the stop of listening of slaves statuses; handling will eventually stop because all status are already set to TERMINATE
        master.stopSignalListeningThread();
        master.inhibitsGlobalStatusWaitingThread();

        // Invoke the stop of the listening of result; handling will continue until it run out of result to handle
        master.stopResultListeningThread();
        master.waitForTermination();
    }
}

