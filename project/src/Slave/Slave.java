package project.src.Slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Slave {
    /* NOTE: ATTRIBUTES */

    public static final int STATUS_PORT = 8889;
    public static final int SHUFFLE_PORT = 8888;

    private volatile boolean listeningCommandsRunning = true;
    private volatile boolean listeningWordsRunning = true;
    private volatile boolean mapingRunning = true;

    private boolean splitter;

    private String myAdress = null;
    private String masterIP;
    private String domain;

    private Integer masterSTATUS_PORT;
    private Integer masterRESULT_PORT;

    private ArrayList<String> slavesAdresses = new ArrayList<>();

    private HashMap<String, InetAddress> slavesIP = new HashMap<>();
    private HashMap<String, Integer> wordsCount = new HashMap<>();

    private ConcurrentLinkedQueue<Object> commandsReceived = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<String> wordsReceived = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<String> wordsSplitten = new ConcurrentLinkedQueue<>();

    private SlaveStatus status = SlaveStatus.IDLE;

    // NOTE:  Constructor

    public Slave(){};

    public void setStatus(SlaveStatus status){
        if (this.status.compareTo(status) < 0){
            this.status = status;
            this.status.setSender(this.myAdress);
        }
    }

    /* NOTE:  METHODS */
    
    // NOTE: Handling commands from Master

    public void startCommandListeningThread() {
        //Implementation of startCommandListeningThread method

        Thread commandListeningThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(STATUS_PORT)) {
                System.out.println("Listening to commands on port " + STATUS_PORT);
                while (this.listeningCommandsRunning) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println(clientSocket.getInetAddress() + " connected");
                    ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    try {
                        Object signal = objectInputStream.readObject();
                        this.commandsReceived.add(signal);
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
        commandListeningThread.start();
    }

    public void stopCommandListeningThread() {
        this.listeningCommandsRunning = false;
    }

    public void startCommandHandlingThread() {
        //Implementation of startCommandHandlingThread method

        Thread commandHandlingThread = new Thread(() -> {
            while (this.listeningCommandsRunning | !this.commandsReceived.isEmpty()) {
                if (!this.commandsReceived.isEmpty()) {
                    Object command = this.commandsReceived.poll();
                    handleCommand(command);
                }
            }
        });
        commandHandlingThread.start();
    }

    public void handleCommand(Object command){
        // Implementation of handleCommand method

        if (command instanceof SlaveCommand) {
            SlaveCommand slaveCommand = (SlaveCommand) command;
            Object[] parameters = slaveCommand.getParameters();

            switch (slaveCommand) {
                case MASTER_INFO:
                    this.masterIP = (String) parameters[0];
                    this.masterSTATUS_PORT = (Integer) parameters[1];
                    this.setStatus(SlaveStatus.MASTER_INFO_RECEIVED);
                    break;
                
                case YOUR_INFO:
                    this.myAdress = (String) parameters[0];
                    this.splitter = (boolean) parameters[1];
                    this.domain = (String) parameters[3];
                    this.setStatus(SlaveStatus.MY_INFO_RECEIVED);
                    sendStatus();
                    break;
                
                case SLAVES_LIST:
                    this.slavesAdresses = (ArrayList<String>) parameters[0]; // FIXME: cast warning
                    findSlavesIP();
                    this.setStatus(SlaveStatus.SLAVES_INFO_RECEIVED);
                    sendStatus();
                    break;

                case INTERCONNECT:
                    startWordListeningThread();
                    if (this.splitter){
                        this.setStatus(SlaveStatus.INTERCONNECTED);
                    } else {
                        this.setStatus(SlaveStatus.WAITING_REDUCE);
                    }
                    sendStatus();
                    break;

                case SHUFFLE_ON:
                    if (this.splitter){
                        startShufflingThread();
                        this.setStatus(SlaveStatus.SHUFFLE_ON);
                    } else {
                        this.setStatus(SlaveStatus.WAITING_REDUCE);
                    }
                    sendStatus();
                    break;

                case MAP:
                    if (this.splitter){
                        startMapingThread();
                        this.setStatus(SlaveStatus.MAPING);
                    } else {
                        this.setStatus(SlaveStatus.WAITING_REDUCE);
                    }
                    sendStatus();
                    break;

                case REDUCE:
                    startReducingThread();
                    this.setStatus(SlaveStatus.REDUCING);
                    sendStatus();
                    break;

                case SEND_RESULTS:
                    this.startSendingResult();
                    this.setStatus(SlaveStatus.SENDING_RESULTS);
                    sendStatus();
                    break;                    
            }

        } else {
            System.out.println("Received signal is not a SlaveCommand.");
        }
    }

    public void findSlavesIP(){
        // Implementation of findSlavesIP method
        
        try {
            for (String slave : slavesAdresses){
                this.slavesIP.put(slave, InetAddress.getByName(slave + this.domain));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void sendStatus() {
        // Implementation of sendStatus method

        try {
            Socket socket = new Socket(masterIP, this.masterSTATUS_PORT);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(this.status);
            outputStream.flush();

            // Close the output stream and socket connection
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            // Handle any IO exceptions
            e.printStackTrace();
        }
    }

    // NOTE: Handle received signals from other Slaves

    public void startWordListeningThread() {
        //Implementation of startWordListeningThread method

        Thread wordListeningThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(STATUS_PORT)) {
                while (this.listeningWordsRunning) {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    try {
                        String word = ((WordCount) objectInputStream.readObject()).getWord();
                        this.wordsReceived.add(word);
                    } catch (ClassNotFoundException | ClassCastException e) {
                        System.out.println("Unknown object received.");
                        e.printStackTrace();
                    }
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        wordListeningThread.start();
    }

    public void stopWordListeningThread() {
        this.listeningWordsRunning = false;
    }

    // NOTE: Handle map-reduce

    public void startMapingThread() {
        // Implementation of startMapingThread

        Thread mapingThread = new Thread(() -> {
            File splitsFolder = new File("./splits/");
            File[] files = splitsFolder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() & file.getName().startsWith("S")) {
                        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                for (String word : line.split(" ")){
                                    this.wordsSplitten.add(word);
                                }
                            }
                        } catch (IOException e){
                            e.printStackTrace();
                        }
                    }
                }
            }
            this.mapingRunning = false;
            this.setStatus(SlaveStatus.MAPING_DONE);
        });
        mapingThread.start();
    }

    public void startShufflingThread() {
        //Implementation of startShufflingThread method

        Thread shufflingThread = new Thread(() -> {
            while (this.mapingRunning | !this.wordsSplitten.isEmpty()) {
                if (!this.wordsSplitten.isEmpty()) {
                    String word = this.wordsSplitten.poll();
                    sendWord(word);
                }
            }
            this.setStatus(SlaveStatus.WAITING_REDUCE);
            sendStatus();
        });
        shufflingThread.start();
    }

    public void startReducingThread() {
        //Implementation of startReducingThread method

        Thread reducingThread = new Thread(() -> {
            listeningWordsRunning = false;
            while (!this.wordsSplitten.isEmpty()) {
                if (!this.wordsSplitten.isEmpty()) {
                    String word = this.wordsSplitten.poll();
                    if (this.wordsCount.containsKey(word)){
                        this.wordsCount.put(word, this.wordsCount.get(word) + 1);
                    } else {
                        this.wordsCount.put(word, 1);
                    }
                }
            }
            this.setStatus(SlaveStatus.REDUCE_DONE);
            sendStatus();
        });
        reducingThread.start();
    }

    public void startSendingResult() {
        //Implementation of startSendingResult method

        Thread sendingResultThread = new Thread(() -> {
            for (String word : this.wordsCount.keySet()) {
                sendWordCount(new WordCount(word, this.wordsCount.get(word)));
            }
            this.setStatus(SlaveStatus.TERMINATED);
            sendStatus();
        });
        sendingResultThread.start();
    }

    public void sendWord(String word) {
        // Implementation of sendWord method

        InetAddress machineIP = this.slavesIP.get(this.slavesAdresses.get(word.hashCode() & 0x7FFFFFFF));
        try {
            Socket socket = new Socket(machineIP, SHUFFLE_PORT);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(new WordCount(word));
            outputStream.flush();

            // Close the output stream and socket connection
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            // Handle any IO exceptions
            e.printStackTrace();
        }
    }

    public void sendWordCount(WordCount wordCount) {
        // Implementation of sendWordCount method

        try {
            Socket socket = new Socket(this.masterIP, this.masterRESULT_PORT);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(wordCount);
            outputStream.flush();

            // Close the output stream and socket connection
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            // Handle any IO exceptions
            e.printStackTrace();
        }
    }

    /* NOTE: MAIN PROGRAM */
    public static void main(String[] args){

        Slave slave = new Slave();
        slave.startCommandListeningThread();
        while (slave.status.getOrder() < SlaveStatus.TERMINATED.getOrder()){
        }
    }

    /* NOTE: SIGNALS */
    public class WordCount implements Serializable{

        private final int count;
        private final String word;

        public WordCount(String word, int count){
            this.word = word;
            this.count = count;
        }

        public WordCount(String word){
            this.count = 1;
            this.word = word;
        }

        public String getWord(){
            return this.word;
        }

        public int getCount(){
            return this.count;
        }
    }

    public enum SlaveStatus implements Serializable{
        IDLE(0), // The slave is idle and available for tasks
        MASTER_INFO_RECEIVED(1), // The slave has received the master's information
        MY_INFO_RECEIVED(2), // The slave has received its own information
        SLAVES_INFO_RECEIVED(3), // The slave has received the information of all other slaves
        INTERCONNECTED(4), // The slave has established connections with all other slaves
        SHUFFLE_ON(5), // The slave has started the shuffle thread process
        MAPING(6), // The slave is currently performing the maping operation
        MAPING_DONE(7), // The slave has finished the maping operation
        WAITING_REDUCE(8), // The slave is waiting for the reduce operation to start
        REDUCING(9), // The slave is currently performing the reduce operation
        REDUCE_DONE(10), // The slave has completed the reduce operation
        SENDING_RESULTS(11), // The slave is sending the results to the master
        SENDING_RESULTS_DONE(12), // The slave has completed sending the results
        TERMINATED(13); // The slave has terminated its execution

        private final int order;
        private String sender;

        SlaveStatus(int order){//, String.class sender) {
            this.order = order;
            this.sender = null;
        }

        public int getOrder() {
            return order;
        }

        public String getSender() {
            return sender;
        }

        public void setSender(String sender){
            this.sender = sender;
        }
    }

    public enum SlaveCommand implements Serializable{
        MASTER_INFO(InetAddress.class, Integer.class),    // Command received to provide master information, with "address" and "port" parameters
        YOUR_INFO(String.class, Boolean.class, String.class),    // Command received to provide slave's own information
        SLAVES_LIST(ArrayList.class),    // Command received to provide the list of available slaves

        INTERCONNECT,   // Command received to create a fully connected network between slaves

        SHUFFLE_ON, // Command received to start the shuffling process on the slave

        MAP, // Command received to initiate the mapping process on the slave

        REDUCE, // Command received to start the reduce process on the slave

        SEND_RESULTS(Integer.class);   // Command received to send the computed results to the master

        private Class<?>[] parameterTypes;
        private Object[] parameters;

        @SafeVarargs
        private SlaveCommand(Class<?>... types) {
            parameterTypes = types;
            parameters = new Object[types.length];
        }

        public void setParameters(Object... params) {
            if (params.length == parameters.length) {
                for (int i = 0; i < params.length; i++) {
                    if (params[i] != null && parameterTypes[i].isAssignableFrom(params[i].getClass())) {
                        parameters[i] = params[i];
                    } else {
                        throw new IllegalArgumentException("Invalid parameter type for " + this.name());
                    }
                }
            } else {
                throw new IllegalArgumentException("Invalid number of parameters for " + this.name());
            }
        }

        public Object[] getParameters() {
            return parameters;
        }
    }
}