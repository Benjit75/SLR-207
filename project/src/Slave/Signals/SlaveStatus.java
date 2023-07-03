package project.src.Slave.Signals;

import java.io.Serializable;

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