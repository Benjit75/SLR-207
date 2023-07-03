package project.src.Slave.Signals;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;

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