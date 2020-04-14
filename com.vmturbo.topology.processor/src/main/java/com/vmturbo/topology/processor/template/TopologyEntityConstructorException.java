package com.vmturbo.topology.processor.template;

/**
 * General exception for TopologyEntityConstructors.
 *
 */
public class TopologyEntityConstructorException extends Exception {

    private static final long serialVersionUID = -5755026202217187911L;

    /**
     * Constructs a new exception with the specified detail message.
     * @param message message
     */
    public TopologyEntityConstructorException(String message) {
        super(message);
    }
}
