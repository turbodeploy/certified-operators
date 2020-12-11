package com.vmturbo.topology.processor.communication.queues;

/**
 * Exception thrown when an attempt is made to add an element to a DiscoveryQueue to which it does
 * not belong.
 */
public class DiscoveryQueueException extends Exception {

    /**
     * Create a DiscoveryQueueException with the specified message.
     *
     * @param message the message describing the reason for the exception.
     */
    public DiscoveryQueueException(String message) {
        super(message);
    }
}
