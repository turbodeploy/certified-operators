package com.vmturbo.topology.processor.probes;

/**
 * Exception thrown when probe management goes wrong.
 */
public class ProbeException extends Exception {
    public ProbeException(String message) {
        super(message);
    }

    public ProbeException(String message, Throwable cause) {
        super(message, cause);
    }
}
