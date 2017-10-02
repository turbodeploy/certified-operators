package com.vmturbo.topology.processor.api;

import com.vmturbo.components.api.client.ApiClientException;

/**
 * Exception, occurred while performing operation with topoloty processor.
 */
public class TopologyProcessorException extends ApiClientException {

    private static final long serialVersionUID = 1L;

    public TopologyProcessorException(String message) {
        super(message);
    }

    public TopologyProcessorException(String message, Throwable cause) {
        super(message, cause);
    }
}
