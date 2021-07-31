package com.vmturbo.topology.processor.planexport;

/**
 * Error encountered when convertiong an actionDTO to an actionExecutionDTO.
 */
public class ActionDTOConversionException extends Exception {

    /**
     * Constructor.
     *
     * @param message error message.
     */
    public ActionDTOConversionException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message error message.
     * @param cause exception cause.
     */
    public ActionDTOConversionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
