package com.vmturbo.extractor.models;

import java.io.IOException;

/**
 * Signals that an exception has occurred while writing records.
 */
public class RecordWriteException extends IOException {
    /**
     * Constructs a RecordWriteException with the specified detail message and cause.
     *
     * @param description description of the exception.
     * @param e exception causing this exception.
     */
    public RecordWriteException(String description, Throwable e) {
        super(description, e);
    }

}
