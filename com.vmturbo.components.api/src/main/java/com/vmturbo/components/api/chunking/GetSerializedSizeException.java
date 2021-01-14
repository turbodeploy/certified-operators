package com.vmturbo.components.api.chunking;

import com.vmturbo.components.api.ComponentCommunicationException;

/**
 * An exception thrown if the serialized size in bytes of an element in chunk can not be determined.
 */
public class GetSerializedSizeException extends ComponentCommunicationException {

    /**
     * Create a new instance.
     *
     * @param object string value to identify the object
     */
    public GetSerializedSizeException(final String object) {
        super("Unable to get serialized size in bytes for object: " + object);
    }
}
