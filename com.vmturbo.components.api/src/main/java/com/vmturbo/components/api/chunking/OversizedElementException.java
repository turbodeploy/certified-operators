package com.vmturbo.components.api.chunking;

import com.vmturbo.components.api.ComponentCommunicationException;

/**
 * An exception thrown if a protobuf message exceeds the maximum request size in the
 * {@link ProtobufChunkCollector} (or the
 * {@link ProtobufChunkIterator#partition(Iterable, int, int)} method.
 */
public class OversizedElementException extends ComponentCommunicationException {
    OversizedElementException(final long elementSize, final long maxRequestSizeBytes) {
        super("Element size " + elementSize + " exceeds maximum request size " + maxRequestSizeBytes);
    }
}
