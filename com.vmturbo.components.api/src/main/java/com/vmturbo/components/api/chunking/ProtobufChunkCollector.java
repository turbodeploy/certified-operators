package com.vmturbo.components.api.chunking;

import com.google.protobuf.AbstractMessage;

/**
 * Utility to collect protobuf messages into chunks, respecting size (in bytes) and count limits.
 * In practice, we send these chunks to Kafka.
 * In the future we may also send these chunks to other brokers.
 *
 * @param <T> The protobuf messages in the chunk.
 */
public class ProtobufChunkCollector<T extends AbstractMessage> extends ChunkCollector<T> {
    /**
     * Create a new instance of the chunk collector.
     *
     * @param desiredSizeBytes The desired size of a chunk. The collector will try to size each
     *                         chunk to this amount without splitting up individual elements.
     *                         e.g. if desiredSize is 100 bytes and we get three elements of 30,
     *                         80, and 60 bytes, we will put the first two into one chunk, and the
     *                         next into another chunk.
     * @param maxSizeBytes The maximum size of a chunk.
     */
    public ProtobufChunkCollector(final int desiredSizeBytes,
                                  final int maxSizeBytes) {
        super(desiredSizeBytes, maxSizeBytes);
    }

    @Override
    public int getSerializedSize(T element) {
        return element.getSerializedSize();
    }
}

