package com.vmturbo.topology.processor.cost.util;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.AbstractMessage;

/**
 * Utility class for chunking large data objects into multiple serializable Protobuf messages.
 */
public final class MessageChunker {

    private MessageChunker() {
    }

    /**
     * Group Protobuf messages into chunks of size <code>maximumChunkSize</code> or less. The
     * actual serialized size of the largest chunk may be a bit smaller than the maximum because we
     * make very few assumptions about the size of the Protobuf definition and the size of the
     * fields it contains.
     *
     * <p>Setting <code>isFieldPacked</code> to true will result in {@link
     * ProtobufUtils#MAX_VARINT_SIZE} bytes being reserved only once, rather than once for each
     * message.
     *
     * @param messages Protobuf messages to group
     * @param newBuilder supplier of the builder that will accumulate multiple messages
     * @param accumulator how to merge a message into an existing builder
     * @param isFieldPacked true if the destination Protobuf field should be treated as
     *         packed; should be false unless using >=Proto3 or the field is explicitly marked with
     *         [packed=true]
     * @param maximumChunkSize upper limit on the size of the chunk
     * @param <MessageT> input Protobuf message type
     * @param <BuilderT> typically a builder, but can be anything that accumulates messages
     * @return multiple message chunks created by grouping the input messages
     */
    public static <MessageT extends AbstractMessage, BuilderT> List<BuilderT> chunkMessages(
            final Iterable<MessageT> messages, final Supplier<BuilderT> newBuilder,
            final BiFunction<BuilderT, MessageT, BuilderT> accumulator, final boolean isFieldPacked,
            final long maximumChunkSize) throws OversizedElementException {

        final int padding = isFieldPacked ? 0 : ProtobufUtils.MAX_VARINT_SIZE;
        final int emptyChunkSize = isFieldPacked ? ProtobufUtils.MAX_VARINT_SIZE : 0;

        final ImmutableList.Builder<BuilderT> chunks = ImmutableList.builder();

        BuilderT currentChunk = newBuilder.get();
        int currentChunkSize = emptyChunkSize;

        for (final MessageT message : messages) {
            if (message.getSerializedSize() + padding > maximumChunkSize) {
                throw new OversizedElementException(message.getSerializedSize() + padding,
                        maximumChunkSize);
            }
            if (currentChunkSize + message.getSerializedSize() + padding > maximumChunkSize) {
                chunks.add(currentChunk);
                currentChunk = newBuilder.get();
                currentChunkSize = emptyChunkSize;
            }
            accumulator.apply(currentChunk, message);
            currentChunkSize += message.getSerializedSize() + padding;
        }

        if (currentChunkSize > emptyChunkSize) {
            chunks.add(currentChunk);
        }

        return chunks.build();
    }

    /**
     * Thrown during a chunking operation when a single element is too large to fit within the
     * maximum chunk size.
     */
    public static class OversizedElementException extends Exception {
        /**
         * Constructor for {@link OversizedElementException}.
         *
         * @param actualSize size of the element
         * @param maximumSize maximum size allowed
         */
        public OversizedElementException(final long actualSize, final long maximumSize) {
            super(String.format("Element of size %d exceeds maximum size %d", actualSize,
                    maximumSize));
        }
    }
}
