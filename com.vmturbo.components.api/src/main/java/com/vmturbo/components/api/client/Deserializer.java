package com.vmturbo.components.api.client;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;

/**
 * Deserializer is an object used to convert input stream (protobuf-related) into a protobuf
 * message. This deserializer is very protobuf-specific.
 *
 * @param <T> type of message that is expected
 */
public interface Deserializer<T> {
    /**
     * Parses the bytes to generate the message
     *
     * @param is input stream
     * @return message
     * @throws IOException if deserialization failed.
     */
    @Nonnull
    T parseFrom(@Nonnull CodedInputStream is) throws IOException;
}
