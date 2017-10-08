package com.vmturbo.components.api.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Empty;

import com.vmturbo.communication.AbstractProtobufEndpoint;
import com.vmturbo.communication.ITransport;

/**
 * Protobuf endpoint for notification sending APIs. This endpoint does not receive anything - it
 * is only able to send data.
 *
 * @param <T> type of messages to send
 */
public class NotificationProtobufEndpoint<T extends AbstractMessage> extends
        AbstractProtobufEndpoint<T, Empty> {

    public NotificationProtobufEndpoint(@Nonnull ITransport<ByteBuffer, InputStream> transport) {
        super(transport);
    }

    @Nonnull
    @Override
    protected Empty parseFromData(CodedInputStream bytes) throws IOException {
        throw new UnsupportedOperationException("Notification endpoints do not support");
    }
}
