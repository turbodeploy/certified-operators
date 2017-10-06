package com.vmturbo.components.api.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Empty;

/**
 * An empty implementation of {@link ComponentNotificationReceiver} for use
 * by {@link ComponentApiClient} child classes that only offer a REST interface.
 *
 * <p>All methods of this class throw IllegalStateExceptions, because if you want
 * to actually use websockets for notifications you need to extend {@link ComponentNotificationReceiver}.
 */
public final class InactiveNotificationReceiver extends ComponentNotificationReceiver<Empty> {

    private static final String ERROR = "Unimplemented websocket client on a code path!";

    public InactiveNotificationReceiver(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    @Nonnull
    @Override
    protected String addWebsocketPath(@Nonnull String serverAddress) {
        throw new IllegalStateException(ERROR);
    }

    @Nonnull
    @Override
    protected Empty parseMessage(@Nonnull CodedInputStream bytes) throws IOException {
        throw new IllegalStateException(ERROR);
    }

    @Override
    protected void processMessage(@Nonnull final Empty message)
            throws ApiClientException {
        throw new IllegalStateException(ERROR);
    }
}
