package com.vmturbo.market.component.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Client-side implementation of the {@link MarketComponent}.
 *
 * <p>Clients should create an instance of this class.
 */
public class MarketComponentClient
        extends ComponentApiClient<MarketComponentRestClient>
        implements MarketComponent {

    public static final String WEBSOCKET_PATH = "/market-api";
    private final MarketComponentNotificationReceiver notificationReceiver;

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig,
                                 @Nonnull final ExecutorService executorService,
            @Nonnull final IMessageReceiver<MarketComponentNotification> messageReceiver) {
        super(connectionConfig);
        this.notificationReceiver =
                new MarketComponentNotificationReceiver(messageReceiver, executorService);
    }

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.notificationReceiver = null;
    }

    public static MarketComponentClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nonnull final IMessageReceiver<MarketComponentNotification> messageReceiver) {
        return new MarketComponentClient(connectionConfig, executorService, messageReceiver);
    }

    public static MarketComponentClient rpcOnly(
            @Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new MarketComponentClient(connectionConfig);
    }

    @Nonnull
    @Override
    protected MarketComponentRestClient createRestClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new MarketComponentRestClient(connectionConfig);
    }

    @Nonnull
    private MarketComponentNotificationReceiver getNotificationReceiver() {
        if (notificationReceiver == null) {
            throw new IllegalStateException("Market client is not set up to receive notifications");
        }
        return notificationReceiver;
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        getNotificationReceiver().addActionsListener(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        getNotificationReceiver().addProjectedTopologyListener(Objects.requireNonNull(listener));
    }
}
