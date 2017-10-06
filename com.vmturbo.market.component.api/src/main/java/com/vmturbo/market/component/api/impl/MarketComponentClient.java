package com.vmturbo.market.component.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * Client-side implementation of the {@link MarketComponent}.
 *
 * <p>Clients should create an instance of this class.
 */
public class MarketComponentClient
        extends ComponentApiClient<MarketComponentRestClient, MarketComponentNotificationReceiver>
        implements MarketComponent {

    public static final String WEBSOCKET_PATH = "/market-api";

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig,
                                 @Nonnull final ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }

    public static MarketComponentClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        return new MarketComponentClient(connectionConfig, executorService);
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
    @Override
    protected MarketComponentNotificationReceiver createWebsocketClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        return new MarketComponentNotificationReceiver(connectionConfig, executorService);
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        websocketClient().addActionsListener(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        websocketClient().addProjectedTopologyListener(Objects.requireNonNull(listener));
    }
}
