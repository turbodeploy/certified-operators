package com.vmturbo.market.component.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * Client-side implementation of the {@link MarketComponent}.
 *
 * <p>Clients should create an instance of this class.
 */
public class MarketComponentClient
        extends ComponentApiClient<MarketComponentRestClient>
        implements MarketComponent {

    public static final String PROJECTED_TOPOLOGIES_TOPIC = "projected-topologies";
    public static final String ACTION_PLANS_TOPIC = "action-plans";

    private final MarketComponentNotificationReceiver notificationReceiver;

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig,
                                 @Nonnull final ExecutorService executorService,
            @Nullable final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nullable final IMessageReceiver<ActionPlan> actionPlanReceiver) {
        super(connectionConfig);
        this.notificationReceiver =
                new MarketComponentNotificationReceiver(projectedTopologyReceiver, actionPlanReceiver, executorService);
    }

    private MarketComponentClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.notificationReceiver = null;
    }

    public static MarketComponentClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nullable final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nullable final IMessageReceiver<ActionPlan> actionPlanReceiver) {
        return new MarketComponentClient(connectionConfig, executorService, projectedTopologyReceiver, actionPlanReceiver);
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
