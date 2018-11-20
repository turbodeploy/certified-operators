package com.vmturbo.api.component.communication;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification.TargetStatus;
import com.vmturbo.api.TargetNotificationDTO.TargetsNotification;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.StatsListener;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.TargetListener;

/**
 * Send a WebSocket notification to UI when backend is "ready" by:
 * 1. wait for target discovered
 * 2. trigger "topology broadcast": better usability, low system impact (only one target)
 * 3. wait for two component (repository and history): repository provide supply chain
 * and history component provides most data for UI widgets.
 * TODO: Consider listening to AO for action readiness too if needed.
 * 4. send system notification to UI
 */
public class ApiComponentTargetListener implements TargetListener, StatsListener, RepositoryListener {
    private final UINotificationChannel uiNotificationChannel;

    private final TopologyServiceBlockingStub topologyServiceClient;

    private final AtomicBoolean isSystemNotificationEnabled = new AtomicBoolean(false);

    private final AtomicBoolean isTargetDiscoveryFinished = new AtomicBoolean(false);

    private final AtomicBoolean isSourceTopologyAvailable = new AtomicBoolean(false);

    private final AtomicBoolean isHistoryStatAvailable = new AtomicBoolean(false);

    private final Logger logger = LogManager.getLogger();

    public ApiComponentTargetListener(final TopologyServiceBlockingStub topologyServiceClient,
                                      final UINotificationChannel uiNotificationChannel) {
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
        this.topologyServiceClient = Objects.requireNonNull(topologyServiceClient);
    }

    @Override
    public void onTargetDiscovered(@Nonnull DiscoveryStatus result) {
        if (isSystemNotificationEnabled.get() && result.isSuccessful()) {
            logger.info("Successfully discovered first target, triggering topology broadcast");
            topologyServiceClient.requestTopologyBroadcast(TopologyBroadcastRequest.getDefaultInstance());
            isTargetDiscoveryFinished.set(true);
        }
    }

    @Override
    public void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable) {
        logger.debug("History stat is available.");
        if (isSystemNotificationEnabled.get() &&
                isTargetDiscoveryFinished.get() &&
                isSourceTopologyAvailable.get()) {
            sendNotification();
        } else {
            isHistoryStatAvailable.set(true);
        }
    }

    @Override
    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId) {
        logger.debug("New live topology available in repository");
        if (isSystemNotificationEnabled.get() &&
                isTargetDiscoveryFinished.get() &&
                isHistoryStatAvailable.get()) {
            sendNotification();
        } else {
            isSourceTopologyAvailable.set(true);
        }
    }

    private void sendNotification() {
        logger.info("Both repository and history component data are available, sending system WebSocket notification to UI. ");
        final TargetsNotification targetNotification = buildTargetNotification();
        uiNotificationChannel.broadcastTargetsNotification(targetNotification);
        isTargetDiscoveryFinished.set(false);
        isSystemNotificationEnabled.set(false);
        isHistoryStatAvailable.set(false);
        isSourceTopologyAvailable.set(false);
        logger.info("Disabled system notification ");
    }


    /**
     * Inform this lister that the first target was validated
     */
    public void triggerBroadcastAfterNextDiscovery() {
        logger.info("Enabling broadcast after next target discovery");
        this.isSystemNotificationEnabled.set(true);
    }

    private TargetsNotification buildTargetNotification() {
        final TargetStatusNotification statusNotification = TargetStatusNotification.newBuilder()
                .setStatus(TargetStatus.DISCOVERED)
                .setDescription("")
                .build();
        final TargetsNotification.Builder retBuilder = TargetsNotification.newBuilder();
        retBuilder.setStatusNotification(statusNotification);
        return retBuilder.build();
    }
}
