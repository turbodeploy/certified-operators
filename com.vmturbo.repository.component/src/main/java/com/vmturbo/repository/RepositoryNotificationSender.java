package com.vmturbo.repository;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.repository.api.RepositoryDTO.AvailableTopology;
import com.vmturbo.repository.api.RepositoryDTO.FailedTopology;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;

/**
 * Repository API backend to send repository notifications through.
 */
public class RepositoryNotificationSender extends
        ComponentNotificationSender<RepositoryNotification> {

    public RepositoryNotificationSender(@Nonnull final ExecutorService threadPool) {
        super(threadPool);
    }

    public void onProjectedTopologyAvailable(final long projectedTopologyId, final long topologyContextId) {
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setNewProjectedTopologyAvailable(AvailableTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(projectedTopologyId))
                .build();
        sendMessage(message.getBroadcastId(), message);
    }

    public void onProjectedTopologyFailure(final long projectedTopologyId,
                                           final long topologyContextId, @Nonnull final String description) {
        Objects.requireNonNull(description);
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setFailedProjectedTopology(FailedTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(projectedTopologyId)
                        .setFailureDescription(description))
                .build();
        sendMessage(message.getBroadcastId(), message);
    }

    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId) {
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setNewSourceTopologyAvailable(AvailableTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(topologyId))
                .build();
        sendMessage(message.getBroadcastId(), message);
    }

    public void onSourceTopologyFailure(final long topologyId, final long topologyContextId,
                                        @Nonnull final String description) {
        Objects.requireNonNull(description);
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setFailedSourceTopology(FailedTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(topologyId)
                        .setFailureDescription(description))
                .build();
        sendMessage(message.getBroadcastId(), message);
    }

    @Override
    protected String describeMessage(@Nonnull RepositoryNotification repositoryNotification) {
        return RepositoryNotification.class.getSimpleName() + "[" +
                repositoryNotification.getBroadcastId() + "]";
    }
}
