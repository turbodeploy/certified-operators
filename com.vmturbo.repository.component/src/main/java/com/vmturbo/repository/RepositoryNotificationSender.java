package com.vmturbo.repository;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.repository.api.RepositoryDTO.AvailableTopology;
import com.vmturbo.repository.api.RepositoryDTO.FailedTopology;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;

/**
 * Repository API backend to send repository notifications through.
 */
public class RepositoryNotificationSender extends
        ComponentNotificationSender<RepositoryNotification> {

    private final IMessageSender<RepositoryNotification> sender;

    public RepositoryNotificationSender(@Nonnull IMessageSender<RepositoryNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    public void onProjectedTopologyAvailable(final long projectedTopologyId,
            final long topologyContextId) throws CommunicationException, InterruptedException {
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setNewProjectedTopologyAvailable(AvailableTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(projectedTopologyId))
                .build();
        sendMessage(sender, message);
    }

    public void onProjectedTopologyFailure(final long projectedTopologyId,
            final long topologyContextId, @Nonnull final String description)
            throws CommunicationException, InterruptedException {
        Objects.requireNonNull(description);
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setFailedProjectedTopology(FailedTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(projectedTopologyId)
                        .setFailureDescription(description))
                .build();
        sendMessage(sender, message);
    }

    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId)
            throws CommunicationException, InterruptedException {
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setNewSourceTopologyAvailable(AvailableTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(topologyId))
                .build();
        sendMessage(sender, message);
    }

    public void onSourceTopologyFailure(final long topologyId, final long topologyContextId,
            @Nonnull final String description) throws CommunicationException, InterruptedException {
        Objects.requireNonNull(description);
        final RepositoryNotification message = RepositoryNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setFailedSourceTopology(FailedTopology.newBuilder()
                        .setContextId(topologyContextId)
                        .setTopologyId(topologyId)
                        .setFailureDescription(description))
                .build();
        sendMessage(sender, message);
    }

    @Override
    protected String describeMessage(@Nonnull RepositoryNotification repositoryNotification) {
        return RepositoryNotification.class.getSimpleName() + "[" +
                repositoryNotification.getBroadcastId() + "]";
    }
}
