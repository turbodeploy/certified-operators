package com.vmturbo.repository.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO;
import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.AvailableTopology;
import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.FailedTopology;
import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Implementation of repository client.
 */
public class RepositoryNotificationReceiver extends
    MulticastNotificationReceiver<RepositoryNotification, RepositoryListener> {

    public static final String TOPOLOGY_TOPIC = "repository-topology-notifications";

    public RepositoryNotificationReceiver(
            @Nonnull final IMessageReceiver<RepositoryNotification> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeout) {
        super(messageReceiver, executorService, kafkaReceiverTimeout);
    }

    @Override
    protected void processMessage(@Nonnull RepositoryNotification message,
                                  @Nonnull final SpanContext tracingContext) throws ApiClientException {
        getLogger().debug("Received message {} of type {}", message.getBroadcastId(),
                message.getTypeCase());
        switch (message.getTypeCase()) {
            case NEW_PROJECTED_TOPOLOGY_AVAILABLE:
                onProjectedTopologyReceived(message.getNewProjectedTopologyAvailable());
                break;
            case FAILED_PROJECTED_TOPOLOGY:
                onFailedProjectedTopology(message.getFailedProjectedTopology());
                break;
            case NEW_SOURCE_TOPOLOGY_AVAILABLE:
                onSourceTopologyReceived(message.getNewSourceTopologyAvailable());
                break;
            case FAILED_SOURCE_TOPOLOGY:
                onFailedSourceTopology(message.getFailedSourceTopology());
                break;
            default:
                getLogger().error("Unknown message type received: {}",
                        message.getTypeCase());
        }
    }

    private void onProjectedTopologyReceived(@Nonnull final AvailableTopology topology) {
        Objects.requireNonNull(topology);
        invokeListeners(listener -> listener.onProjectedTopologyAvailable(topology.getTopologyId(),
                topology.getContextId()));
    }

    private void onFailedProjectedTopology(@Nonnull final RepositoryNotificationDTO.FailedTopology topology) {
        Objects.requireNonNull(topology);
        invokeListeners(listener -> listener.onProjectedTopologyFailure(topology.getTopologyId(),
                topology.getContextId(), topology.getFailureDescription()));
    }

    private void onSourceTopologyReceived(@Nonnull final AvailableTopology topology) {
        Objects.requireNonNull(topology);
        invokeListeners(listener -> listener.onSourceTopologyAvailable(topology.getTopologyId(),
                topology.getContextId()));
    }

    private void onFailedSourceTopology(@Nonnull final FailedTopology topology) {
        Objects.requireNonNull(topology);
        invokeListeners(listener -> listener.onSourceTopologyFailure(topology.getTopologyId(),
                topology.getContextId(), topology.getFailureDescription()));
    }
}
