package com.vmturbo.repository.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.RepositoryDTO.AvailableTopology;
import com.vmturbo.repository.api.RepositoryDTO.FailedTopology;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Implementation of repository client.
 */
public class RepositoryNotificationReceiver extends
        ComponentNotificationReceiver<RepositoryNotification> implements Repository {

    public static final String TOPOLOGY_TOPIC = "repository-topology-notifications";

    private final Set<RepositoryListener> listeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public RepositoryNotificationReceiver(
            @Nonnull final IMessageReceiver<RepositoryNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull RepositoryNotification message) throws ApiClientException {
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
        for (RepositoryListener listener : listeners) {
            getExecutorService().submit(
                    () -> listener.onProjectedTopologyAvailable(topology.getTopologyId(),
                            topology.getContextId()));
        }
    }

    private void onFailedProjectedTopology(@Nonnull final FailedTopology topology) {
        Objects.requireNonNull(topology);
        for (RepositoryListener listener : listeners) {
            getExecutorService().submit(
                    () -> listener.onProjectedTopologyFailure(topology.getTopologyId(),
                            topology.getContextId(), topology.getFailureDescription()));
        }
    }

    private void onSourceTopologyReceived(@Nonnull final AvailableTopology topology) {
        Objects.requireNonNull(topology);
        for (RepositoryListener listener : listeners) {
            getExecutorService().submit(
                    () -> listener.onSourceTopologyAvailable(topology.getTopologyId(),
                            topology.getContextId()));
        }
    }

    private void onFailedSourceTopology(@Nonnull final FailedTopology topology) {
        Objects.requireNonNull(topology);
        for (RepositoryListener listener : listeners) {
            getExecutorService().submit(
                    () -> listener.onSourceTopologyFailure(topology.getTopologyId(),
                            topology.getContextId(), topology.getFailureDescription()));
        }
    }

    @Override
    public void addListener(@Nonnull final RepositoryListener listener) {
        listeners.add(Objects.requireNonNull(listener, "Listener should not be null"));
    }
}
