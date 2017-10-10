package com.vmturbo.topology.processor.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * Topology processor API client-side implementation. Obtained instance of this class shoule be
 * closed in order to release all the resources
 */
public class TopologyProcessorClient extends
        ComponentApiClient<TopologyProcessorRestClient> implements TopologyProcessor {

    public static final String NOTIFICATIONS_TOPIC = "tp-notifications";
    public static final String TOPOLOGY_BROADCAST_TOPIC = "tp-topology-broadcast";

    private final TopologyProcessorNotificationReceiver notificationClient;

    public static TopologyProcessorClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new TopologyProcessorClient(connectionConfig);
    }

    public static TopologyProcessorClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nullable final IMessageReceiver<TopologyProcessorNotification> messageReceiver,
            @Nullable final IMessageReceiver<Topology> topologyReceiver) {
        return new TopologyProcessorClient(connectionConfig, messageReceiver, topologyReceiver,
                executorService);
    }

    private TopologyProcessorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.notificationClient = null;
    }

    /**
     * Creates an instance of TopologyProcessor client. This instance will hold the connection to
     * TopologyProcessor ever online and try to reconnect if required. Constructor blocks until
     * connection is established.
     *
     * @param connectionConfig TopologyProcessor connection configuration
     * @param threadPool thread pool to use
     */
    private TopologyProcessorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nullable IMessageReceiver<TopologyProcessorNotification> messageReceiver,
            @Nullable IMessageReceiver<Topology> topologyReceiver,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig);
        this.notificationClient =
                new TopologyProcessorNotificationReceiver(messageReceiver, topologyReceiver,
                        threadPool);
    }

    @Nonnull
    @Override
    protected TopologyProcessorRestClient createRestClient(@Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new TopologyProcessorRestClient(connectionConfig);
    }

    @Override
    @Nonnull
    public Set<ProbeInfo> getAllProbes() throws CommunicationException {
        return restClient.getAllProbes();
    }

    @Override
    @Nonnull
    public ProbeInfo getProbe(final long id) throws CommunicationException, TopologyProcessorException {
        return restClient.getProbe(id);
    }

    @Override
    @Nonnull
    public Set<TargetInfo> getAllTargets() throws CommunicationException {
        return Collections.unmodifiableSet(restClient.getAllTargets());
    }

    @Override
    @Nonnull
    public TargetInfo getTarget(final long id) throws CommunicationException, TopologyProcessorException {
        return restClient.getTarget(id);
    }

    @Override
    public long addTarget(final long probeId, @Nonnull final TargetData targetData)
                    throws CommunicationException, TopologyProcessorException {
        return restClient.addTarget(probeId, targetData);
    }

    @Override
    public void removeTarget(final long target)
                    throws CommunicationException, TopologyProcessorException {
        restClient.removeTarget(target);
    }

    @Override
    public void modifyTarget(final long targetId, @Nonnull final TargetData newData)
                    throws CommunicationException, TopologyProcessorException {
        restClient.modifyTarget(targetId, newData);
    }

    @Override
    @Nonnull
    public ValidationStatus validateTarget(final long targetId) throws CommunicationException,
                    TopologyProcessorException, InterruptedException {
        return restClient.validateTarget(targetId);
    }

    @Override
    @Nonnull
    public Set<ValidationStatus> validateAllTargets()
                    throws CommunicationException, InterruptedException {
        return restClient.validateAllTargets();
    }

    @Override
    @Nonnull
    public DiscoveryStatus discoverTarget(final long targetId) throws CommunicationException,
                    TopologyProcessorException, InterruptedException {
        return restClient.discoverTarget(targetId);
    }

    @Override
    @Nonnull
    public Set<DiscoveryStatus> discoverAllTargets()
                    throws CommunicationException, InterruptedException {
        return restClient.discoverAllTargets();
    }

    @Override
    public void addTargetListener(@Nonnull final TargetListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addTargetListener(listener);
    }

    @Override
    public void addEntitiesListener(@Nonnull final EntitiesListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addEntitiesListener(listener);
    }

    @Override
    public void addActionListener(@Nonnull final ActionExecutionListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addActionListener(listener);
    }

    @Override
    public void addProbeListener(@Nonnull final ProbeListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addProbeListener(listener);
    }

    @Nonnull
    private TopologyProcessorNotificationReceiver getNotificationClient() {
        if (notificationClient == null) {
            throw new IllegalStateException("Notification API is not enabled");
        }
        return notificationClient;
    }
}
