package com.vmturbo.topology.processor.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * Topology processor API client-side implementation. Obtained instance of this class shoule be
 * closed in order to release all the resources
 */
public class TopologyProcessorClient extends
        ComponentApiClient<TopologyProcessorRestClient, TopologyProcessorNotificationReceiver> implements TopologyProcessor {

    public static final String WEBSOCKET_PATH = "/tp-api";

    private final Logger logger = LogManager.getLogger();

    public static TopologyProcessorClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new TopologyProcessorClient(connectionConfig);
    }

    public static TopologyProcessorClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        return new TopologyProcessorClient(connectionConfig, executorService);
    }

    private TopologyProcessorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }

    /**
     * Creates an instance of TopologyProcessor client. This instance will hold the connection to
     * TopologyProcessor ever online and try to reconnect if required. Constructor blocks until
     * connection is established.
     *
     * @param connectionConfig TopologyProcessor connection configuration
     * @param threadPool thread pool to use
     */
    private TopologyProcessorClient(@Nonnull ComponentApiConnectionConfig connectionConfig,
                    ExecutorService threadPool) {
        super(connectionConfig, threadPool);
    }

    @Nonnull
    @Override
    protected TopologyProcessorRestClient createRestClient(@Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new TopologyProcessorRestClient(connectionConfig);
    }

    @Nonnull
    @Override
    protected TopologyProcessorNotificationReceiver createWebsocketClient(
            @Nonnull final ComponentApiConnectionConfig apiClient,
            @Nonnull final ExecutorService executorService) {
        return new TopologyProcessorNotificationReceiver(apiClient, executorService);
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
        websocketClient().addTargetListener(listener);
    }

    @Override
    public void addEntitiesListener(@Nonnull final EntitiesListener listener) {
        Preconditions.checkArgument(listener != null);
        websocketClient().addEntitiesListener(listener);
    }

    @Override
    public void addActionListener(@Nonnull final ActionExecutionListener listener) {
        Preconditions.checkArgument(listener != null);
        websocketClient().addActionListener(listener);
    }

    @Override
    public void addProbeListener(@Nonnull final ProbeListener listener) {
        Preconditions.checkArgument(listener != null);
        websocketClient().addProbeListener(listener);
    }
}
