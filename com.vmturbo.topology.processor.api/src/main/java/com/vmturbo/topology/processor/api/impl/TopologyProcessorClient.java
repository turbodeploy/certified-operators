package com.vmturbo.topology.processor.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.EntitiesWithNewStateListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.TopologySummaryListener;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * Topology processor API client-side implementation. Obtained instance of this class should be
 * closed in order to release all the resources
 */
public class TopologyProcessorClient extends
        ComponentApiClient<TopologyProcessorRestClient> implements TopologyProcessor {

    public static final String NOTIFICATIONS_TOPIC = "tp-notifications";
    public static final String TOPOLOGY_LIVE = "tp-live-topologies";
    public static final String TOPOLOGY_USER_PLAN = "tp-user-plan-topologies";
    public static final String TOPOLOGY_SCHEDULED_PLAN = "tp-scheduled-plan-topologies";
    public static final String TOPOLOGY_SUMMARIES = "topology-summaries";
    /**
     * Topic used to notify about new action state updates received from external action approval
     * backend.
     */
    public static final String EXTERNAL_ACTION_UPDATES_TOPIC = "tp-external-action-state-updates";
    /**
     * Kafka topic used to notify when approval response is received successfully from external
     * action approval backend.
     */
    public static final String EXTERNAL_ACTION_APPROVAL_RESPONSE =
            "tp-external-action-approval-responses";

    /**
     * Represent the kafka topic containing host state change messages.
     */
    public static final String ENTITIES_WITH_NEW_STATE = "entities-with-new-state";

    private final TopologyProcessorNotificationReceiver notificationClient;

    public static TopologyProcessorClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new TopologyProcessorClient(connectionConfig);
    }

    public static TopologyProcessorClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nullable final IMessageReceiver<TopologyProcessorNotification> messageReceiver,
            @Nullable IMessageReceiver<Topology> liveTopologyReceiver,
            @Nullable IMessageReceiver<Topology> planTopologyReceiver,
            @Nullable IMessageReceiver<TopologySummary> topologySummaryReceiver,
            @Nullable IMessageReceiver<EntitiesWithNewState> entitiesWithNewStateReceiver) {
        return new TopologyProcessorClient(connectionConfig, messageReceiver, liveTopologyReceiver,
                planTopologyReceiver, topologySummaryReceiver, entitiesWithNewStateReceiver,
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
     * @param messageReceiver the receiver for notifications
     * @param liveTopologyReceiver the receiver for live topologies
     * @param planTopologyReceiver the receiver for plan topologies
     * @param topologySummaryReceiver the receiver for topology summaries
     * @param entitiesWithNewStateReceiver the receiver for host state changes
     * @param threadPool thread pool to use
     */
    private TopologyProcessorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nullable IMessageReceiver<TopologyProcessorNotification> messageReceiver,
            @Nullable IMessageReceiver<Topology> liveTopologyReceiver,
            @Nullable IMessageReceiver<Topology> planTopologyReceiver,
            @Nullable IMessageReceiver<TopologySummary> topologySummaryReceiver,
            @Nullable IMessageReceiver<EntitiesWithNewState> entitiesWithNewStateReceiver,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig);
        this.notificationClient =
                new TopologyProcessorNotificationReceiver(messageReceiver, liveTopologyReceiver,
                        planTopologyReceiver, topologySummaryReceiver, entitiesWithNewStateReceiver,
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
    public void addActionListener(@Nonnull final ActionExecutionListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addActionListener(listener);
    }

    @Override
    public void addProbeListener(@Nonnull final ProbeListener listener) {
        Preconditions.checkArgument(listener != null);
        getNotificationClient().addProbeListener(listener);
    }

    @Override
    public void addLiveTopologyListener(@Nonnull EntitiesListener listener) {
        getNotificationClient().addLiveTopoListener(listener);
    }

    @Override
    public void addPlanTopologyListener(@Nonnull EntitiesListener listener) {
        getNotificationClient().addPlanTopoListener(listener);
    }

    @Override
    public void addTopologySummaryListener(@Nonnull final TopologySummaryListener listener) {
        getNotificationClient().addTopologySummaryListener(listener);
    }

    @Override
    public void addEntitiesWithNewStatesListener(@Nonnull final EntitiesWithNewStateListener listener) {
        getNotificationClient().addEntitiesWithNewStateListener(listener);
    }

    @Nonnull
    private TopologyProcessorNotificationReceiver getNotificationClient() {
        if (notificationClient == null) {
            throw new IllegalStateException("Notification API is not enabled");
        }
        return notificationClient;
    }
}
