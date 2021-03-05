package com.vmturbo.integrations.intersight.targetsync;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.ApiException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * {@link IntersightTargetSyncService} checks on the specified Intersight instance and synchronize
 * target infos between Intersight and topology processor.
 */
public class IntersightTargetSyncService implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The connection to access the Intersight instance.
     */
    private final IntersightConnection intersightConnection;

    /**
     * The entry point to access topology processor APIs.
     */
    private final TopologyProcessor topologyProcessor;

    /**
     * How long in seconds to hold off target status update since the target is created/modified in
     * Intersight.
     */
    private final long noUpdateOnChangePeriodSeconds;

    /**
     * whether to inject the assist moid when calling the topology processor API to add targets.
     */
    protected final boolean injectAssistId;

    /**
     * Create a {@link IntersightTargetSyncService} object that will check on the specified
     * Intersight instance and synchronize targets between Intersight and our topology processor.
     *
     * @param intersightConnection          provides connection to the Intersight instance
     * @param topologyProcessor             the entry point to access topology processor APIs
     * @param noUpdateOnChangePeriodSeconds how long to hold off target status update upon target
     *                                      creation/modification
     * @param injectAssistId                whether to inject the assist moid when calling the
     *                                      topology processor API to add targets
     */
    public IntersightTargetSyncService(@Nonnull IntersightConnection intersightConnection,
            @Nonnull TopologyProcessor topologyProcessor, long noUpdateOnChangePeriodSeconds,
            boolean injectAssistId) {
        this.intersightConnection = Objects.requireNonNull(intersightConnection);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.noUpdateOnChangePeriodSeconds = noUpdateOnChangePeriodSeconds;
        this.injectAssistId = injectAssistId;
    }

    @Override
    public void run() {
        try {
            syncTargets();
        } catch (Throwable t) {
            logger.error("Intersight target sync did not complete due to error: ", t);
        }
    }

    /**
     * Discover targets from Intersight and sync them to what's in the topology processor.
     *
     * @throws IOException            when having problems acquiring an auth token from the
     *                                Intersight instance
     * @throws ApiException           when having problems fetching the list of targets from
     *                                Intersight
     * @throws CommunicationException when having problems communicating with the topology
     *                                processor
     * @throws InterruptedException   when interrupted while waiting for topology processor to
     *                                respond
     */
    private void syncTargets() throws IOException, CommunicationException, ApiException,
            InterruptedException {
        final IntersightTargetSyncHelper syncHelper =
                new IntersightTargetSyncHelper(intersightConnection, topologyProcessor);
        syncHelper.addIntersightEndpointAsTarget();
        syncHelper.syncAssetTargets(noUpdateOnChangePeriodSeconds, injectAssistId);
        syncHelper.removeStaleTargets();
    }
}
