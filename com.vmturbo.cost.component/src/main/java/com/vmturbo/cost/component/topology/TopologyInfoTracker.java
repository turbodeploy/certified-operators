package com.vmturbo.cost.component.topology;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary.ResultCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.topology.processor.api.TopologySummaryListener;
/**
 * A {@link TopologySummaryListener} designed to track the latest summary broadcast to allow skipping
 * logic, if a topology listener falls behind in processing topology broadcast. The ordering of
 * {@link TopologySummary} instances is based on the creation timestamp within the {@link TopologyInfo}.
 */
public class TopologyInfoTracker implements TopologySummaryListener {

    /**
     * A {@link TopologySummary} filter to select only successful realtime topology summaries.
     */
    public static final Predicate<TopologySummary> SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR =
            summary ->
                    (summary.getTopologyInfo().getTopologyType() == TopologyType.REALTIME &&
                            summary.getResultCase() == ResultCase.SUCCESS);

    private final Logger logger = LogManager.getLogger();

    private final ReentrantReadWriteLock latestTopologyInfoLock = new ReentrantReadWriteLock(true);

    private final Predicate<TopologySummary> topologySummarySelector;

    @Nullable
    private TopologyInfo latestTopologyInfo = null;

    /**
     * Build a {@link TopologyInfoTracker}, based on a topology summary selector. The selector is
     * intended to filter out topologies not relevant to this tracker.
     *
     * @param topologySummarySelector A filter for {@link TopologySummary} instances. This selector is
     *                                expected to focus on a specific topology type (e.g. plans or RT).
     */
    public TopologyInfoTracker(@Nonnull Predicate<TopologySummary> topologySummarySelector) {
        this.topologySummarySelector = Objects.requireNonNull(topologySummarySelector);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologySummary(@Nonnull final TopologySummary topologySummary) {

        final TopologyInfo topologyInfo = topologySummary.getTopologyInfo();

        logger.info("Received topology summary (Context ID={}, Topology ID={}, Type={}, Creation Time={})",
                topologyInfo.getTopologyContextId(),
                topologyInfo.getTopologyId(),
                topologyInfo.getTopologyType(),
                Instant.ofEpochMilli(topologyInfo.getCreationTime()));

        if (topologySummarySelector.test(topologySummary)) {

            latestTopologyInfoLock.writeLock().lock();
            try {
                final boolean updatedLatest = getLatestTopologyInfo()
                        .map(recordedInfo ->
                                recordedInfo.getCreationTime() < topologyInfo.getCreationTime())

                        .orElse(true);

                if (updatedLatest) {

                    logger.info("Latest tracked topology summary " +
                            "(Context ID={}, Topology ID={}, Type={}, Creation Time={})",
                            topologyInfo.getTopologyContextId(),
                            topologyInfo.getTopologyId(),
                            topologyInfo.getTopologyType(),
                            Instant.ofEpochMilli(topologyInfo.getCreationTime()));

                    latestTopologyInfo = topologyInfo;
                }
            } finally {
                latestTopologyInfoLock.writeLock().unlock();
            }
        }
    }


    /**
     * Provides access to the latest tracked {@link TopologyInfo}, based on the {@link TopologySummary}
     * selector configured for this tracker instance.
     *
     * @return An {@link Optional} containing the latest {@link TopologyInfo}. The {@link Optional}
     * may be empty if no matching {@link TopologySummary} broadcasts have been received. "Latest"
     * {@link TopologyInfo} is determined by {@link TopologyInfo#getCreationTime()}
     */
    public Optional<TopologyInfo> getLatestTopologyInfo() {

        latestTopologyInfoLock.readLock().lock();
        try {
            return Optional.ofNullable(latestTopologyInfo);
        } finally {
            latestTopologyInfoLock.readLock().unlock();
        }
    }


    /**
     * Checks whether the provided {@link TopologyInfo} matches the latest received {@link TopologySummary}.
     * It is expected that the provided {@code topologyInfo} is within the same set of topologies as this
     * tracker.
     *
     * @param topologyInfo The target {@link TopologyInfo} to check.
     * @return True, if the provided {@code topologyInfo} mirrors the latest {@link TopologySummary}
     * broadcast or if no {@link TopologySummary} broadcasts have been received. False, otherwise.
     */
    public boolean isLatestTopology(@Nonnull TopologyInfo topologyInfo) {
        return getLatestTopologyInfo()
                .map(recordedInfo -> recordedInfo.getCreationTime() <= topologyInfo.getCreationTime())
                .orElse(true);
    }
}
