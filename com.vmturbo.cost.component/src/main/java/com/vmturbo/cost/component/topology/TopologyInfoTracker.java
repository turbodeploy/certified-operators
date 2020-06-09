package com.vmturbo.cost.component.topology;

import java.time.Instant;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
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

    private final ReentrantReadWriteLock topologyInfoLock = new ReentrantReadWriteLock(true);

    private final Predicate<TopologySummary> topologySummarySelector;

    private final int maxTrackedTopologies;

    private NavigableSet<TopologyInfo> topologyInfoQueue =
            new TreeSet<>(Comparator.comparing(TopologyInfo::getCreationTime));

    /**
     * Build a {@link TopologyInfoTracker}, based on a topology summary selector. The selector is
     * intended to filter out topologies not relevant to this tracker.
     *
     * @param topologySummarySelector A filter for {@link TopologySummary} instances. This selector is
     *                                expected to focus on a specific topology type (e.g. plans or RT).
     * @param maxTrackedTopologies The maximum number of topologies to track. Topologies will be tracked
     *                             by their creation time, keeping only the latest {@code maxTrackedTopologies}
     *                             topologies.
     */
    public TopologyInfoTracker(@Nonnull Predicate<TopologySummary> topologySummarySelector,
                               int maxTrackedTopologies) {
        this.topologySummarySelector = Objects.requireNonNull(topologySummarySelector);
        this.maxTrackedTopologies = maxTrackedTopologies;
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

            topologyInfoLock.writeLock().lock();
            try {

                topologyInfoQueue.add(topologyInfo);
                final boolean updatedLatest = getLatestTopologyInfo()
                        .map(latestTopoInfo -> latestTopoInfo.getTopologyId() == topologyInfo.getTopologyId())
                        .orElse(false);

                if (updatedLatest) {
                    logger.info("Latest tracked topology summary " +
                            "(Context ID={}, Topology ID={}, Type={}, Creation Time={})",
                            topologyInfo.getTopologyContextId(),
                            topologyInfo.getTopologyId(),
                            topologyInfo.getTopologyType(),
                            Instant.ofEpochMilli(topologyInfo.getCreationTime()));
                }

                // Limit the tracked topologies to the max size
                while (topologyInfoQueue.size() > maxTrackedTopologies) {
                    topologyInfoQueue.pollFirst();
                }

            } finally {
                topologyInfoLock.writeLock().unlock();
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

        topologyInfoLock.readLock().lock();
        try {
            return topologyInfoQueue.isEmpty() ?
                    Optional.empty() : Optional.of(topologyInfoQueue.last());
        } finally {
            topologyInfoLock.readLock().unlock();
        }
    }


    /**
     * Searches for the latest topology info within the {@link TopologyInfoTracker}'s tracked history,
     * in which the creation time is still less than the creation time of {@code targetTopologyInfo}. A
     * prior topology may not be found if fewer than two topology summaries have been broadcast or if
     * the {@code targetTopologyInfo} is the oldest topology tracked.
     *
     * @param targetTopologyInfo The target {@link TopologyInfo}.
     * @return The immediately prior topology to {@code targetTopologyInfo} or {@link Optional#empty()},
     * if one can not be found.
     */
    @Nonnull
    public Optional<TopologyInfo> getPriorTopologyInfo(@Nonnull TopologyInfo targetTopologyInfo) {

        topologyInfoLock.readLock().lock();
        try {
            return Optional.ofNullable(topologyInfoQueue.lower(targetTopologyInfo));
        } finally {
            topologyInfoLock.readLock().unlock();
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
