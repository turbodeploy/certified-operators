package com.vmturbo.cost.calculation.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * A factory to create instances of {@link TopologyEntityCloudTopology}.
 */
public interface TopologyEntityCloudTopologyFactory {

    /**
     * Create a new {@link TopologyEntityCloudTopology} out of a stream of {@link TopologyEntityDTO}s.
     *
     * @param entities The entities in the cloud topology. The factory may filter out non-cloud
     *                 entities.
     * @return A {@link TopologyEntityCloudTopology} containing the cloud subset of the entities.
     */
    @Nonnull
    TopologyEntityCloudTopology newCloudTopology(@Nonnull final Stream<TopologyEntityDTO> entities);

    /**
     * Create a new {@link TopologyEntityCloudTopology} out of a {@link RemoteIterator}.
     *
     * @param entities The {@link RemoteIterator} over the entities in the cloud topology.
     *                 The factory may filter out non-cloud entities.
     * @return A {@link TopologyEntityCloudTopology} containing the cloud subset of the entities.
     */
    @Nonnull
    TopologyEntityCloudTopology newCloudTopology(final long topologyContextId, @Nonnull final RemoteIterator<TopologyEntityDTO> entities);

    /**
     * The default implementation of {@link TopologyEntityCloudTopologyFactory}.
     */
    class DefaultTopologyEntityCloudTopologyFactory implements TopologyEntityCloudTopologyFactory {

        private static final Logger logger = LogManager.getLogger();

        private final TopologyProcessor topologyProcessorClient;

        private static final Set<SDKProbeType> CLOUD_PROBE_TYPES = ImmutableSet.of(
                SDKProbeType.AWS,
                SDKProbeType.AZURE);

        public DefaultTopologyEntityCloudTopologyFactory(@Nonnull final TopologyProcessor topologyProcessorClient) {
            this.topologyProcessorClient = Objects.requireNonNull(topologyProcessorClient);
        }

        /**
         *  {@inheritDoc}
         */
        @Nonnull
        @Override
        public TopologyEntityCloudTopology newCloudTopology(@Nonnull final Stream<TopologyEntityDTO> entities) {
            final Map<Long, SDKProbeType> probeTypesOfTargets = fetchTargetProbeTypes();
            return new TopologyEntityCloudTopology(
                    entities.filter(entity -> isCloudEntity(entity, probeTypesOfTargets)));
        }

        /**
         *  {@inheritDoc}
         */
        @Nonnull
        @Override
        public TopologyEntityCloudTopology newCloudTopology(final long topologyContextId,
                                                            @Nonnull final RemoteIterator<TopologyEntityDTO> entities) {
            final Map<Long, SDKProbeType> probeTypesOfTargets = fetchTargetProbeTypes();
            final Stream.Builder<TopologyEntityDTO> streamBuilder = Stream.builder();
            try {
                while (entities.hasNext()) {
                    entities.nextChunk().stream()
                        .filter(entity -> isCloudEntity(entity, probeTypesOfTargets))
                        .forEach(streamBuilder);
                }
            } catch (TimeoutException | CommunicationException  e) {
                logger.error("Error retrieving topology in context " + topologyContextId, e);
            } catch (InterruptedException ie) {
                logger.error("Thread interrupted while processing topology in context " + topologyContextId, ie);
            }
            return new TopologyEntityCloudTopology(streamBuilder.build());
        }

        /**
         * Check if the entity was discovered by cloud probes(AWS/Azure etc).
         * TODO (roman, 17 Oct 2018): We should move this logic to the TP, as part of the Origin
         * information. There are too many cases to properly handle here.
         *
         * @param entityDTO DTO of the entity.
         * @return Return true if the entity was discovered by Cloud probes.
         *          Else return false.
         */
        private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entityDTO,
                                      @Nonnull final Map<Long, SDKProbeType> probeTypesOfTargets) {
            // If, for whatever reason, we don't have the probe types of targets, let all entities
            // into the cloud topology. This may be bad for performance, but the alternative is not
            // letting any entities, which would be much worse.
            // TODO (roman, 17 Oct 2018): Restrict by entity type.
            if (probeTypesOfTargets.isEmpty()) {
                return true;
            }

            final Set<Long> targetIds = new HashSet<>(entityDTO.getOrigin().getDiscoveryOrigin()
                    .getDiscoveringTargetIdsList());
            if (targetIds.isEmpty()) {
                // If the discovering target is not set, we can't know if the entity should or
                // shouldn't be in the cloud topology. We include it if it was added as part of
                // a plan.
                // TODO (roman, 17 Oct 2018): Restrict by entity type.
                return entityDTO.getOrigin().hasPlanOrigin();
            }

            return targetIds.stream()
                .map(targetId -> {
                    final SDKProbeType probeType = probeTypesOfTargets.get(targetId);
                    if (probeType == null) {
                        logger.warn("No targetInfo present for target {}.", targetId);
                    }
                    return probeType;
                })
                .filter(Objects::nonNull)
                .anyMatch(CLOUD_PROBE_TYPES::contains);
        }

        /**
         *  Get all the targets from the TopologyProcessor and create a mapping
         *  from TargetId -> TargetType
         */
        @Nonnull
        private Map<Long, SDKProbeType> fetchTargetProbeTypes() {
            final Map<Long, SDKProbeType> retMap = new HashMap<>();
            try {
                final Set<TargetInfo> targets =
                        topologyProcessorClient.getAllTargets();
                logger.info("Loaded {} targets from TP",
                        targets.size());
                // Mapping from TargetId -> ProbeId
                final Map<Long, Long> targetIdToProbeIdMap = targets.stream()
                        .collect(Collectors.toMap(TargetInfo::getId, TargetInfo::getProbeId));
                if (targetIdToProbeIdMap.isEmpty()) {
                    return retMap;
                }

                // Mapping from ProbeId -> ProbeType
                final Set<ProbeInfo> probeInfos = topologyProcessorClient.getAllProbes();
                logger.info("Loaded {} probeInfos from TP", probeInfos.size());
                final Map<Long, SDKProbeType> probeIdToProbeTypeMap = probeInfos.stream()
                    // May be null for non-production probes (e.g. stress probe)
                    .filter(probeInfo -> SDKProbeType.create(probeInfo.getType()) != null)
                    .collect(Collectors.toMap(ProbeInfo::getId,
                        probeInfo -> SDKProbeType.create(probeInfo.getType())));

                targetIdToProbeIdMap.forEach((targetId, probeId) -> {
                    final SDKProbeType probeType = probeIdToProbeTypeMap.get(probeId);
                    if (probeType != null) {
                        retMap.put(targetId, probeType);
                    } else {
                        logger.error("Probe {} for target {} not found. Ignoring target.", probeId, targetId);
                    }
                });
            } catch (CommunicationException e) {
                logger.error("Error getting target and probe infos from TP", e);
            } catch (RuntimeException e) {
                logger.error("Runtime error getting target and probe infos from TP.", e);
            }
            return retMap;
        }
    }
}
