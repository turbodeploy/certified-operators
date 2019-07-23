package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

public class ServiceEntityMapper {

    private final Logger logger = LogManager.getLogger();

    private final DiscoveredByCache discoveredByCache;

    public ServiceEntityMapper(@Nonnull final TopologyProcessor topologyProcessor) {
        this(new DiscoveredByCache(topologyProcessor));
    }

    @VisibleForTesting
    ServiceEntityMapper(@Nonnull final DiscoveredByCache discoveredByCache) {
        this.discoveredByCache = discoveredByCache;
    }

    /**
     * Copies the the basic fields of a {@link BaseApiDTO} object from a {@link TopologyEntityDTO}
     * object.  Basic fields are: display name, class name, and uuid.
     *
     * @param topologyEntityDTO the object whose basic fields are to be copied.
     */
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final ApiPartialEntity topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final MinimalEntity topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        return baseApiDTO;
    }

    /**
     * Converts a {@link ApiPartialEntity} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API.
     *
     * Note: because of the structure of {@link ServiceEntityApiDTO},
     * only one of the discovering targets can be included in the result.
     *
     * @param topologyEntityDTO the internal {@link ApiPartialEntity} to convert
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull ApiPartialEntity topologyEntityDTO) {
        // basic information
        final ServiceEntityApiDTO result = ServiceEntityMapper.toBasicEntity(topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            result.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).apiStr());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(result::setEnvironmentType);
        }

        if (!topologyEntityDTO.getDiscoveringTargetIdsList().isEmpty()) {
            discoveredByCache.getTargetApiDTO(topologyEntityDTO.getDiscoveringTargetIds(0))
                .ifPresent(result::setDiscoveredBy);
        }

        //tags
        result.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        return result;
    }

    /**
     * Converts a {@link TopologyEntityDTO} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API.
     *
     * Note: because of the structure of {@link ServiceEntityApiDTO},
     * only one of the discovering targets can be included in the result.
     *
     * @param topologyEntityDTO the internal {@link TopologyEntityDTO} to convert
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        // basic information
        final ServiceEntityApiDTO seDTO = ServiceEntityMapper.toBasicEntity(topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            seDTO.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).apiStr());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(seDTO::setEnvironmentType);
        }

        final List<Long> discoveringTargets =
            topologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList();
        if (!discoveringTargets.isEmpty()) {
            discoveredByCache.getTargetApiDTO(discoveringTargets.get(0))
                .ifPresent(seDTO::setDiscoveredBy);
        }

        //tags
        seDTO.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        return seDTO;
    }

    /**
     * Creates a shallow clone of a {@link ServiceEntityApiDTO} object.
     *
     * @param serviceEntityApiDTO the object to clone.
     * @return the new object.
     */
    @Nonnull
    public static ServiceEntityApiDTO copyServiceEntityAPIDTO(
            @Nonnull ServiceEntityApiDTO serviceEntityApiDTO) {
        // basic information
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.setDisplayName(Objects.requireNonNull(serviceEntityApiDTO).getDisplayName());
        result.setClassName(serviceEntityApiDTO.getClassName());
        result.setUuid(serviceEntityApiDTO.getUuid());
        result.setState(serviceEntityApiDTO.getState());
        result.setEnvironmentType(serviceEntityApiDTO.getEnvironmentType());

        // aspects, if required
        result.setAspects(serviceEntityApiDTO.getAspects());

        // target, if exists
        if (serviceEntityApiDTO.getDiscoveredBy() != null) {
            final TargetApiDTO targetApiDTO = new TargetApiDTO();
            targetApiDTO.setUuid(serviceEntityApiDTO.getDiscoveredBy().getUuid());
            targetApiDTO.setDisplayName(serviceEntityApiDTO.getDiscoveredBy().getDisplayName());
            targetApiDTO.setType(serviceEntityApiDTO.getDiscoveredBy().getType());
            result.setDiscoveredBy(targetApiDTO);
        }

        //tags
        result.setTags(serviceEntityApiDTO.getTags());

        return result;
    }

    /**
     * Caches the "thin" {@link TargetApiDTO}s required for the
     * {@link ServiceEntityApiDTO#setDiscoveredBy(TargetApiDTO)} operation.
     */
    static class DiscoveredByCache implements TargetListener {
        private static final Logger logger = LogManager.getLogger();

        private final TopologyProcessor topologyProcessor;

        private final Map<Long, SetOnce<TargetApiDTO>> targetsById = Collections.synchronizedMap(new HashMap<>());

        DiscoveredByCache(@Nonnull final TopologyProcessor topologyProcessor) {
            this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
            topologyProcessor.addTargetListener(this);
        }

        /**
         * Get the {@link TargetApiDTO} associated with the target ID. This may result in a call
         * to the topology processor. Concurrent calls to this method for the same target ID will
         * only result in one topology processor RPC.
         *
         * @param targetId The target ID.
         * @return An {@link Optional} containing a {@link TargetApiDTO}. Note - it's a thin
         *         {@link TargetApiDTO}, containing only high-level display information (e.g. name).
         */
        @Nonnull
        public Optional<TargetApiDTO> getTargetApiDTO(final long targetId) {
            final SetOnce<TargetApiDTO> target = targetsById.computeIfAbsent(targetId, k -> new SetOnce<>());
            return Optional.ofNullable(target.ensureSet(() -> loadTargetApiDTO(targetId).orElse(null)));
        }

        @Override
        public void onTargetRemoved(final long targetId) {
            logger.info("Target {} removed. Clearing it from the cache.", targetId);
            targetsById.remove(targetId);
        }

        @Override
        public void onTargetChanged(final TargetInfo newInfo) {
            logger.info("Target {} modified. Clearing it from the cache.", newInfo.getId());
            targetsById.remove(newInfo.getId());
        }

        @Nonnull
        private Optional<TargetApiDTO> loadTargetApiDTO(long targetId) {
            logger.info("Loading target {} into cache.", targetId);
            final TargetApiDTO result = new TargetApiDTO();
            result.setUuid(Long.toString(targetId));

            final TargetInfo targetInfo;
            try {
                // get target info from the topology processor
                targetInfo = topologyProcessor.getTarget(targetId);
                result.setDisplayName(
                    TargetData.getDisplayName(targetInfo)
                        .orElseGet(() -> {
                            logger.warn("Cannot find the display name of target with id {}", targetId);
                            return "";
                        }));

                // fetch information about the probe, and store the probe type in the result
                result.setType(topologyProcessor.getProbe(targetInfo.getProbeId()).getType());

            } catch (TopologyProcessorException | CommunicationException e) {
                logger.warn("Error communicating with the topology processor: {}", e.getMessage());
                return Optional.empty();
            }

            return Optional.of(result);
        }
    }

}
