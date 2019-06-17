package com.vmturbo.api.component.external.api.mapper;

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

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

public class ServiceEntityMapper {

    private final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    public ServiceEntityMapper(@Nonnull TopologyProcessor topologyProcessor) {
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
    }

    /**
     * Copies the the basic fields of a {@link BaseApiDTO} object from a {@link TopologyEntityDTO}
     * object.  Basic fields are: display name, class name, and uuid.
     *
     * @param baseApiDTO the object whose basic fields are to be set.
     * @param topologyEntityDTO the object whose basic fields are to be copied.
     */
    public static void setBasicFields(
            @Nonnull BaseApiDTO baseApiDTO,
            @Nonnull TopologyEntityDTO topologyEntityDTO) {
        baseApiDTO.setDisplayName(topologyEntityDTO.getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromEntity(topologyEntityDTO).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
    }

    /**
     * Converts a {@link TopologyEntityDTO} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API.
     *
     * Note: because of the structure of {@link ServiceEntityApiDTO},
     * only one of the discovering targets can be included in the result.
     *
     * @param topologyEntityDTO the internal {@link TopologyEntityDTO} to convert
     * @param aspectMapper aspect mapper to use for including aspects.
     *                     if null, then no aspects will be returned.
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public ServiceEntityApiDTO toServiceEntityApiDTO(
            @Nonnull TopologyEntityDTO topologyEntityDTO,
            @Nullable EntityAspectMapper aspectMapper) {
        // basic information
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        setBasicFields(result, topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            result.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).apiStr());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(result::setEnvironmentType);
        }

        // aspects, if required
        if (aspectMapper != null) {
            result.setAspects(aspectMapper.getAspectsByEntity(topologyEntityDTO));
        }

        // target
        final List<Long> discoveringTargetIdList =
            topologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList();
        if (!discoveringTargetIdList.isEmpty()) {
            getTargetApiDTO(discoveringTargetIdList.get(0)).ifPresent(result::setDiscoveredBy);
        }

        //tags
        result.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        return result;
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
     * Convert a {@link com.vmturbo.common.protobuf.search.Search.Entity} to a {@link ServiceEntityApiDTO}.
     *
     * @param entity the entity to convert
     * @param targetIdToProbeType map from target ids to probe types. TODO: remove. OM-47354
     * @return the to resulting service entity API DTO.
     */
    public ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull Entity entity,
                                                     @Nonnull Map<Long, String> targetIdToProbeType) {
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.setDisplayName(entity.getDisplayName());
        result.setState(UIEntityState.fromEntityState(entity.getState()).apiStr());
        result.setClassName(UIEntityType.fromType(entity.getType()).apiStr());
        result.setUuid(String.valueOf(entity.getOid()));
        if (entity.getTargetIdsCount() > 0) {
            getTargetApiDTO(entity.getTargetIdsList().get(0)).ifPresent(result::setDiscoveredBy);
        } else if (targetIdToProbeType.size() > 0) {
            result.setDiscoveredBy(SearchMapper.createDiscoveredBy(
                Long.toString(targetIdToProbeType.keySet().iterator().next()),
                targetIdToProbeType));
        }
        return result;
    }

    @Nonnull
    private Optional<TargetApiDTO> getTargetApiDTO(long targetId) {
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
            logger.warn("Error communicating with the topology processor", e);
            return Optional.empty();
        }

        return Optional.of(result);
    }
}
