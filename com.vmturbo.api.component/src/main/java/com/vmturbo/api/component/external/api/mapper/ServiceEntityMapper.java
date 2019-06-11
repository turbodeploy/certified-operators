package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.topology.UIEntityState;

public class ServiceEntityMapper {

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
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
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
    public static ServiceEntityApiDTO toServiceEntityApiDTO(
            @Nonnull TopologyEntityDTO topologyEntityDTO,
            @Nullable EntityAspectMapper aspectMapper) {
        // basic information
        final ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        setBasicFields(seDTO, topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            seDTO.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).apiStr());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(seDTO::setEnvironmentType);
        }

        // aspects, if required
        if (aspectMapper != null) {
            seDTO.setAspects(aspectMapper.getAspectsByEntity(topologyEntityDTO));
        }

        final List<Long> discoveringTargetIdList = topologyEntityDTO
            .getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList();
        if (!discoveringTargetIdList.isEmpty()) {
            final TargetApiDTO discoveringTarget = new TargetApiDTO();
            discoveringTarget.setUuid(Long.toString(discoveringTargetIdList.get(0)));
            seDTO.setDiscoveredBy(discoveringTarget);
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

        //tags
        result.setTags(serviceEntityApiDTO.getTags());

        return result;
    }

    /**
     * Convert a {@link com.vmturbo.common.protobuf.search.Search.Entity} to a {@link ServiceEntityApiDTO}.
     *
     * @param entity the entity to convert
     * @param targetIdToProbeType map from target ids to probe types.
     * @return the to resulting service entity API DTO.
     */
    public static ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull Entity entity,
                                                            @Nonnull Map<Long, String> targetIdToProbeType) {
        ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        seDTO.setDisplayName(entity.getDisplayName());
        seDTO.setState(UIEntityState.fromEntityState(entity.getState()).apiStr());
        seDTO.setClassName(UIEntityType.fromType(entity.getType()).apiStr());
        seDTO.setUuid(String.valueOf(entity.getOid()));
        // set discoveredBy
        if (entity.getTargetIdsCount() > 0) {
            seDTO.setDiscoveredBy(SearchMapper.createDiscoveredBy(String.valueOf(entity.getTargetIdsList().get(0)),
                targetIdToProbeType));
        } else if (targetIdToProbeType.size() > 0) {
            seDTO.setDiscoveredBy(SearchMapper.createDiscoveredBy(
                Long.toString(targetIdToProbeType.keySet().iterator().next()),
                targetIdToProbeType));
        }
        return seDTO;
    }
}
