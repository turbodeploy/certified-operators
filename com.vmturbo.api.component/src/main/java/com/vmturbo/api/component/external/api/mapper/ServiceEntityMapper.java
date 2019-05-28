package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.topology.UIEntityState;

public class ServiceEntityMapper {

    /**
     * Copy the the basic fields of a {@link BaseApiDTO} object from a {@link TopologyEntityDTO}
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
     * Convert a {@link TopologyEntityDTO} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API
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
            // TODO (roman, May 23 2019) OM-44276: We should get the probe type map and set
            // the discoveredBy field of the entity.
            final TargetApiDTO discoveringTarget = new TargetApiDTO();
            discoveringTarget.setUuid(Long.toString(discoveringTargetIdList.get(0)));
            seDTO.setDiscoveredBy(discoveringTarget);
        }

        //tags
        seDTO.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getValuesList())));

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
}
