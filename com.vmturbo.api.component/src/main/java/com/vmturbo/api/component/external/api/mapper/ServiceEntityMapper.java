package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;

public class ServiceEntityMapper {

    private final Logger logger = LogManager.getLogger();

    private final ThinTargetCache thinTargetCache;

    public ServiceEntityMapper(@Nonnull final ThinTargetCache thinTargetCache) {
        this.thinTargetCache = thinTargetCache;
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
            thinTargetCache.getTargetInfo(topologyEntityDTO.getDiscoveringTargetIds(0))
                .map(this::createTargetApiDto)
                .ifPresent(result::setDiscoveredBy);
        }

        //tags
        result.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        return result;
    }

    @Nonnull
    private TargetApiDTO createTargetApiDto(@Nonnull final ThinTargetInfo thinTargetInfo) {
        final TargetApiDTO apiDTO = new TargetApiDTO();
        apiDTO.setType(thinTargetInfo.probeInfo().type());
        apiDTO.setUuid(Long.toString(thinTargetInfo.oid()));
        apiDTO.setDisplayName(thinTargetInfo.displayName());
        apiDTO.setCategory(thinTargetInfo.probeInfo().category());
        return apiDTO;
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
            thinTargetCache.getTargetInfo(discoveringTargets.get(0))
                .map(this::createTargetApiDto)
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
}
