package com.vmturbo.api.component.external.api.mapper;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

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
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final MinimalEntity topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    /**
     * Set displayName to UUID if the former is null or empty
     * @param apiDTO    DTO to update
     */
    private static void setNonEmptyDisplayName(final ServiceEntityApiDTO apiDTO) {
        if (StringUtils.isEmpty(apiDTO.getDisplayName())) {
            apiDTO.setDisplayName(apiDTO.getUuid());
        }
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

        setDiscoveredBy(topologyEntityDTO::getDiscoveredTargetDataMap, result);

        //tags
        result.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        // template name
        topologyEntityDTO.getProvidersList().stream()
                .filter(provider -> provider.hasDisplayName() &&
                        TopologyDTOUtil.PRIMARY_TIER_VALUES.contains(provider.getEntityType()))
                .findAny()
                .ifPresent(provider -> {
                    TemplateApiDTO template = new TemplateApiDTO();
                    template.setDisplayName(provider.getDisplayName());
                    result.setTemplate(template);
                });

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

        setDiscoveredBy(() -> topologyEntityDTO.getOrigin().getDiscoveryOrigin()
                        .getDiscoveredTargetDataMap(), seDTO);

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

        result.setVendorIds(serviceEntityApiDTO.getVendorIds());

        return result;
    }

    private void setDiscoveredBy(Supplier<Map<Long, PerTargetEntityInformation>> idMapGetter, ServiceEntityApiDTO result) {
        Map<Long, PerTargetEntityInformation> target2data = idMapGetter.get();
        if (!target2data.isEmpty()) {
            // only one target will be returned - preferably any non-hidden, if none then any at all
            ThinTargetInfo preferredInfo = null;
            for (Map.Entry<Long, PerTargetEntityInformation> info : target2data.entrySet()) {
                Optional<ThinTargetInfo> thinInfo = thinTargetCache.getTargetInfo(info.getKey());
                if (thinInfo.isPresent() && (!thinInfo.get().isHidden() || preferredInfo == null)) {
                    preferredInfo = thinInfo.get();
                }
            }
            if (preferredInfo != null) {
                result.setDiscoveredBy(createTargetApiDto(preferredInfo));
            } else {
                logger.debug("Failed to locate discoveredBy target information for " + result.getUuid());
            }
            // convert target ids to strings for api
            // skip targets where vendor ids are unset
            result.setVendorIds(target2data.entrySet().stream()
                .filter(target2id -> target2id.getValue().hasVendorId() && !StringUtils
                                .isEmpty(target2id.getValue().getVendorId()))
                .collect(Collectors
                        .toMap(target2id -> String.valueOf(target2id.getKey()),
                               target2id -> target2id.getValue().getVendorId())));
        }
    }

}
