package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.ApplicationServiceAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.ApplicationServiceTier;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;

/**
 * Map a topology entity DTO to an application service aspect.
 */
public class ApplicationServiceAspectMapper extends AbstractAspectMapper {

    /**
     * Maps a topology entity DTO to an application service aspect.
     *
     * @param entity the {@link TopologyEntityDTO} to get aspect for
     * @return mapped app component aspect
     */
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        final ApplicationServiceAspectApiDTO aspect = new ApplicationServiceAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasApplicationService()) {
            ApplicationServiceInfo appSvcInfo = entity.getTypeSpecificInfo().getApplicationService();
            if (appSvcInfo.hasMaxInstanceCount()) {
                aspect.setMaxInstanceCount(appSvcInfo.getMaxInstanceCount());
            }
            if (appSvcInfo.hasCurrentInstanceCount()) {
                aspect.setInstanceCount(appSvcInfo.getCurrentInstanceCount());
            }
            if (appSvcInfo.hasPlatform()) {
                aspect.setPlatform(toPlatform(appSvcInfo.getPlatform()));
            }
            if (appSvcInfo.hasTier()) {
                aspect.setTier(toTier(appSvcInfo.getTier()));
            }
            if (appSvcInfo.hasAppCount()) {
                aspect.setAppCount(appSvcInfo.getAppCount());
            }
            if (appSvcInfo.hasZoneRedundant()) {
                aspect.setZoneRedundant(appSvcInfo.getZoneRedundant());
            }
            if (appSvcInfo.hasDaysEmpty()) {
                aspect.setDaysEmpty(appSvcInfo.getDaysEmpty());
            }
        }
        return aspect;
    }

    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(
            @Nonnull List<TopologyEntityDTO> entities) {
        List<TopologyEntityDTO> cloudEntities = entities.stream().filter(
                AbstractAspectMapper::isCloudEntity).collect(Collectors.toList());
        Map<Long, EntityAspect> resp = new HashMap<>();
        cloudEntities.forEach(entity -> {
            resp.put(entity.getOid(), mapEntityToAspect(entity));
        });
        return Optional.of(resp);
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatch(
            @Nonnull List<TopologyEntityDTO> entities, final long planTopologyContextId)
            throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(String.format("Plan entity aspects not supported by {}",
                getClass().getSimpleName()));
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatchPartial(
            @Nonnull List<ApiPartialEntity> entities, final long planTopologyContextId)
            throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(String.format("Plan entity aspects not supported by {}",
                getClass().getSimpleName()));
    }

    /**
     * Convert a TopologyDTO protobuf tier to the API enum tier.
     *
     * @param from The "from" enum value.
     * @return Converted enum value.
     */
    private static ApplicationServiceTier toTier(ApplicationServiceInfo.Tier from) {
        try {
            return ApplicationServiceTier.valueOf(from.toString());
        } catch (IllegalArgumentException | NullPointerException e) {
            // Unsupported service tier. Must change enums to add support.
            return ApplicationServiceTier.OTHER;
        }
    }

    /**
     * Convert a TopologyDTO protobuf platform to the API enum platform.
     *
     * @param from The "from" enum value.
     * @return Converted enum value.
     */
    private static Platform toPlatform(ApplicationServiceInfo.Platform from) {
        try {
            return Platform.valueOf(from.toString());
        } catch (IllegalArgumentException | NullPointerException e) {
            // Unsupported platform. Must change enums to add support.
            return Platform.UNKNOWN;
        }
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.APP_SERVICE;
    }
}
