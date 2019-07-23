package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.service.ReservedInstancesService;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CloudAspectMapper implements IAspectMapper {

    private final ReservedInstancesService riService;

    public CloudAspectMapper(@Nonnull final ReservedInstancesService riService) {
        this.riService = riService;
    }

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        // this aspect only applies to cloud service entities
        if (!IAspectMapper.isCloudEntity(entity)) {
            return null;
        }
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            // get latest RI coverage
            setRiCoverage(entity.getOid(), aspect);
        }
        return aspect;
    }

    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final ApiPartialEntity entity) {
        // this aspect only applies to cloud service entities
        if (!IAspectMapper.isCloudEntity(entity)) {
            return null;
        }
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            // get latest RI coverage
            setRiCoverage(entity.getOid(), aspect);
        }
        return aspect;
    }

    /**
     * Fetch and set the riCoverage and riCoveragePercentage on the given entity's CloudAspectApiDTO.
     *
     * @param entityId the id of the entity to fetch riCoverage for.
     * @param aspect the CloudAspectApiDTO to set riCoverage and riCoveragePercentage on
     */
    private void setRiCoverage(final long entityId,
                               @Nonnull CloudAspectApiDTO aspect) {
        // get latest RI coverage
        Optional<StatApiDTO> optRiCoverage = riService.getLatestRICoverageStats(
            String.valueOf(entityId));
        optRiCoverage.ifPresent(riCoverage -> {
            // set riCoverage
            aspect.setRiCoverage(riCoverage);
            // set riCoveragePercentage
            StatValueApiDTO capacity = riCoverage.getCapacity();
            if (capacity != null && capacity.getAvg() > 0) {
                Float utilization = (riCoverage.getValue() / capacity.getAvg()) * 100;
                aspect.setRiCoveragePercentage(utilization);
            } else {
                aspect.setRiCoveragePercentage(0f);
            }
        });
    }

    @Override
    public @Nonnull String getAspectName() {
        return StringConstants.CLOUD_ASPECT_NAME;
    }
}
