package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.service.ReservedInstancesService;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CloudAspectMapper implements IAspectMapper {
    private static final Logger logger = LogManager.getLogger();

    private final StatsQueryExecutor statsQueryExecutor;

    private final UuidMapper uuidMapper;

    public CloudAspectMapper(@Nonnull final StatsQueryExecutor statsQueryExecutor,
                             @Nonnull final UuidMapper uuidMapper) {
        this.statsQueryExecutor = statsQueryExecutor;
        this.uuidMapper = uuidMapper;
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
        final StatApiInputDTO cvgRequest = new StatApiInputDTO();
        cvgRequest.setName(StringConstants.RI_COUPON_COVERAGE);
        final StatPeriodApiInputDTO statInput = new StatPeriodApiInputDTO();
        statInput.setStatistics(Collections.singletonList(cvgRequest));
        try {
            final Optional<StatApiDTO> optRiCoverage =
                statsQueryExecutor.getAggregateStats(uuidMapper.fromOid(entityId), statInput).stream()
                    .findFirst()
                    .flatMap(snapshot -> snapshot.getStatistics().stream().findFirst());
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
        } catch (OperationFailedException e) {
            logger.error("Failed to get RI coverage for entity {} : {}",
                entityId, e.getMessage());
        }
    }

    @Override
    public @Nonnull String getAspectName() {
        return StringConstants.CLOUD_ASPECT_NAME;
    }
}
