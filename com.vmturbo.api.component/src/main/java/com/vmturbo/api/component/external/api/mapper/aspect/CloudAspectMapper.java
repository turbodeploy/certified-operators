package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.CommonComparators;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CloudAspectMapper implements IAspectMapper {
    private static final Logger logger = LogManager.getLogger();

    private final StatsQueryExecutor statsQueryExecutor;

    private final SearchServiceBlockingStub searchService;

    private final UuidMapper uuidMapper;

    public CloudAspectMapper(@Nonnull final StatsQueryExecutor statsQueryExecutor,
                             @Nonnull final UuidMapper uuidMapper,
                             @Nonnull final SearchServiceBlockingStub searchService) {
        this.statsQueryExecutor = statsQueryExecutor;
        this.uuidMapper = uuidMapper;
        this.searchService = searchService;
    }

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        // this aspect only applies to cloud service entities
        if (!IAspectMapper.isCloudEntity(entity)) {
            return null;
        }
        return mapEntityToAspect(entity.getEntityType(), entity.getOid());
    }

    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final ApiPartialEntity entity) {
        // this aspect only applies to cloud service entities
        if (!IAspectMapper.isCloudEntity(entity)) {
            return null;
        }
        return mapEntityToAspect(entity.getEntityType(), entity.getOid());
    }

    private EntityAspect mapEntityToAspect(final int entityType, final long entityOid) {
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
            // get latest RI coverage
            setRiCoverage(entityOid, aspect);
            // get/set business account
            getBusinessAccount(entityOid).ifPresent(topologyEntityDTO ->
                    createBusinessAccountBaseApiDTO(topologyEntityDTO, aspect));
        }
        return aspect;
    }

    /**
     * Create base api dto for business account for Cloud Aspect dto
     * @param businessAccount
     * @param aspect
     */
    private void createBusinessAccountBaseApiDTO(TopologyEntityDTO businessAccount, CloudAspectApiDTO aspect) {
        BaseApiDTO businessAccountBaseApiDTO = new BaseApiDTO();
        businessAccountBaseApiDTO.setDisplayName(businessAccount.getDisplayName());
        businessAccountBaseApiDTO.setUuid((Long.toString(businessAccount.getOid())));
        businessAccountBaseApiDTO.setClassName(StringConstants.BUSINESS_ACCOUNT);
        aspect.setBusinessAccount(businessAccountBaseApiDTO);
    }

    /**
     * Get associated business account for a given entity.
     * @param entityId - uuid of the entity
     * @return business account TopologyEntityDTO
     */
    private Optional<TopologyEntityDTO> getBusinessAccount(final long entityId) {
        Search.SearchParameters params = SearchProtoUtil.neighborsOfType(entityId,
                Search.TraversalFilter.TraversalDirection.CONNECTED_FROM,
                UIEntityType.BUSINESS_ACCOUNT);

        Search.SearchEntitiesRequest request = Search.SearchEntitiesRequest.newBuilder().addSearchParameters(params).build();

        return RepositoryDTOUtil.topologyEntityStream(searchService.searchEntitiesStream(request))
                .map(TopologyDTO.PartialEntity::getFullEntity)
                .collect(Collectors.toList()).stream().findFirst();
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
                statsQueryExecutor.getAggregateStats(uuidMapper.fromOid(entityId), statInput)
                        .stream()
                        // Exclude projected snapshot
                        .filter(snapshot -> !isProjected(snapshot))
                        // Get the snapshot with the most recent date assuming that it represents
                        // current state
                        .max((snapshot1, snapshot2) -> CommonComparators.longNumber().compare(
                                getTimeStamp(snapshot1), getTimeStamp(snapshot2)))
                        .flatMap(snapshot -> snapshot.getStatistics().stream().findFirst());
            optRiCoverage.ifPresent(riCoverage -> {
                // set riCoverage
                aspect.setRiCoverage(riCoverage);
                // set riCoveragePercentage
                final float utilization;
                final StatValueApiDTO capacity = riCoverage.getCapacity();
                if (capacity != null
                        && capacity.getAvg() != null
                        && capacity.getAvg() > 0
                        && riCoverage.getValue() != null) {
                    utilization = (riCoverage.getValue() / capacity.getAvg()) * 100;
                } else {
                    utilization = 0F;
                }
                aspect.setRiCoveragePercentage(utilization);
            });
        } catch (OperationFailedException e) {
            logger.error("Failed to get RI coverage for entity {} : {}",
                entityId, e.getMessage());
        }
    }

    /**
     * Check if snapshot is for the projected period.
     *
     * @param snapshot Snapshot to check.
     * @return True for projected snapshot.
     */
    private static boolean isProjected(@Nonnull final StatSnapshotApiDTO snapshot) {
        final Long parsedTimestamp = getTimeStamp(snapshot);
        return parsedTimestamp != null && parsedTimestamp > System.currentTimeMillis();
    }

    @Nullable
    private static Long getTimeStamp(@Nonnull final StatSnapshotApiDTO snapshot) {
        return DateTimeUtil.parseTime(snapshot.getDate());
    }

    @Override
    public @Nonnull String getAspectName() {
        return StringConstants.CLOUD_ASPECT_NAME;
    }
}
