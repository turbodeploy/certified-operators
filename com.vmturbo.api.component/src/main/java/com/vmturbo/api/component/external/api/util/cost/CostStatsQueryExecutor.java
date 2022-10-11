package com.vmturbo.api.component.external.api.util.cost;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.CostGroupByMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.Market;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Execute cost stats queries, for history billed cost charged by cloud vendors.
 */
public class CostStatsQueryExecutor {
    private static final Logger logger = LogManager.getLogger();
    private final CostServiceBlockingStub costServiceRpc;
    private final StatsMapper statsMapper;

    /**
     * Constructor for CostStatsQueryExecutor with cost service.
     *
     * @param costServiceRpc cost service
     * @param statsMapper    Statistics mapper.
     */
    public CostStatsQueryExecutor(
            @Nonnull final CostServiceBlockingStub costServiceRpc,
            @Nonnull final StatsMapper statsMapper) {
        this.costServiceRpc = costServiceRpc;
        this.statsMapper = statsMapper;
    }

    /**
     * Get billed costs stats for an entity.
     *
     * @param entityOid Entity OID.
     * @param entityType Entity type.
     * @param costInputApiDTO Optional {@link CostInputApiDTO} object.
     * @return List of statistics.
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getEntityCostStats(
            final long entityOid,
            @Nonnull final EntityType entityType,
            @Nullable final CostInputApiDTO costInputApiDTO) {
        final GetCloudBilledStatsRequest request = buildGetCloudBilledStatsRequest(
                costInputApiDTO, entityType, Collections.singletonList(entityOid));
        return queryCostServiceAndConvert(request);
    }

    /**
     * Get cost stats for given group.
     *
     * @param groupUuid group uuid
     * @param groupAndMembers grouping and members for the group
     * @param costInputApiDTO query input DTO
     * @return list of StatSnapshotApiDTOs
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getGroupCostStats(@Nonnull String groupUuid,
                                                      @Nonnull GroupAndMembers groupAndMembers,
                                                      @Nullable CostInputApiDTO costInputApiDTO) {
        final Grouping grouping = groupAndMembers.group();
        if (grouping == null) {
            logger.error("Couldn't find valid grouping for queried group {}", groupUuid);
            return Collections.emptyList();
        }
        final EntityType entityMemberType = grouping.getExpectedTypesList().stream()
                .filter(MemberType::hasEntity)
                .map(MemberType::getEntity)
                .map(EntityType::forNumber)
                .findFirst().orElse(null);
        if (entityMemberType == null) {
            logger.error("No valid entity member type found in group {}", groupUuid);
            return Collections.emptyList();
        }
        final Collection<Long> leafMembers = groupAndMembers.entities();
        // Supported group type:
        // 1. a ResourceGroup, a BillingFamily
        // 2. a group of VMs/DBs/DBSs/Volumes/regions/accounts/ResourceGroups/BillingFamilies
        final GetCloudBilledStatsRequest request = buildGetCloudBilledStatsRequest(costInputApiDTO,
                entityMemberType, leafMembers);
        return queryCostServiceAndConvert(request);
    }

    /**
     * Get cost stats for global scope, i.e. everything in cloud.
     *
     * @param costInputApiDTO query input DTO
     * @return list of StatSnapshotApiDTOs
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getGlobalCostStats(@Nullable CostInputApiDTO costInputApiDTO) {
        final GetCloudBilledStatsRequest request =
                buildGetCloudBilledStatsRequest(costInputApiDTO)
                        .setMarket(Market.newBuilder().build())
                        .build();
        return queryCostServiceAndConvert(request);
    }

    @VisibleForTesting
    protected List<StatSnapshotApiDTO> queryCostServiceAndConvert(GetCloudBilledStatsRequest request) {
        return costServiceRpc.getCloudBilledStats(request)
                .getBilledStatRecordList().stream()
                .map(statsMapper::toCostStatSnapshotApiDTO)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected GetCloudBilledStatsRequest buildGetCloudBilledStatsRequest(
            @Nullable final CostInputApiDTO costInputApiDTO,
            @Nonnull final EntityType entityType,
            @Nonnull Collection<Long> entityOids) {
        final GetCloudBilledStatsRequest.Builder requestBuilder = buildGetCloudBilledStatsRequest(costInputApiDTO);
        switch (entityType) {
            case REGION:
                requestBuilder.setRegionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(entityOids)
                        .build());
                break;
            case BUSINESS_ACCOUNT:
                requestBuilder.setAccountFilter(AccountFilter.newBuilder()
                        .addAllAccountId(entityOids)
                        .build());
                break;
            default:
                requestBuilder.setEntityFilter(EntityFilter.newBuilder()
                        .addAllEntityId(entityOids)
                        .build());
        }
        return requestBuilder.build();
    }

    @VisibleForTesting
    protected GetCloudBilledStatsRequest.Builder buildGetCloudBilledStatsRequest(@Nullable CostInputApiDTO costInputApiDTO) {
        GetCloudBilledStatsRequest.Builder requestBuilder = GetCloudBilledStatsRequest.newBuilder();
        if (costInputApiDTO == null) {
            return requestBuilder;
        }
        if (costInputApiDTO.getStartDate() != null) {
            requestBuilder.setStartDate(DateTimeUtil.parseTime(costInputApiDTO.getStartDate()));
        }
        if (costInputApiDTO.getEndDate() != null) {
            requestBuilder.setEndDate(DateTimeUtil.parseTime(costInputApiDTO.getEndDate()));
        }
        final List<TagApiDTO> requestedTags = costInputApiDTO.getTagFilters();
        if (requestedTags != null) {
            if (requestedTags.size() > 1) {
                throw new UnsupportedOperationException("Currently only support one tagFilter!");
            }
            requestBuilder.addAllTagFilter(requestedTags.stream()
                    .map(TagsMapper::convertTagToTagFilter)
                    .collect(Collectors.toList()));
        }
        final List<CostGroupBy> costGroupBys = costInputApiDTO.getCostGroupBys();
        if (costGroupBys == null || costGroupBys.isEmpty()) {
            throw new UnsupportedOperationException("Group by field is required!");
        }
        if (costGroupBys.size() != 1) {
            throw new UnsupportedOperationException("Currently only support one group by!");
        }
        requestBuilder.addAllGroupBy(costGroupBys.stream()
                .map(CostGroupByMapper::toGroupByType)
                .collect(Collectors.toList()));
        return requestBuilder;
    }
}
