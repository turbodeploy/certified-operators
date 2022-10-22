package com.vmturbo.api.component.external.api.util.cost;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.CostGroupByMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.cost.BilledCostGroupByMapper;
import com.vmturbo.api.component.external.api.mapper.cost.BilledCostStatsMapper;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostFilter;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.TagFilter;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Execute cost stats queries, for history billed cost charged by cloud vendors.
 */
public class CostStatsQueryExecutor {

    private static final Logger logger = LogManager.getLogger();

    private final CostServiceBlockingStub costServiceRpc;

    private final BilledCostServiceBlockingStub billedCostService;

    private final StatsMapper statsMapper;

    private final BilledCostStatsMapper billedCostStatsMapper;

    /**
     * Constructor for CostStatsQueryExecutor with cost service.
     *
     * @param costServiceRpc cost service
     * @param statsMapper    Statistics mapper.
     */
    public CostStatsQueryExecutor(
            @Nonnull final CostServiceBlockingStub costServiceRpc,
            @Nonnull final BilledCostServiceBlockingStub billedCostService,
            @Nonnull final StatsMapper statsMapper,
            @Nonnull final BilledCostStatsMapper billedCostStatsMapper) {
        this.costServiceRpc = costServiceRpc;
        this.billedCostService = Objects.requireNonNull(billedCostService);
        this.statsMapper = statsMapper;
        this.billedCostStatsMapper = Objects.requireNonNull(billedCostStatsMapper);
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

        return queryCostDataAndConvert(costInputApiDTO, entityType, Collections.singletonList(entityOid));
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
        return queryCostDataAndConvert(costInputApiDTO, entityMemberType, leafMembers);
    }

    /**
     * Get cost stats for global scope, i.e. everything in cloud.
     *
     * @param costInputApiDTO query input DTO
     * @return list of StatSnapshotApiDTOs
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getGlobalCostStats(@Nullable CostInputApiDTO costInputApiDTO) {
        return queryCostDataAndConvert(costInputApiDTO, null, null);
    }

    protected List<StatSnapshotApiDTO> queryCostDataAndConvert(@Nullable final CostInputApiDTO costInputApiDTO,
                                                               @Nullable final EntityType entityType,
                                                               @Nullable final Collection<Long> entityOids) {

        if (FeatureFlags.PARTITIONED_BILLED_COST_QUERY.isEnabled()) {

            final GetBilledCostStatsRequest statsRequest = buildBilledCostStatsRequest(costInputApiDTO, entityType, entityOids);
            return queryBilledCostServiceAndConvert(statsRequest);
        } else {
            final GetCloudBilledStatsRequest billedStatsRequest = buildGetCloudBilledStatsRequest(
                    costInputApiDTO, entityType, entityOids);
            return queryCostServiceAndConvert(billedStatsRequest);
        }
    }

    @VisibleForTesting
    protected List<StatSnapshotApiDTO> queryCostServiceAndConvert(GetCloudBilledStatsRequest request) {
        return costServiceRpc.getCloudBilledStats(request)
                .getBilledStatRecordList().stream()
                .map(statsMapper::toCostStatSnapshotApiDTO)
                .collect(Collectors.toList());
    }

    private List<StatSnapshotApiDTO> queryBilledCostServiceAndConvert(@Nonnull GetBilledCostStatsRequest statsRequest) {
        final GetBilledCostStatsResponse statsResponse = billedCostService.getBilledCostStats(statsRequest);
        return billedCostStatsMapper.convertBilledCostStats(statsResponse.getCostStatsList());
    }

    @VisibleForTesting
    protected GetCloudBilledStatsRequest buildGetCloudBilledStatsRequest(
            @Nullable final CostInputApiDTO costInputApiDTO,
            @Nullable final EntityType entityType,
            @Nullable Collection<Long> entityOids) {
        final GetCloudBilledStatsRequest.Builder requestBuilder = buildGetCloudBilledStatsRequest(costInputApiDTO);

        if (entityType != null && CollectionUtils.isNotEmpty(entityOids)) {
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

    private GetBilledCostStatsRequest buildBilledCostStatsRequest(@Nullable CostInputApiDTO costInputApiDTO,
                                                                  @Nullable final EntityType entityType,
                                                                  @Nullable Collection<Long> entityOids) {


        final GetBilledCostStatsRequest.Builder requestBuilder = GetBilledCostStatsRequest.newBuilder();
        final BilledCostStatsQuery.Builder statsQuery = requestBuilder.getQueryBuilder()
                .setFilter(buildBilledCostFilter(costInputApiDTO, entityType, entityOids));


        // Add group by conditions
        if (costInputApiDTO != null && CollectionUtils.isNotEmpty(costInputApiDTO.getCostGroupBys())) {

            costInputApiDTO.getCostGroupBys().stream()
                    .filter(Objects::nonNull)
                    .map(BilledCostGroupByMapper::toGroupByType)
                    .forEach(statsQuery::addGroupBy);
        }

        return requestBuilder.build();
    }

    private BilledCostFilter buildBilledCostFilter(@Nullable CostInputApiDTO costInputApiDTO,
                                                   @Nullable final EntityType entityType,
                                                   @Nullable Collection<Long> entityOids) {

        final BilledCostFilter.Builder costFilter = BilledCostFilter.newBuilder();

        if (costInputApiDTO != null) {

            if (StringUtils.isNotBlank(costInputApiDTO.getStartDate()))  {
                costFilter.setSampleTsStart(DateTimeUtil.parseTime(costInputApiDTO.getStartDate()));
            }

            if (StringUtils.isNotBlank(costInputApiDTO.getEndDate()))  {
                costFilter.setSampleTsEnd(DateTimeUtil.parseTime(costInputApiDTO.getEndDate()));
            }

            final List<TagApiDTO> tagFilters = costInputApiDTO.getTagFilters();
            if (CollectionUtils.isNotEmpty(tagFilters)) {

                if (tagFilters.size() > 1) {
                    throw new UnsupportedOperationException("Only support one tagFilter!");
                }

                final TagApiDTO tagFilter = Iterables.getOnlyElement(tagFilters);
                costFilter.setTagFilter(TagFilter.newBuilder()
                        .setTagKey(tagFilter.getKey())
                        .addAllTagValue(CollectionUtils.emptyIfNull(tagFilter.getValues()))
                        .build());
            }
        }

        if (entityType != null && CollectionUtils.isNotEmpty(entityOids)) {
            switch (entityType) {
                case REGION:
                    costFilter.addAllRegionId(entityOids);
                    break;
                case BUSINESS_ACCOUNT:
                    costFilter.addAllAccountId(entityOids);
                    break;
                case SERVICE_PROVIDER:
                    costFilter.addAllServiceProviderId(entityOids);
                    break;
                case VIRTUAL_MACHINE:
                case DATABASE:
                case DATABASE_SERVER:
                case VIRTUAL_VOLUME:
                    costFilter.addAllResourceId(entityOids);
                    break;
            }
        }

        return costFilter.build();
    }
}
