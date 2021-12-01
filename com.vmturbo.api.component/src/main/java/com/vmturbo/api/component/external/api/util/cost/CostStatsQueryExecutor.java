package com.vmturbo.api.component.external.api.util.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord.TagKeyValuePair;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.Market;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.TagFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
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

    private static final String TAG_KEY = "tagKey";
    private static final String TAG_VALUE = "tagValue";

    /**
     * Constructor for CostStatsQueryExecutor with cost service.
     *
     * @param costServiceRpc cost service
     */
    public CostStatsQueryExecutor(@Nonnull CostServiceBlockingStub costServiceRpc) {
        this.costServiceRpc = costServiceRpc;
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
        if (grouping == null || grouping.getExpectedTypesList() == null) {
            logger.error("Couldn't find valid grouping for queried group {}", groupUuid);
            return Collections.emptyList();
        }
        final Integer entityMemberType = grouping.getExpectedTypesList().stream()
                .filter(t -> t.hasEntity()).map(MemberType::getEntity).findFirst().orElse(null);
        final GetCloudBilledStatsRequest.Builder requestBuilder = buildGetCloudBilledStatsRequest(costInputApiDTO);
        final Collection<Long> leafMembers = groupAndMembers.entities();
        if (entityMemberType != null) {
            // Supported group type:
            // 1. a ResourceGroup, a BillingFamily
            // 2. a group of VMs/DBs/DBSs/Volumes/regions/accounts/ResourceGroups/BillingFamilies
            switch (entityMemberType) {
                case EntityType.REGION_VALUE:
                    requestBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(leafMembers).build());
                    break;
                case EntityType.BUSINESS_ACCOUNT_VALUE:
                    requestBuilder.setAccountFilter(AccountFilter.newBuilder().addAllAccountId(leafMembers).build());
                    break;
                default:
                    requestBuilder.setEntityFilter(EntityFilter.newBuilder().addAllEntityId(leafMembers).build());
            }
        } else {
            logger.error("No valid entity member type found in group {}", groupUuid);
            return Collections.emptyList();
        }
        return queryCostServiceAndConvert(requestBuilder.build());
    }

    /**
     * Get cost stats for global scope, i.e. everything in cloud.
     *
     * @param costInputApiDTO query input DTO
     * @return list of StatSnapshotApiDTOs
     */
    @Nonnull
    public List<StatSnapshotApiDTO> getGlobalCostStats(@Nullable CostInputApiDTO costInputApiDTO) {
        final GetCloudBilledStatsRequest.Builder requestBuilder = buildGetCloudBilledStatsRequest(costInputApiDTO);
        requestBuilder.setMarket(Market.newBuilder().build());
        return queryCostServiceAndConvert(requestBuilder.build());
    }

    @VisibleForTesting
    protected List<StatSnapshotApiDTO> queryCostServiceAndConvert(GetCloudBilledStatsRequest request) {
        final List<StatSnapshotApiDTO> res = new ArrayList<>();
        final GetCloudBilledStatsResponse response = costServiceRpc.getCloudBilledStats(request);
        response.getBilledStatRecordList().forEach(record -> {
            final StatSnapshotApiDTO statDTO = new StatSnapshotApiDTO();
            if (record.hasSnapshotDate()) {
                statDTO.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
            }
            List<StatApiDTO> costStats = record.getStatRecordsList().stream()
                    .map(r -> {
                        final StatApiDTO statApiDTO = new StatApiDTO();
                        if (r.hasName()) {
                            statApiDTO.setName(r.getName());
                        }
                        for (TagKeyValuePair tagKeyValuePair : r.getTagList()) {
                            statApiDTO.addFilter(TAG_KEY, tagKeyValuePair.getKey());
                            statApiDTO.addFilter(TAG_VALUE, tagKeyValuePair.getValue());
                        }
                        if (r.hasValue()) {
                            final StatValue statValue = r.getValue();
                            StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
                            statValueApiDTO.setMax(statValue.getMax());
                            statValueApiDTO.setMin(statValue.getMin());
                            statValueApiDTO.setAvg(statValue.getAvg());
                            statValueApiDTO.setTotal(statValue.getTotal());
                            statApiDTO.setValues(statValueApiDTO);
                        }
                        return statApiDTO;
                    }).collect(Collectors.toList());
            statDTO.setStatistics(costStats);
            res.add(statDTO);
        });
        return res;
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
            for (TagApiDTO tagApiDTO : requestedTags) {
                if (tagApiDTO.getKey() != null) {
                    final TagFilter.Builder tagFilter = TagFilter.newBuilder().setTagKey(tagApiDTO.getKey());
                    if (tagApiDTO.getValues() != null) {
                        tagFilter.addAllTagValue(tagApiDTO.getValues());
                    }
                    requestBuilder.addTagFilter(tagFilter.build());
                }
            }
        }
        final List<CostGroupBy> costGroupBys = costInputApiDTO.getCostGroupBys();
        if (costGroupBys != null) {
            if (costGroupBys.size() != 1) {
                throw new UnsupportedOperationException("Currently only support one group by!");
            }
            costGroupBys.forEach(c -> requestBuilder.addGroupBy(convertCostGroupBy(c)));
        }
        return requestBuilder;
    }

    private GroupByType convertCostGroupBy(@Nonnull CostGroupBy costGroupBy) {
        switch (costGroupBy) {
            case TAG:
                return GroupByType.TAG;
            default:
                throw new UnsupportedOperationException(String.format("GroupBy % is not supported", costGroupBy));
        }
    }
}
