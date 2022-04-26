package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest.Builder;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Sub-query responsible for getting cloud commitment stats from the cost component.
 */
public class CloudCommitmentStatsSubQuery implements StatsSubQuery {

    private static final Set<String> SUPPORTED_STATS =
            ImmutableSet.of(StringConstants.CLOUD_COMMITMENT_UTILIZATION, StringConstants.CLOUD_COMMITMENT_COVERAGE);

    private static final Set<ApiEntityType> SUPPORTED_SCOPE_TYPES = ImmutableSet.<ApiEntityType>builder()
            .add(ApiEntityType.SERVICE_PROVIDER)
            .add(ApiEntityType.REGION)
            .add(ApiEntityType.BUSINESS_ACCOUNT)
            .add(ApiEntityType.VIRTUAL_MACHINE)
            .add(ApiEntityType.CLOUD_COMMITMENT)
            .build();

    private final CloudCommitmentStatsServiceBlockingStub cloudCommitmentStatsServiceGrpc;

    /**
     * Constructor for the CloudCommitmentsStatsSubQuery.
     *
     * @param cloudCommitmentStatsServiceGrpc the {@link com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc}
     */
    public CloudCommitmentStatsSubQuery(@Nonnull final CloudCommitmentStatsServiceBlockingStub cloudCommitmentStatsServiceGrpc) {
        this.cloudCommitmentStatsServiceGrpc = cloudCommitmentStatsServiceGrpc;
    }

    @Override
    public boolean applicableInContext(@Nonnull StatsQueryContext context) {

        boolean supportedScope = !context.getInputScope().getScopeTypes().isPresent()
                || context.getInputScope().getScopeTypes().get().stream().allMatch(SUPPORTED_SCOPE_TYPES::contains);
        // plans do not support cloud commitments at the moment
        boolean isNotPlan = !context.getInputScope().isPlan();
        return supportedScope && isNotPlan;
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull Set<StatApiInputDTO> stats, @Nonnull StatsQueryContext context)
            throws OperationFailedException, InterruptedException, ConversionException {

        final Map<Long, StatSnapshotApiDTO> snapshotTimestampMap = new ConcurrentHashMap<>();

        if (containsStat(StringConstants.CLOUD_COMMITMENT_UTILIZATION, stats)) {
            final Iterator<GetHistoricalCloudCommitmentUtilizationResponse> responseIterator =
                    cloudCommitmentStatsServiceGrpc.getHistoricalCommitmentUtilization(createHistoricalUtilizationRequest(context));
            while (responseIterator.hasNext()) {
                GetHistoricalCloudCommitmentUtilizationResponse response = responseIterator.next();
                List<CloudCommitmentStatRecord> statResponse = response.getCommitmentStatRecordChunkList();
                statResponse.forEach(statRecord -> {
                    final StatSnapshotApiDTO snapshot = snapshotTimestampMap.computeIfAbsent(statRecord.getSnapshotDate(), snaphotDate -> createSnapshotApi(snaphotDate, Epoch.HISTORICAL));
                    addRecordToSnapshot(snapshot, statRecord, StringConstants.CLOUD_COMMITMENT_UTILIZATION);
                });
            }
            final List<TopologyType> types = new ArrayList<>();
            if (context.includeCurrent()) {
                types.add(TopologyType.TOPOLOGY_TYPE_SOURCE);
            }
            if (context.requestProjected()) {
                types.add(TopologyType.TOPOLOGY_TYPE_PROJECTED);
            }
            for (TopologyType topologyType : types) {
                final GetTopologyCommitmentUtilizationStatsRequest request = createTopologyUtilizationRequest(context, topologyType);

                final Epoch epoch = topologyType == TopologyType.TOPOLOGY_TYPE_SOURCE ? Epoch.CURRENT : Epoch.PROJECTED;

                cloudCommitmentStatsServiceGrpc.getTopologyCommitmentUtilization(request)
                        .forEachRemaining(response -> response.getCommitmentUtilizationRecordChunkList().forEach(record -> {
                            final StatSnapshotApiDTO snapshot = snapshotTimestampMap.computeIfAbsent(record.getSnapshotDate(),
                                    snaphotDate -> createSnapshotApi(snaphotDate, epoch));
                            addRecordToSnapshot(snapshot, record, StringConstants.CLOUD_COMMITMENT_UTILIZATION);
                        }));
            }
        }
        if (containsStat(StringConstants.CLOUD_COMMITMENT_COVERAGE, stats)) {
            Iterator<GetHistoricalCommitmentCoverageStatsResponse> responseIterator = cloudCommitmentStatsServiceGrpc.getHistoricalCommitmentCoverageStats(createHistoricalCoverageRequest(context));
            while (responseIterator.hasNext()) {
                GetHistoricalCommitmentCoverageStatsResponse response = responseIterator.next();
                response.getCommitmentStatRecordChunkList().forEach(statRecord -> {
                    final StatSnapshotApiDTO snapshot = snapshotTimestampMap.computeIfAbsent(statRecord.getSnapshotDate(), snaphotDate -> createSnapshotApi(snaphotDate, Epoch.HISTORICAL));
                    addRecordToSnapshot(snapshot, statRecord, StringConstants.CLOUD_COMMITMENT_COVERAGE);
                });
            }
            final List<TopologyType> types = new ArrayList<>();
            if (context.includeCurrent()) {
                types.add(TopologyType.TOPOLOGY_TYPE_SOURCE);
            }
            if (context.requestProjected()) {
                types.add(TopologyType.TOPOLOGY_TYPE_PROJECTED);
            }
            for (TopologyType topologyType : types) {

                final GetTopologyCommitmentCoverageStatsRequest request = createTopologyCoverageRequest(context, topologyType);
                final Epoch epoch = topologyType == TopologyType.TOPOLOGY_TYPE_SOURCE ? Epoch.CURRENT : Epoch.PROJECTED;

                cloudCommitmentStatsServiceGrpc.getTopologyCommitmentCoverage(request)
                        .forEachRemaining(response -> response.getCommitmentCoverageStatChunkList().forEach(record -> {
                            final long snapShotDate = epoch == Epoch.PROJECTED ? (record.getSnapshotDate() + TimeUnit.HOURS.toMillis(1)) : record.getSnapshotDate();
                            final StatSnapshotApiDTO snapshot = snapshotTimestampMap.computeIfAbsent(snapShotDate,
                                    snaphotDate -> createSnapshotApi(snaphotDate, epoch));
                            addRecordToSnapshot(snapshot, record, StringConstants.CLOUD_COMMITMENT_COVERAGE);
                        }));
            }
        }
        return ImmutableList.copyOf(snapshotTimestampMap.values());
    }

    private StatValueApiDTO extractStatValue(CloudCommitmentStatRecord.StatValue value) {
        final StatValueApiDTO result = new StatValueApiDTO();
        result.setAvg((float)value.getAvg());
        result.setTotal((float)value.getTotal());
        result.setMax((float)value.getMax());
        result.setMin((float)value.getMin());
        return result;
    }

    @Nonnull
    private GetHistoricalCloudCommitmentUtilizationRequest createHistoricalUtilizationRequest(@Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        Builder reqBuilder = GetHistoricalCloudCommitmentUtilizationRequest.newBuilder();
        context.getTimeWindow().ifPresent(timeWindow -> {
            reqBuilder.setStartTime(timeWindow.startTime());
            reqBuilder.setEndTime(timeWindow.endTime());
        });
        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            // This is a set of scope oids filtered by CSP.
            for (Map.Entry<ApiEntityType, Set<Long>> entry : scopeEntitiesByType.entrySet()) {
                ApiEntityType currentType = entry.getKey();
                switch (currentType) {
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(
                                AccountFilter.newBuilder().addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
                        break;

                    case REGION:
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(scopeEntitiesByType.get(ApiEntityType.REGION)));
                        break;

                    case CLOUD_COMMITMENT:
                        reqBuilder.setCloudCommitmentFilter(CloudCommitmentFilter.newBuilder()
                                .addAllCloudCommitmentId(scopeEntitiesByType.get(ApiEntityType.CLOUD_COMMITMENT))
                                .build());
                        break;

                    case SERVICE_PROVIDER:
                        reqBuilder.setServiceProviderFilter(ServiceProviderFilter.newBuilder()
                                .addAllServiceProviderId(scopeEntitiesByType.get(ApiEntityType.SERVICE_PROVIDER))
                                .build());
                        break;

                    default:
                        throw new OperationFailedException(
                                String.format("Invalid scope for cloud commitment utilization query: %s", currentType));
                }
            }
        }
        return reqBuilder.build();
    }

    @Nonnull
    private GetTopologyCommitmentUtilizationStatsRequest createTopologyUtilizationRequest(@Nonnull final StatsQueryContext context,
                                                                                          @Nonnull TopologyType topologyType)
            throws OperationFailedException {

        final GetTopologyCommitmentUtilizationStatsRequest.Builder request =
                GetTopologyCommitmentUtilizationStatsRequest.newBuilder().setTopologyType(topologyType);

        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            // This is a set of scope oids filtered by CSP.
            for (Map.Entry<ApiEntityType, Set<Long>> entry : scopeEntitiesByType.entrySet()) {
                ApiEntityType currentType = entry.getKey();
                switch (currentType) {
                    case BUSINESS_ACCOUNT:
                        request.setAccountFilter(AccountFilter.newBuilder().addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
                        break;

                    case REGION:
                        request.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(scopeEntitiesByType.get(ApiEntityType.REGION)));
                        break;

                    case CLOUD_COMMITMENT:
                        request.setCloudCommitmentFilter(CloudCommitmentFilter.newBuilder()
                                .addAllCloudCommitmentId(scopeEntitiesByType.get(ApiEntityType.CLOUD_COMMITMENT))
                                .build());
                        break;

                    case SERVICE_PROVIDER:
                        request.setServiceProviderFilter(ServiceProviderFilter.newBuilder()
                                .addAllServiceProviderId(scopeEntitiesByType.get(ApiEntityType.SERVICE_PROVIDER))
                                .build());
                        break;

                    default:
                        throw new OperationFailedException(
                                String.format("Invalid scope for cloud commitment utilization query: %s", currentType));
                }
            }
        }

        return request.build();
    }

    @Nonnull
    private GetTopologyCommitmentCoverageStatsRequest createTopologyCoverageRequest(@Nonnull final StatsQueryContext context,
                                                                                    @Nonnull TopologyType topologyType)
            throws OperationFailedException {

        final GetTopologyCommitmentCoverageStatsRequest.Builder request =
                GetTopologyCommitmentCoverageStatsRequest.newBuilder().setTopologyType(topologyType);

        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            // This is a set of scope oids filtered by CSP.
            for (Map.Entry<ApiEntityType, Set<Long>> entry : scopeEntitiesByType.entrySet()) {
                ApiEntityType currentType = entry.getKey();
                switch (currentType) {
                    case BUSINESS_ACCOUNT:
                        request.setAccountFilter(AccountFilter.newBuilder().addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
                        break;

                    case REGION:
                        request.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(scopeEntitiesByType.get(ApiEntityType.REGION)));
                        break;

                    case SERVICE_PROVIDER:
                        request.setServiceProviderFilter(ServiceProviderFilter.newBuilder()
                                .addAllServiceProviderId(scopeEntitiesByType.get(ApiEntityType.SERVICE_PROVIDER))
                                .build());
                        break;

                    case VIRTUAL_MACHINE:
                        request.setEntityFilter(EntityFilter.newBuilder()
                                .addAllEntityId(scopeEntitiesByType.get(currentType))
                                .build());
                        break;

                    default:
                        throw new OperationFailedException(
                                String.format("Invalid scope for cloud commitment coverage query: %s", currentType));
                }
            }
        }

        return request.build();
    }

    @Nonnull
    private GetHistoricalCommitmentCoverageStatsRequest createHistoricalCoverageRequest(@Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        GetHistoricalCommitmentCoverageStatsRequest.Builder reqBuilder = GetHistoricalCommitmentCoverageStatsRequest.newBuilder();
        context.getTimeWindow().ifPresent(timeWindow -> {
            reqBuilder.setStartTime(timeWindow.startTime());
            reqBuilder.setEndTime(timeWindow.endTime());
        });
        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            for (Map.Entry<ApiEntityType, Set<Long>> entry : scopeEntitiesByType.entrySet()) {
                ApiEntityType currentType = entry.getKey();
                switch (currentType) {
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(
                                AccountFilter.newBuilder().addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
                        break;

                    case REGION:
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(scopeEntitiesByType.get(ApiEntityType.REGION)));
                        break;

                    case SERVICE_PROVIDER:
                        reqBuilder.setServiceProviderFilter(ServiceProviderFilter.newBuilder()
                                .addAllServiceProviderId(scopeEntitiesByType.get(ApiEntityType.SERVICE_PROVIDER))
                                .build());
                        break;

                    case VIRTUAL_MACHINE:
                        reqBuilder.setEntityFilter(EntityFilter.newBuilder()
                                .addAllEntityId(scopeEntitiesByType.get(currentType))
                                .build());
                        break;

                    default:
                        throw new OperationFailedException(
                                String.format("Invalid scope for cloud commitment coverage query: %s", currentType));
                }
            }
        }
        return reqBuilder.build();
    }

    @Nonnull
    private StatSnapshotApiDTO createSnapshotApi(long snapshotDate, @Nonnull Epoch epoch) {

        final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
        snapshotApiDTO.setDate(DateTimeUtil.toString(snapshotDate));
        snapshotApiDTO.setEpoch(epoch);
        // Initialize the list as mutable to add stats
        snapshotApiDTO.setStatistics(Lists.newLinkedList());

        return snapshotApiDTO;
    }

    private void addRecordToSnapshot(@Nonnull StatSnapshotApiDTO statSnapshot, @Nonnull CloudCommitmentStatRecord statRecord,
                                     @Nonnull String statName) {

        final StatApiDTO stat = new StatApiDTO();

        stat.setName(statName);
        stat.setCapacity(extractStatValue(statRecord.getCapacity()));
        stat.setValues(extractStatValue(statRecord.getValues()));

        final StatFilterApiDTO coverageTypeFilter = new StatFilterApiDTO();
        coverageTypeFilter.setType("CoverageType");

        final CloudCommitmentCoverageTypeInfo vectorTypeInfo = statRecord.getCoverageTypeInfo();
        switch (vectorTypeInfo.getCoverageType()) {
            case SPEND_COMMITMENT:
                stat.setUnits(CostProtoUtil.getCurrencyUnit(vectorTypeInfo.getCoverageSubtype()));
                coverageTypeFilter.setValue("SPEND");
                break;
            case COUPONS:
                stat.setUnits(StringConstants.NUMBER_OF_COUPONS);
                coverageTypeFilter.setValue(StringConstants.NUMBER_OF_COUPONS);
                break;
            case COMMODITY:

                stat.setUnits(CommodityTypeMapping.getUnitForCommodityType(vectorTypeInfo.getCoverageSubtype()));

                final CommodityType commodityType = CommodityType.forNumber(vectorTypeInfo.getCoverageSubtype());
                coverageTypeFilter.setValue(String.format("Commodity::%s", commodityType.name()));
                break;
        }
        stat.setFilters(ImmutableList.of(coverageTypeFilter));

        statSnapshot.getStatistics().add(stat);
    }
}