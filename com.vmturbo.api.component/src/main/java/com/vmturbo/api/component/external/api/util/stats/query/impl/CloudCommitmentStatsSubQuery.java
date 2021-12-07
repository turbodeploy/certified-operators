package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest.Builder;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse.CloudCommitmentUtilizationRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Sub-query responsible for getting cloud commitment stats from the cost component.
 */
public class CloudCommitmentStatsSubQuery implements StatsSubQuery {

    private static final Set<String> SUPPORTED_STATS =
            ImmutableSet.of(StringConstants.CLOUD_COMMITMENT_UTILIZATION, StringConstants.CLOUD_COMMITMENT_COVERAGE);

    private final CloudCommitmentStatsServiceBlockingStub cloudCommitmentStatsServiceGrpc;

    /**
     * Constructor for the CloudCommitmentsStatsSubQuery.
     *
     * @param cloudCommitmentStatsServiceGrpc the {@link com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc}
     */
    public CloudCommitmentStatsSubQuery(@Nonnull final CloudCommitmentStatsServiceBlockingStub  cloudCommitmentStatsServiceGrpc) {
        this.cloudCommitmentStatsServiceGrpc = cloudCommitmentStatsServiceGrpc;
    }

    @Override
    public boolean applicableInContext(@Nonnull StatsQueryContext context) {
        // plans do not support cloud commitments at the moment
        return !context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull Set<StatApiInputDTO> stats,
            @Nonnull StatsQueryContext context)
            throws OperationFailedException, InterruptedException, ConversionException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();
        if (containsStat(StringConstants.CLOUD_COMMITMENT_UTILIZATION, stats)) {
            final Iterator<GetHistoricalCloudCommitmentUtilizationResponse> responseIterator =
                    cloudCommitmentStatsServiceGrpc.getHistoricalCommitmentUtilization(createHistoricalUtilizationRequest(context));
            while (responseIterator.hasNext()) {
                GetHistoricalCloudCommitmentUtilizationResponse response = responseIterator.next();
                List<CloudCommitmentStatRecord> statResponse = response.getCommitmentStatRecordChunkList();
                snapshots.addAll(convertCloudCommitmentStatRecordsToStatsDTO(statResponse,
                        StringConstants.CLOUD_COMMITMENT_UTILIZATION));
            }
            final List<TopologyType> types = new ArrayList<>();
            if (context.includeCurrent()) {
                types.add(TopologyType.TOPOLOGY_TYPE_SOURCE);
            }
            if (context.requestProjected()) {
                types.add(TopologyType.TOPOLOGY_TYPE_PROJECTED);
            }
            types.forEach(topologyType -> {
                final GetTopologyCommitmentUtilizationStatsRequest request =
                        GetTopologyCommitmentUtilizationStatsRequest.newBuilder()
                                .setTopologyType(topologyType).build();
                cloudCommitmentStatsServiceGrpc.getTopologyCommitmentUtilization(request)
                        .forEachRemaining(response ->
                                response.getCommitmentUtilizationRecordChunkList().forEach(chunk ->
                                        snapshots.add(makeUtilizationSnapshot(topologyType, chunk))));
            });

        }
        if (containsStat(StringConstants.CLOUD_COMMITMENT_COVERAGE, stats)) {
            Iterator<GetHistoricalCommitmentCoverageStatsResponse> responseIterator = cloudCommitmentStatsServiceGrpc
                    .getHistoricalCommitmentCoverageStats(createHistoricalCoverageRequest(context));
            while (responseIterator.hasNext()) {
                GetHistoricalCommitmentCoverageStatsResponse response = responseIterator.next();
                List<CloudCommitmentStatRecord> statResponse = response.getCommitmentStatRecordChunkList();
                snapshots.addAll(convertCloudCommitmentStatRecordsToStatsDTO(statResponse,
                        StringConstants.CLOUD_COMMITMENT_COVERAGE));
            }
            final List<TopologyType> types = new ArrayList<>();
            if (context.includeCurrent()) {
                types.add(TopologyType.TOPOLOGY_TYPE_SOURCE);
            }
            if (context.requestProjected()) {
                types.add(TopologyType.TOPOLOGY_TYPE_PROJECTED);
            }
            types.forEach(topologyType -> {
                final GetTopologyCommitmentCoverageStatsRequest request =
                        GetTopologyCommitmentCoverageStatsRequest.newBuilder()
                                .setTopologyType(topologyType)
                                .build();
                cloudCommitmentStatsServiceGrpc.getTopologyCommitmentCoverage(request)
                        .forEachRemaining(response ->
                                response.getCommitmentCoverageStatChunkList().forEach(chunk -> {
                                    final StatApiDTO stat = makeInnerStat(chunk.getCapacity(), chunk.getUsed());
                                    stat.setName(StringConstants.CLOUD_COMMITMENT_COVERAGE);
                                    snapshots.add(makeTopologySnapshot(topologyType, stat));
                                })
                        );
            });
        }
        return snapshots;
    }

    private StatSnapshotApiDTO makeUtilizationSnapshot(TopologyType topologyType, CloudCommitmentUtilizationRecord chunk) {
        final StatApiDTO stat = makeInnerStat(chunk.getUtilization().getCapacity(), chunk.getUtilization().getUsed());
        stat.setName(StringConstants.CLOUD_COMMITMENT_UTILIZATION);
        if (chunk.hasCommitmentOid()) {
            final BaseApiDTO relatedCloudCommitment = new BaseApiDTO();
            relatedCloudCommitment.setUuid(String.valueOf(chunk.getCommitmentOid()));
            relatedCloudCommitment.setClassName(StringConstants.CLOUD_COMMITMENT);
            stat.setRelatedEntityType(StringConstants.CLOUD_COMMITMENT);
            stat.setRelatedEntity(relatedCloudCommitment);
        }
        stat.setReserved(extractStatValue(chunk.getUtilization().getOverhead()));
        return makeTopologySnapshot(topologyType, stat);
    }

    private StatSnapshotApiDTO makeTopologySnapshot(@Nonnull TopologyType topologyType, StatApiDTO innerStat) {
        final StatSnapshotApiDTO result = new StatSnapshotApiDTO();
        if (topologyType == TopologyType.TOPOLOGY_TYPE_SOURCE) {
            result.setEpoch(Epoch.CURRENT);
        }
        if (topologyType == TopologyType.TOPOLOGY_TYPE_PROJECTED) {
            result.setEpoch(Epoch.PROJECTED);
        }
        result.setStatistics(Collections.singletonList(innerStat));
        return result;
    }

    private StatValueApiDTO extractStatValue(CloudCommitmentAmount amount) {
        final StatValueApiDTO result = new StatValueApiDTO();
        if (amount.hasAmount()) {
            float value = (float)amount.getAmount().getAmount();
            result.setAvg(value);
            result.setTotal(value);
            result.setMin(value);
            result.setMax(value);
        } else if (amount.hasCoupons()) {
            float value = (float)amount.getCoupons();
            result.setAvg(value);
            result.setTotal(value);
            result.setMin(value);
            result.setMax(value);
        }
        return result;
    }

    private StatApiDTO makeInnerStat(CloudCommitmentAmount capacity, CloudCommitmentAmount used) {
        final StatApiDTO stat = new StatApiDTO();
        if (capacity.hasAmount()) {
            stat.setUnits(CostProtoUtil.getCurrencyUnit(capacity.getAmount()));
        } else if (capacity.hasCoupons()) {
            stat.setUnits(StringConstants.NUMBER_OF_COUPONS);
        }
        stat.setCapacity(extractStatValue(capacity));
        stat.setValues(extractStatValue(used));
        return stat;
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
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
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
                                "Invalid scope for cloud commitment utilization query. Must be global or have an entity type.");
                }
            }
        }
        return reqBuilder.build();
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
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntitiesByType.get(ApiEntityType.BUSINESS_ACCOUNT)));
                        break;

                    case REGION:
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(scopeEntitiesByType.get(ApiEntityType.REGION)));
                        break;

                    case SERVICE_PROVIDER:
                        reqBuilder.setServiceProviderFilter(ServiceProviderFilter.newBuilder()
                                .addAllServiceProviderId(scopeEntitiesByType.get(ApiEntityType.SERVICE_PROVIDER))
                                .build());
                        break;

                    default:
                        throw new OperationFailedException(
                                "Invalid scope for cloud commitment coverage query. Must be global or have an entity type.");
                }
            }
        }
        return reqBuilder.build();
    }


    private List<StatSnapshotApiDTO> convertCloudCommitmentStatRecordsToStatsDTO(List<CloudCommitmentStatRecord> records, String statName)
            throws OperationFailedException {
        List<StatSnapshotApiDTO> statSnapshotApiDTOS = new ArrayList<>();
        for (CloudCommitmentStatRecord record: records) {
            final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
            snapshotApiDTO.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
            snapshotApiDTO.setEpoch(Epoch.HISTORICAL);
            StatValueApiDTO statsValueDto = new StatValueApiDTO();
            final ValueCase valueCase = record.getValues().getAvg().getValueCase();
            setStatCapacityAndValues(statsValueDto, valueCase, record.getValues());
            StatValueApiDTO capacityDto = new StatValueApiDTO();
            final ValueCase capacityValueCase = record.getCapacity().getAvg().getValueCase();
            setStatCapacityAndValues(capacityDto, capacityValueCase, record.getCapacity());
            StatApiDTO statsDto = new StatApiDTO();
            statsDto.setValues(statsValueDto);
            statsDto.setCapacity(capacityDto);
            statsDto.setName(statName);
            statsDto.setUnits(StringConstants.DOLLARS_PER_DAY);
            snapshotApiDTO.setStatistics(Lists.newArrayList(statsDto));
            statSnapshotApiDTOS.add(snapshotApiDTO);
        }
        return statSnapshotApiDTOS;
    }

    private void setStatCapacityAndValues(StatValueApiDTO statsValueDto, ValueCase valueCase, StatValue statValue)
            throws OperationFailedException {
        switch (valueCase) {
            case AMOUNT:
                statsValueDto.setAvg((float)statValue.getAvg().getAmount().getAmount());
                statsValueDto.setMax((float)statValue.getMax().getAmount().getAmount());
                statsValueDto.setMin((float)statValue.getMin().getAmount().getAmount());
                statsValueDto.setTotal((float)statValue.getTotal().getAmount().getAmount());
                break;
            case COUPONS:
                statsValueDto.setAvg((float)statValue.getAvg().getCoupons());
                statsValueDto.setMax((float)statValue.getMax().getCoupons());
                statsValueDto.setMin((float)statValue.getMin().getCoupons());
                statsValueDto.setTotal((float)statValue.getTotal().getCoupons());
                break;
            case VALUE_NOT_SET:
                throw new OperationFailedException(
                        "Invalid type for cloud commitment capacity in the stat record");
        }
    }
}
