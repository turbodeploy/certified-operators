package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sub-query responsible for getting reserved instance stats from the cost component.
 */
public class RIStatsSubQuery implements StatsSubQuery {
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;

    private final RIStatsMapper riStatsMapper;

    private static final Set<String> SUPPORTED_STATS =
        ImmutableSet.of(StringConstants.RI_COUPON_UTILIZATION,
            StringConstants.RI_COUPON_COVERAGE);

    public RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService) {
        this(riUtilizationCoverageService, new RIStatsMapper());
    }

    RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
                    @Nonnull final RIStatsMapper riStatsMapper) {
        this.riUtilizationCoverageService = riUtilizationCoverageService;
        this.riStatsMapper = riStatsMapper;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // Doesn't seem like it should support plan, because the backend doesn't allow specifying
        // the plan ID.
        return !context.getInputScope().isPlan() &&
            // Right now we don't support retrieving "latest" RI stats - we expect the UI/API
            // to pass in some values.
            context.getTimeWindow().isPresent();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();
        if (containsStat(StringConstants.RI_COUPON_COVERAGE, stats)) {
            final GetReservedInstanceCoverageStatsRequest coverageRequest =
                riStatsMapper.createCoverageRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                riUtilizationCoverageService.getReservedInstanceCoverageStats(coverageRequest)
                    .getReservedInstanceStatsRecordsList(), true));
        }

        if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
            final GetReservedInstanceUtilizationStatsRequest utilizationRequest =
                riStatsMapper.createUtilizationRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                riUtilizationCoverageService.getReservedInstanceUtilizationStats(utilizationRequest)
                .getReservedInstanceStatsRecordsList(), false));
        }

        return snapshots.stream()
            .collect(Collectors.toMap(snapshot -> DateTimeUtil.parseTime(snapshot.getDate()),
                StatSnapshotApiDTO::getStatistics, (v1, v2) -> {
                    // Merge stats lists with the same date.
                    final List<StatApiDTO> combinedList =
                        new ArrayList<>(v1.size() + v2.size());
                    combinedList.addAll(v1);
                    combinedList.addAll(v2);
                    return combinedList;
                }));
    }

    @VisibleForTesting
    static class RIStatsMapper {

        @Nonnull
        GetReservedInstanceUtilizationStatsRequest createUtilizationRequest(@Nonnull final StatsQueryContext context)
                throws OperationFailedException {
            final GetReservedInstanceUtilizationStatsRequest.Builder reqBuilder =
                GetReservedInstanceUtilizationStatsRequest.newBuilder();
            context.getTimeWindow().ifPresent(timeWindow -> {
                reqBuilder.setStartDate(timeWindow.startTime());
                reqBuilder.setEndDate(timeWindow.endTime());
            });

            if (context.getInputScope().getScopeType().isPresent()) {
                final UIEntityType type = context.getInputScope().getScopeType().get();
                final Set<Long> scopeEntities = context.getQueryScope().getEntities();
                if (type == UIEntityType.REGION) {
                    reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeEntities));
                } else if (type == UIEntityType.AVAILABILITY_ZONE) {
                    reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeEntities));
                } else if (type == UIEntityType.BUSINESS_ACCOUNT) {
                    reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                        .addAllAccountId(scopeEntities));
                } else {
                    throw new OperationFailedException("Invalid scope for query: " + type.apiStr());
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid scope for query." +
                    " Must be global or have an entity type.");
            }

            return reqBuilder.build();
        }

        @Nonnull
        GetReservedInstanceCoverageStatsRequest createCoverageRequest(@Nonnull final StatsQueryContext context) throws OperationFailedException {
            final GetReservedInstanceCoverageStatsRequest.Builder reqBuilder =
                GetReservedInstanceCoverageStatsRequest.newBuilder();
            context.getTimeWindow().ifPresent(timeWindow -> {
                reqBuilder.setStartDate(timeWindow.startTime());
                reqBuilder.setEndDate(timeWindow.endTime());
            });

            if (context.getInputScope().getScopeType().isPresent()) {
                final UIEntityType type = context.getInputScope().getScopeType().get();
                final Set<Long> scopeEntities = context.getQueryScope().getEntities();
                if (type == UIEntityType.REGION) {
                    reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeEntities));
                } else if (type == UIEntityType.AVAILABILITY_ZONE) {
                    reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeEntities));
                } else if (type == UIEntityType.BUSINESS_ACCOUNT) {
                    reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                        .addAllAccountId(scopeEntities));
                } else {
                    reqBuilder.setEntityFilter(EntityFilter.newBuilder()
                        .addAllEntityId(scopeEntities));
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid context - must be global or have entity type");
            }
            return reqBuilder.build();
        }

        /**
         * Convert a list of {@link ReservedInstanceStatsRecord} to a list of {@link StatSnapshotApiDTO}.
         *
         * @param records a list of {@link ReservedInstanceStatsRecord}.
         * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
         *                     false means it's a reserved instance utilization stats request.
         * @return a list {@link ReservedInstanceStatsRecord}.
         */
        public List<StatSnapshotApiDTO> convertRIStatsRecordsToStatSnapshotApiDTO(
            @Nonnull final List<ReservedInstanceStatsRecord> records,
            final boolean isRICoverage) {
            return records.stream()
                .map(record -> {
                    final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
                    snapshotApiDTO.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
                    final StatApiDTO statApiDTO = createRIUtilizationStatApiDTO(record, isRICoverage);
                    snapshotApiDTO.setStatistics(Lists.newArrayList(statApiDTO));
                    return snapshotApiDTO;
                })
                .collect(Collectors.toList());
        }

        /**
         * Create a {@link StatApiDTO} from input {@link ReservedInstanceStatsRecord}.
         *
         * @param record a {@link ReservedInstanceStatsRecord}.
         * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
         *                     false means it's a reserved instance utilization stats request.
         * @return a {@link StatApiDTO}.
         */
        private StatApiDTO createRIUtilizationStatApiDTO(@Nonnull final ReservedInstanceStatsRecord record,
                                                         final boolean isRICoverage) {
            final String name = isRICoverage ? StringConstants.RI_COUPON_COVERAGE : StringConstants.RI_COUPON_UTILIZATION;
            StatApiDTO statsDto = new StatApiDTO();
            StatValueApiDTO statsValueDto = new StatValueApiDTO();
            statsValueDto.setAvg(record.getValues().getAvg());
            statsValueDto.setMax(record.getValues().getMax());
            statsValueDto.setMin(record.getValues().getMin());
            statsValueDto.setTotal(record.getValues().getTotal());
            statsDto.setValues(statsValueDto);
            StatValueApiDTO capacityDto = new StatValueApiDTO();
            capacityDto.setAvg(record.getCapacity().getAvg());
            capacityDto.setMax(record.getCapacity().getMax());
            capacityDto.setMin(record.getCapacity().getMin());
            capacityDto.setTotal(record.getCapacity().getTotal());
            statsDto.setCapacity(capacityDto);
            statsDto.setUnits(StringConstants.RI_COUPON_UNITS);
            statsDto.setName(name);
            statsDto.setValue(record.getValues().getAvg());
            return statsDto;
        }
    }
}
