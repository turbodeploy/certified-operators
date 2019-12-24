package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sub-query responsible for getting reserved instance stats from the cost component.
 */
public class RIStatsSubQuery implements StatsSubQuery {
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;
    private final ReservedInstanceBoughtServiceBlockingStub riBoughtService;

    private final RIStatsMapper riStatsMapper;
    private final RepositoryApi repositoryApi;

    private static final Set<String> SUPPORTED_STATS =
            ImmutableSet.of(StringConstants.RI_COUPON_UTILIZATION,
                    StringConstants.RI_COUPON_COVERAGE, StringConstants.NUM_RI);

    public RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
                           @Nonnull final ReservedInstanceBoughtServiceBlockingStub riBoughtService,
                           @Nonnull final RepositoryApi repositoryApi) {
        this(riUtilizationCoverageService, riBoughtService, new RIStatsMapper(), repositoryApi);
    }

    RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
                    @Nonnull final ReservedInstanceBoughtServiceBlockingStub riBoughtService,
                    @Nonnull final RIStatsMapper riStatsMapper,
                    @Nonnull final RepositoryApi repositoryApi) {
        this.riUtilizationCoverageService = riUtilizationCoverageService;
        this.riBoughtService = riBoughtService;
        this.riStatsMapper = riStatsMapper;
        this.repositoryApi = repositoryApi;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // related entity types that we support RI stats for
        List<UIEntityType> validEntityTypesForRIStats = new ArrayList<>();
        validEntityTypesForRIStats.add(UIEntityType.REGION);
        validEntityTypesForRIStats.add(UIEntityType.AVAILABILITY_ZONE);
        validEntityTypesForRIStats.add(UIEntityType.BUSINESS_ACCOUNT);
        validEntityTypesForRIStats.add(UIEntityType.VIRTUAL_MACHINE);
        // Doesn't seem like it should support plan, because the backend doesn't allow specifying
        // the plan ID.
        UIEntityType relatedEntityType;
        if (context.getQueryScope().getGlobalScope().isPresent()
                && !context.getQueryScope().getGlobalScope().get().entityTypes().isEmpty()) {
            // if related entity type doesn't support RI stats, we don't go through the query
            relatedEntityType = context.getQueryScope().getGlobalScope().get().entityTypes().iterator().next();
            return !context.getInputScope().isPlan() && validEntityTypesForRIStats.contains(relatedEntityType);
        }
        return !context.getInputScope().isPlan();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();
        if (containsStat(StringConstants.NUM_RI, stats) && isValidScopeForNumRIRequest(context)) {
            final GetReservedInstanceBoughtCountRequest countRequest = riStatsMapper
                    .createRIBoughtCountRequest(context);

            GetReservedInstanceBoughtCountByTemplateResponse response = riBoughtService
                    .getReservedInstanceBoughtCountByTemplateType(countRequest).toBuilder().build();

            final Map<Long, Long> riBoughtCountsByTierId = response.getReservedInstanceCountMapMap();

            final Map<Long, ServiceEntityApiDTO> tierApiDTOByTierId =
                    repositoryApi.entitiesRequest(riBoughtCountsByTierId.keySet())
                    .getSEMap();

            final Map<String, Long> riBoughtCountByTierName = riBoughtCountsByTierId
                    .entrySet().stream()
                    .filter(e -> tierApiDTOByTierId.containsKey(e.getKey()))
                    .collect(Collectors
                            .toMap(e -> tierApiDTOByTierId.get(e.getKey()).getDisplayName(),
                                    Entry::getValue, Long::sum));
            snapshots.addAll(riStatsMapper
                    .convertNumRIStatsRecordsToStatSnapshotApiDTO(riBoughtCountByTierName));
        }

        if (containsStat(StringConstants.RI_COUPON_COVERAGE, stats) && isValidScopeForCoverageRequest(context)) {
            final GetReservedInstanceCoverageStatsRequest coverageRequest =
                riStatsMapper.createCoverageRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                riUtilizationCoverageService.getReservedInstanceCoverageStats(coverageRequest)
                    .getReservedInstanceStatsRecordsList(), true));
        }

        if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats) && isValidScopeForUtilizationRequest(context)) {
            final GetReservedInstanceUtilizationStatsRequest utilizationRequest =
                riStatsMapper.createUtilizationRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                riUtilizationCoverageService.getReservedInstanceUtilizationStats(utilizationRequest)
                .getReservedInstanceStatsRecordsList(), false));
        }

        return snapshots.stream()
            .collect(Collectors.toMap(snapshot -> DateTimeUtil.parseTime(snapshot.getDate()),
                Function.identity(), (v1, v2) -> {
                    // Merge stats lists with the same date.
                    final List<StatApiDTO> stats1 = v1.getStatistics();
                    final List<StatApiDTO> stats2 = v2.getStatistics();
                    final List<StatApiDTO> combinedList =
                        new ArrayList<>(stats1.size() + stats2.size());
                    combinedList.addAll(stats1);
                    combinedList.addAll(stats2);
                    v1.setStatistics(combinedList);
                    return v1;
                })).values().stream().collect(Collectors.toList());
    }

    /**
     * Check if valid scope for RI Utilization request.
     * @param context - query context
     * @return boolean
     */
    private boolean isValidScopeForUtilizationRequest(@Nonnull final StatsQueryContext context) {
        if (context.getInputScope().getScopeTypes().isPresent()) {
            final UIEntityType type = context.getInputScope().getScopeTypes().get()
                    .iterator().next();
            if (type == UIEntityType.REGION || type == UIEntityType.AVAILABILITY_ZONE ||
            type == UIEntityType.BUSINESS_ACCOUNT) {
                return true;
            }
        } else if (context.isGlobalScope()) {
           return true;
        }

        return false;
    }

    /**
     * Check if valid scope for numRI request.
     * @param context - query context
     * @return boolean
     */
    private boolean isValidScopeForNumRIRequest(@Nonnull final StatsQueryContext context) {
        if (context.getInputScope().getScopeTypes().isPresent()) {
            final UIEntityType type = context.getInputScope().getScopeTypes().get()
                    .iterator().next();
            if (type == UIEntityType.REGION || type == UIEntityType.AVAILABILITY_ZONE ||
                    type == UIEntityType.BUSINESS_ACCOUNT) {
                return true;
            }
        } else if (context.isGlobalScope()) {
            return true;
        }

        return false;
    }

    /**
     * Check if valid scope for RI Coverage request.
     * @param context - query context
     * @return boolean
     */
    private boolean isValidScopeForCoverageRequest(@Nonnull final StatsQueryContext context) {
        if (context.getInputScope().getScopeTypes().isPresent()
                && !context.getInputScope().getScopeTypes().get().isEmpty()) {
            if (context.getInputScope().getScopeTypes().get().size() == 1) {
                return true;
            }
        } else if (context.isGlobalScope()) {
            return true;
        }
        return false;
    }

    @VisibleForTesting
    static class RIStatsMapper {

        @Nonnull
        GetReservedInstanceBoughtCountRequest createRIBoughtCountRequest(
                @Nonnull final StatsQueryContext context) throws OperationFailedException {
            final GetReservedInstanceBoughtCountRequest.Builder reqBuilder =
                                        GetReservedInstanceBoughtCountRequest.newBuilder();
            if (context.getInputScope().getScopeTypes().isPresent()) {
                final Set<Long> scopeEntities = new HashSet<>();
                if (context.getInputScope().isGroup()) {
                    if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                        scopeEntities.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                    }
                } else {
                    scopeEntities.add(context.getInputScope().oid());
                }
                final Set<UIEntityType> uiEntityTypes = context.getInputScope().getScopeTypes().get();

                if (CollectionUtils.isEmpty(uiEntityTypes)) {
                    throw new OperationFailedException("Entity type not present");
                }
                final UIEntityType type = uiEntityTypes.stream().findFirst().get();

                switch (type) {
                    case REGION :
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(scopeEntities));
                        break;
                    case AVAILABILITY_ZONE:
                        reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(scopeEntities));
                        break;
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntities));
                        break;
                    default:
                        throw new OperationFailedException("Invalid scope for query: " + type.apiStr());
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid scope for query." +
                        " Must be global or have an entity type.");
            }
            return reqBuilder.build();
        }

        @Nonnull
        GetReservedInstanceUtilizationStatsRequest createUtilizationRequest(@Nonnull final StatsQueryContext context)
                throws OperationFailedException {
            final GetReservedInstanceUtilizationStatsRequest.Builder reqBuilder =
                GetReservedInstanceUtilizationStatsRequest.newBuilder();
            context.getTimeWindow().ifPresent(timeWindow -> {
                reqBuilder.setStartDate(timeWindow.startTime());
                reqBuilder.setEndDate(timeWindow.endTime());
            });

            if (context.getInputScope().getScopeTypes().isPresent()) {
                final Set<Long> scopeEntities = new HashSet<>();
                if (context.getInputScope().isGroup()) {
                    if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                        scopeEntities.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                    }
                } else {
                    scopeEntities.add(context.getInputScope().oid());
                }
                final Set<UIEntityType> uiEntityTypes = context.getInputScope().getScopeTypes().get();

                if (CollectionUtils.isEmpty(uiEntityTypes)) {
                    throw new OperationFailedException("Entity type not present");
                }
                final UIEntityType type = uiEntityTypes.stream().findFirst().get();

                switch (type) {
                    case REGION :
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(scopeEntities));
                        break;
                    case AVAILABILITY_ZONE:
                        reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(scopeEntities));
                        break;
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntities));
                        break;
                    default:
                        throw new OperationFailedException("Invalid scope for query: " + type.apiStr());
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid scope for query." +
                    " Must be global or have an entity type.");
            }

            return reqBuilder.build();
        }

        @Nonnull
        GetReservedInstanceCoverageStatsRequest createCoverageRequest(
                @Nonnull final StatsQueryContext context) throws OperationFailedException {
            final GetReservedInstanceCoverageStatsRequest.Builder reqBuilder =
                GetReservedInstanceCoverageStatsRequest.newBuilder();
            context.getTimeWindow().ifPresent(timeWindow -> {
                reqBuilder.setStartDate(timeWindow.startTime());
                reqBuilder.setEndDate(timeWindow.endTime());
            });

            if (context.getInputScope().getScopeTypes().isPresent()
                            && !context.getInputScope().getScopeTypes().get().isEmpty()) {

                final Set<Long> scopeEntities = new HashSet<>();
                if (context.getInputScope().isGroup()) {
                    if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                        scopeEntities.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                    }
                } else {
                    scopeEntities.add(context.getInputScope().oid());
                }

                if (context.getInputScope().getScopeTypes().get().size() != 1) {
                    //TODO (mahdi) Change the logic to support scopes with more than one type
                    throw new IllegalStateException("Scopes with more than one type is not supported.");
                }

                final Set<UIEntityType> uiEntityTypes = context.getInputScope().getScopeTypes().get();

                if (CollectionUtils.isEmpty(uiEntityTypes)) {
                    throw new OperationFailedException("Entity type not present");
                }
                final UIEntityType type = uiEntityTypes.stream().findFirst().get();

                switch (type) {
                    case REGION :
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(scopeEntities));
                        break;
                    case AVAILABILITY_ZONE:
                        reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(scopeEntities));
                        break;
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntities));
                        break;
                    default:
                        reqBuilder.setEntityFilter(EntityFilter.newBuilder()
                                .addAllEntityId(context.getQueryScope().getExpandedOids()));
                        break;
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid context - must be global or have entity type");
            }
            return reqBuilder.build();
        }

        /**
         * Convert numRI records to StatSnapshotApiDTO
         * @param records - map containing template types and counts from users RI inventory
         * @return a list {@link StatSnapshotApiDTO}
         */
        public List<StatSnapshotApiDTO> convertNumRIStatsRecordsToStatSnapshotApiDTO(
                @Nonnull final Map<String, Long> records) {
            final List<StatSnapshotApiDTO> response = new ArrayList<>();
            final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
            snapshotApiDTO.setDate(DateTimeUtil.toString(Clock.systemUTC().millis()));
            snapshotApiDTO.setEpoch(Epoch.CURRENT);
            List<StatApiDTO> statApiDTOList = new ArrayList<>();
            for (String template : records.keySet()) {
                statApiDTOList.add(createNumRIStatApiDTO(template, records.get(template)));
            }
            snapshotApiDTO.setStatistics(statApiDTOList);
            response.add(snapshotApiDTO);
            return response;
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
                    // TODO: Can these be projected?
                    snapshotApiDTO.setEpoch(Epoch.HISTORICAL);
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

        /**
         * Create StatApiDTO for NumRI stats.
         *
         * @param template - template type key
         * @param count - number of RIs in users inventory for given template type
         * @return a {@link StatApiDTO}
         */
        private StatApiDTO createNumRIStatApiDTO(@Nonnull String template, @Nonnull Long count) {
            StatApiDTO statsDto = new StatApiDTO();
            statsDto.setValue((float)count);
            statsDto.setName(StringConstants.NUM_RI);
            StatValueApiDTO statsValueDto = new StatValueApiDTO();
            statsValueDto.setAvg((float)count);
            statsValueDto.setMax((float)count);
            statsValueDto.setMin((float)count);
            statsValueDto.setTotal((float)count);
            statsDto.setValues(statsValueDto);
            List<StatFilterApiDTO> filterList = new ArrayList<>();
            StatFilterApiDTO filterDto = new StatFilterApiDTO();
            filterDto.setType(StringConstants.TEMPLATE);
            filterDto.setValue(template);
            filterList.add(filterDto);
            statsDto.setFilters(filterList);
            return statsDto;
        }
    }
}
