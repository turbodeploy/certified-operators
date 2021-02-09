package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Abstract sub-query responsible for getting reserved instance stats from the cost component.
 */
public abstract class AbstractRIStatsSubQuery implements StatsSubQuery {
    private static final Set<String> SUPPORTED_STATS =
                    ImmutableSet.of(StringConstants.RI_COUPON_UTILIZATION,
                                    StringConstants.RI_COUPON_COVERAGE, StringConstants.NUM_RI,
                                    StringConstants.RI_COST);

    private final RepositoryApi repositoryApi;
    private final BuyRiScopeHandler buyRiScopeHandler;

    /**
     * Creates {@link AbstractRIStatsSubQuery} instance.
     *
     * @param repositoryApi repository API.
     * @param buyRiScopeHandler buy RI scope handler.
     */
    public AbstractRIStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
            @Nonnull final BuyRiScopeHandler buyRiScopeHandler) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.buyRiScopeHandler = Objects.requireNonNull(buyRiScopeHandler);
    }

    @Override
    public SubQuerySupportedStats getHandledStats(final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    /**
     * Get repository API.
     *
     * @return {@link RepositoryApi}
     */
    public RepositoryApi getRepositoryApi() {
        return repositoryApi;
    }

    /**
     * Get buy RI scope handler.
     *
     * @return {@link BuyRiScopeHandler}
     */
    public BuyRiScopeHandler getBuyRiScopeHandler() {
        return buyRiScopeHandler;
    }

    /**
     * Merge stats by date.
     *
     * @param snapshots list of stats to merge.
     * @return list of merged {@link StatSnapshotApiDTO}.
     */
    protected static List<StatSnapshotApiDTO> mergeStatsByDate(
            final List<StatSnapshotApiDTO> snapshots) {
        return new ArrayList<>(snapshots.stream()
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
                        }))
                .values());
    }

    /**
     * Convert numRI records to StatSnapshotApiDTO.
     *
     * @param numRIStatsMap - map containing template types and counts from users RI inventory
     * @return a list {@link StatSnapshotApiDTO}
     */
    protected static List<StatSnapshotApiDTO> convertNumRIStatsMapToStatSnapshotApiDTO(
        @Nonnull final Map<String, Long> numRIStatsMap) {
        final List<StatSnapshotApiDTO> response = new ArrayList<>();
        final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
        snapshotApiDTO.setDate(DateTimeUtil.toString(Clock.systemUTC().millis()));
        snapshotApiDTO.setEpoch(Epoch.CURRENT);
        final List<StatApiDTO> statApiDTOList = new ArrayList<>();
        for (String template : numRIStatsMap.keySet()) {
            statApiDTOList.add(createNumRIStatApiDTO(template, numRIStatsMap.get(template)));
        }
        snapshotApiDTO.setStatistics(statApiDTOList);
        response.add(snapshotApiDTO);
        return response;
    }

    /**
     * Create StatApiDTO for NumRI stats.
     *
     * @param template - template type key
     * @param count - number of RIs in users inventory for given template type
     * @return a {@link StatApiDTO}
     */
    private static StatApiDTO createNumRIStatApiDTO(@Nonnull String template, @Nonnull Long count) {
        final StatApiDTO statsDto = new StatApiDTO();
        statsDto.setValue((float)count);
        statsDto.setName(StringConstants.NUM_RI);
        final StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setAvg((float)count);
        statsValueDto.setMax((float)count);
        statsValueDto.setMin((float)count);
        statsValueDto.setTotal((float)count);
        statsDto.setValues(statsValueDto);
        final List<StatFilterApiDTO> filterList = new ArrayList<>();
        final StatFilterApiDTO filterDto = new StatFilterApiDTO();
        filterDto.setType(StringConstants.TEMPLATE);
        filterDto.setValue(template);
        filterList.add(filterDto);
        statsDto.setFilters(filterList);
        return statsDto;
    }

    /**
     * Convert a list of {@link ReservedInstanceStatsRecord} to a list of {@link StatSnapshotApiDTO}.
     *
     * @param records                   a list of {@link ReservedInstanceStatsRecord}.
     * @param isRICoverage              a boolean which true means it's a reserved instance coverage stats request,
     *                                  false means it's a reserved instance utilization stats request.
     * @param isPlan                    flag to indicate the records are from a Plan.
     * @param projectedThresholdTime    the Time in millis to decide if the snapshot is PROJECTED.
     * @return a list {@link ReservedInstanceStatsRecord}.
     */
    protected static List<StatSnapshotApiDTO> internalConvertRIStatsRecordsToStatSnapshotApiDTO(
            @Nonnull final List<ReservedInstanceStatsRecord> records,
            final boolean isRICoverage, final boolean isPlan, final long projectedThresholdTime) {
        final Epoch projectedEpoch = isPlan ? Epoch.PLAN_PROJECTED : Epoch.PROJECTED;
        return records.stream()
                .map(record -> {
                    final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
                    snapshotApiDTO.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
                    snapshotApiDTO.setEpoch(projectedThresholdTime < record.getSnapshotDate()
                            ? projectedEpoch : Epoch.HISTORICAL);
                    final StatApiDTO statApiDTO = createRIUtilizationStatApiDTO(record, isRICoverage);
                    snapshotApiDTO.setStatistics(Lists.newArrayList(statApiDTO));
                    return snapshotApiDTO;
                })
                .collect(Collectors.toList());
    }

    /**
     * Create a {@link StatApiDTO} from input {@link ReservedInstanceStatsRecord}.
     *
     * @param record       a {@link ReservedInstanceStatsRecord}.
     * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
     *                     false means it's a reserved instance utilization stats request.
     * @return a {@link StatApiDTO}.
     */
    private static StatApiDTO createRIUtilizationStatApiDTO(@Nonnull final ReservedInstanceStatsRecord record,
                                                            final boolean isRICoverage) {
        final String name = isRICoverage ? StringConstants.RI_COUPON_COVERAGE : StringConstants.RI_COUPON_UTILIZATION;
        StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setAvg(record.getValues().getAvg());
        statsValueDto.setMax(record.getValues().getMax());
        statsValueDto.setMin(record.getValues().getMin());
        statsValueDto.setTotal(record.getValues().getTotal());
        StatValueApiDTO capacityDto = new StatValueApiDTO();
        capacityDto.setAvg(record.getCapacity().getAvg());
        capacityDto.setMax(record.getCapacity().getMax());
        capacityDto.setMin(record.getCapacity().getMin());
        capacityDto.setTotal(record.getCapacity().getTotal());
        StatApiDTO statsDto = new StatApiDTO();
        statsDto.setValues(statsValueDto);
        statsDto.setCapacity(capacityDto);
        statsDto.setUnits(StringConstants.RI_COUPON_UNITS);
        statsDto.setName(name);
        statsDto.setValue(record.getValues().getAvg());
        return statsDto;
    }

    @Nonnull
    protected GetReservedInstanceCoverageStatsRequest internalCreateCoverageRequest(
            @Nonnull final StatsQueryContext context,
            @Nonnull final GetReservedInstanceCoverageStatsRequest.Builder reqBuilder)
            throws OperationFailedException {
        context.getTimeWindow().ifPresent(timeWindow -> {
            reqBuilder.setStartDate(timeWindow.startTime());
            reqBuilder.setEndDate(timeWindow.endTime());
        });

        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()
                        && context.getQueryScope() != null) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            // This is a set of scope oids filtered by CSP.
            final Set<Long> scopeOids = context.getQueryScope().getScopeOids();
            if (scopeEntitiesByType.containsKey(ApiEntityType.SERVICE_PROVIDER)) {
                reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(
                                translateServiceProvidersToRegions(scopeOids)));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.BUSINESS_ACCOUNT)) {
                reqBuilder.setAccountFilter(
                        AccountFilter.newBuilder().addAllAccountId(scopeOids));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.REGION)) {
                reqBuilder.setRegionFilter(
                        RegionFilter.newBuilder().addAllRegionId(scopeOids));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.AVAILABILITY_ZONE)) {
                reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeOids));
            // Resource Groups and Groups of Entities and Single Entity Scopes.
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.VIRTUAL_MACHINE)
                            || scopeEntitiesByType.containsKey(ApiEntityType.DATABASE)
                            || scopeEntitiesByType.containsKey(ApiEntityType.VIRTUAL_VOLUME)) {
                reqBuilder.setEntityFilter(EntityFilter.newBuilder().addAllEntityId(scopeOids));
           } else {
                throw new OperationFailedException(new StringBuilder(
                        "Invalid scope for RI Coverage query: ")
                        .append(inputScope.getScopeTypes())
                        .append(". Must have a supported entity type: ")
                        .append(ApiEntityType.SERVICE_PROVIDER.displayName()).append(", ")
                        .append(ApiEntityType.BUSINESS_ACCOUNT.displayName()).append(", ")
                        .append(ApiEntityType.REGION.displayName()).append(", ")
                        .append(ApiEntityType.AVAILABILITY_ZONE.displayName()).append(", ")
                        .append(ApiEntityType.VIRTUAL_MACHINE.displayName())
                        .append(ApiEntityType.VIRTUAL_VOLUME.displayName())
                        .append(ApiEntityType.DATABASE.displayName())
                        .toString());
            }
        } else if (!context.isGlobalScope()) {
            throw new OperationFailedException(
                    "Invalid scope for query. Must be global or have an entity type.");
        }
        return reqBuilder.build();
    }

    @Nonnull
    protected GetReservedInstanceUtilizationStatsRequest internalCreateUtilizationRequest(
            @Nonnull final StatsQueryContext context,
            @Nonnull final GetReservedInstanceUtilizationStatsRequest.Builder reqBuilder)
            throws OperationFailedException {
        context.getTimeWindow().ifPresent(timeWindow -> {
            reqBuilder.setStartDate(timeWindow.startTime());
            reqBuilder.setEndDate(timeWindow.endTime());
        });

        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()
                        && context.getQueryScope() != null) {
            final Map<ApiEntityType, Set<Long>> scopeEntitiesByType = inputScope.getScopeEntitiesByType();
            // This is a set of scope oids filtered by CSP.
            final Set<Long> scopeOids = context.getQueryScope().getScopeOids();
            if (scopeEntitiesByType.containsKey(ApiEntityType.SERVICE_PROVIDER)) {
                reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(
                                translateServiceProvidersToRegions(scopeOids)));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.BUSINESS_ACCOUNT)) {
                reqBuilder.setAccountFilter(
                        AccountFilter.newBuilder().addAllAccountId(scopeOids));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.REGION)) {
                reqBuilder.setRegionFilter(
                        RegionFilter.newBuilder().addAllRegionId(scopeOids));
            } else if (scopeEntitiesByType.containsKey(ApiEntityType.AVAILABILITY_ZONE)) {
                reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeOids));
            // Workloads (single entities, groups of entities and entities in Resource Groups are not supported
            // for the utilization widget, as RI utilization in the DB is keyed off of the RI IDs.
            // It doesn't contain the covered entities.  Resource Groups in OCP only show the RI coverage widget.
            } else {
                throw new OperationFailedException(new StringBuilder(
                        "Invalid scope for RI Utilization query: ")
                        .append(inputScope.getScopeTypes())
                        .append(". Must have a supported entity type: ")
                        .append(ApiEntityType.SERVICE_PROVIDER.displayName()).append(", ")
                        .append(ApiEntityType.BUSINESS_ACCOUNT.displayName()).append(", ")
                        .append(ApiEntityType.REGION.displayName()).append(", ")
                        .append(ApiEntityType.AVAILABILITY_ZONE.displayName())
                        .toString());
            }
        } else if (!context.isGlobalScope()) {
            throw new OperationFailedException(
                    "Invalid scope for query. Must be global or have an entity type.");
        }
        return reqBuilder.build();
    }

    protected Set<Long> getScopeEntities(@Nonnull final StatsQueryContext context) {
        final ApiId inputScope = context.getInputScope();
        if (inputScope.isGroup()) {
            return inputScope.getCachedGroupInfo()
                    .map(CachedGroupInfo::getEntityIds)
                    .orElse(Collections.emptySet());
        } else if (inputScope.isPlan()) {
            return inputScope.getCachedPlanInfo().get().getPlanScopeIds();
        } else {
            return Collections.singleton(inputScope.oid());
        }
    }

    /**
     * Translate the service providers to regions for RI coverage/utilization requests.
     * TODO: short-term solution for 7.21.200, until the cost component supports queries
     *       for cost/RI attributes based on a ServiceProviderFilter.
     *       OM-53727: Cost + API: Support RI coverage & utilization queries by ServiceProvider
     *
     * @param serviceProviders the service providers
     * @return the regions belonging to the service providers
     */
    protected Set<Long> translateServiceProvidersToRegions(final Set<Long> serviceProviders) {
        return getRepositoryApi().expandServiceProvidersToRegions(serviceProviders);
    }

    /**
     * Create list of snapshots with RI cost stats.
     *
     * @param rICostStats list of RI cost stat.
     * @return list of {@link StatSnapshotApiDTO}.
     */
    protected static List<StatSnapshotApiDTO> convertRICostStatsToSnapshots(
            final List<ReservedInstanceCostStat> rICostStats) {
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = new ArrayList<>();
        for (final ReservedInstanceCostStat stat : rICostStats) {
            final StatApiDTO statApiDTO = new StatApiDTO();
            statApiDTO.setName(StringConstants.RI_COST);
            statApiDTO.setUnits(StringConstants.DOLLARS_PER_HOUR);
            final float totalCost = (float)stat.getAmortizedCost();
            final StatValueApiDTO statsValueDto = new StatValueApiDTO();
            statsValueDto.setAvg(totalCost);
            statsValueDto.setMax(totalCost);
            statsValueDto.setMin(totalCost);
            statsValueDto.setTotal(totalCost);
            statApiDTO.setValues(statsValueDto);
            statApiDTO.setCapacity(statsValueDto);
            final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
            statSnapshotApiDTO.setStatistics(Lists.newArrayList(statApiDTO));
            statSnapshotApiDTO.setDate(DateTimeUtil.toString(stat.getSnapshotTime()));
            statSnapshotApiDTOS.add(statSnapshotApiDTO);
        }
        return statSnapshotApiDTOS;
    }
}
