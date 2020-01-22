package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.time.Clock;
import java.util.ArrayList;
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
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.components.common.utils.StringConstants;

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
    public AbstractRIStatsSubQuery(@Nonnull RepositoryApi repositoryApi,
                    @Nonnull BuyRiScopeHandler buyRiScopeHandler) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.buyRiScopeHandler = Objects.requireNonNull(buyRiScopeHandler);
    }

    @Override
    public SubQuerySupportedStats getHandledStats(StatsQueryContext context) {
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
    protected static List<StatSnapshotApiDTO> mergeStatsByDate(List<StatSnapshotApiDTO> snapshots) {
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
     * Create list of snapshots with RI cost stats.
     *
     * @param rICostStats list of RI cost stat.
     * @return list of {@link StatSnapshotApiDTO}.
     */
    protected static List<StatSnapshotApiDTO> convertRICostStatsToSnapshots(List<ReservedInstanceCostStat> rICostStats) {
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = new ArrayList<>();

        for (ReservedInstanceCostStat stat : rICostStats) {
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
