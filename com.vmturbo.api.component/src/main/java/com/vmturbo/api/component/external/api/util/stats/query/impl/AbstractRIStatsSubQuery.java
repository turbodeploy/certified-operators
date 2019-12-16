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

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Abstract sub-query responsible for getting reserved instance stats from the cost component.
 */
public abstract class AbstractRIStatsSubQuery implements StatsSubQuery {
    private static final Set<String> SUPPORTED_STATS =
                    ImmutableSet.of(StringConstants.RI_COUPON_UTILIZATION,
                                    StringConstants.RI_COUPON_COVERAGE, StringConstants.NUM_RI);

    private final RepositoryApi repositoryApi;

    /**
     * Creates {@link AbstractRIStatsSubQuery} instance.
     *
     * @param repositoryApi repository API.
     */
    public AbstractRIStatsSubQuery(@Nonnull RepositoryApi repositoryApi) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
    }

    @Override
    public SubQuerySupportedStats getHandledStats(StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    public RepositoryApi getRepositoryApi() {
        return repositoryApi;
    }

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
}
