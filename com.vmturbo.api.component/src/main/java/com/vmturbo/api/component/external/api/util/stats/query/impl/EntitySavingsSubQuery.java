package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.Cost.GetEntitySavingsStatsRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;

/**
 * Sub-query for handling requests for entity savings/investments.
 */
public class EntitySavingsSubQuery implements StatsSubQuery {

    private final CostServiceBlockingStub costServiceRpc;

    private static final Set<String> SUPPORTED_STATS = Arrays.stream(EntitySavingsStatsType.values())
            .map(EntitySavingsStatsType::name)
            .collect(Collectors.toSet());

    /**
     * Constructor for EntitySavingsSubQuery.
     *
     * @param costServiceRpc cost RPC service
     */
    public EntitySavingsSubQuery(@Nonnull final CostServiceBlockingStub costServiceRpc) {
        this.costServiceRpc = costServiceRpc;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // This sub query is designed for real-time market and only for cloud entities or groups.
        // Hybrid groups are also applicable. Only the cloud entities in the group will have savings
        // stats. There is no need to process on-prem entities or groups since they don't have
        // savings data.
        return !context.getInputScope().isPlan()
                && (context.getInputScope().isCloud() || context.getInputScope().isHybridGroup());
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException, InterruptedException, ConversionException {

        GetEntitySavingsStatsRequest.Builder request = GetEntitySavingsStatsRequest.newBuilder();

        // Set time range.
        boolean includeHistorical = context.getTimeWindow().map(TimeWindow::includeHistorical).orElse(false);
        if (context.getTimeWindow().isPresent() && includeHistorical) {
            TimeWindow timeWindow = context.getTimeWindow().get();
            request.setStartDate(timeWindow.startTime());
            request.setEndDate(timeWindow.endTime());
        } else {
            // This sub-query will only process historical requests.
            // Return empty list if request is not historical or time range is not present.
            return Collections.emptyList();
        }


        // Set requested stat names.
        Set<EntitySavingsStatsType> requestedStatsTypes = stats.stream()
                .map(StatApiInputDTO::getName)
                .filter(SUPPORTED_STATS::contains)
                .map(EntitySavingsStatsType::valueOf)
                .collect(Collectors.toSet());
        request.addAllStatsTypes(requestedStatsTypes);

        // Set scope entity IDs.
        EntityFilter entityFilter = EntityFilter.newBuilder()
                .addAllEntityId(context.getInputScope().getScopeOids()).build();
        request.setEntityFilter(entityFilter);

        // Call cost component api to get the list of stats
        Iterator<EntitySavingsStatsRecord> savingsStatsRecords = costServiceRpc.getEntitySavingsStats(request.build());

        // convert response to list of StatSnapshotApiDTO
        final List<StatSnapshotApiDTO> statsResponse = new ArrayList<>();
        while (savingsStatsRecords.hasNext()) {
            EntitySavingsStatsRecord record = savingsStatsRecords.next();
            statsResponse.add(toStatSnapshotApiDTO(record));
        }

        return statsResponse;
    }

    /**
     * Convert the response from cost component api to StatSnapshotApiDTO.
     *
     * @param record stats record
     * @return stats converted to StatSnapshotApiDTO
     */
    private StatSnapshotApiDTO toStatSnapshotApiDTO(EntitySavingsStatsRecord record) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        // set values
        dto.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
        dto.setStatistics(record.getStatRecordsList().stream()
                .map(EntitySavingsSubQuery::toStatApiDTO)
                .collect(toList()));
        dto.setEpoch(Epoch.HISTORICAL);
        return dto;
    }

    private static StatApiDTO toStatApiDTO(@Nonnull final SavingsRecord savingsRecord) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(savingsRecord.getName());
        float value = savingsRecord.getValue();
        statApiDTO.setValue(value);

        final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
        statValueApiDTO.setAvg(value);
        statValueApiDTO.setMax(value);
        statValueApiDTO.setMin(value);
        statValueApiDTO.setTotal(value);
        statApiDTO.setValues(statValueApiDTO);
        statApiDTO.setUnits("$");

        return statApiDTO;
    }
}
