/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.INonPaginatingStatsReader;

/**
 * {@link HistUtilizationReader} reads information from historical utilization for specified
 * entities and filters. For more information, please, look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 */
public class HistUtilizationReader implements INonPaginatingStatsReader<HistUtilizationRecord> {
    @Nonnull
    @Override
    public List<HistUtilizationRecord> getRecords(@Nonnull Set<String> entityIds,
                    @Nonnull StatsFilter statsFilter) {
        // TODO alexandr.vasin will do implementation
        return Collections.emptyList();
    }
}
