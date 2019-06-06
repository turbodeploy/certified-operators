/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.stats.INonPaginatingStatsReader;

/**
 * {@link PercentileReader} reads percentile information from database. For more information,
 * please, look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 *
 * @param <R> type of the record which will be provided to provide information about
 *                 percentile.
 */
public class PercentileReader<R extends Record> implements INonPaginatingStatsReader<R> {
    @Nonnull
    @Override
    public List<R> getRecords(@Nonnull Set<String> entityIds, @Nonnull StatsFilter statsFilter) {
        // TODO alexandr.vasin will do implementation.
        return Collections.emptyList();
    }
}
