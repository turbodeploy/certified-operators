package com.vmturbo.history.stats;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Record;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.records.MktSnapshotsStatsRecord;

/**
 * Read records from the PLAN topologies stats table, mkt_snapshots_stats.
 **/
public class PlanStatsReader {

    private final HistorydbIO historydbIO;

    public PlanStatsReader(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    public @Nonnull List<MktSnapshotsStatsRecord> getStatsRecords(
            long topologyContextId,
            @Nonnull List<String> commodityNames) throws VmtDbException, SQLException {
        return historydbIO.getPlanStats(topologyContextId, commodityNames);
    }
}
