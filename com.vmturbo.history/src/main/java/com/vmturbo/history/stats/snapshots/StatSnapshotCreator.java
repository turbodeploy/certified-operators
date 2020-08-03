package com.vmturbo.history.stats.snapshots;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;

/**
 * A helper class to assemble database {@link Record}s into a {@link StatSnapshot} that can be
 * returned to clients.
 */
@FunctionalInterface
public interface StatSnapshotCreator {

    /**
     * Process the given DB Stats records, organizing into {@link StatSnapshot} by time and
     * commodity, and then invoking the handler ({@link Consumer}) from the caller on each
     * StatsSnapshot that is built.
     *
     * @param statDBRecords the list of DB stats records to organize
     * @param fullMarket is this a query against the full market table vs.
     *                 individual SE type
     * @param commodityRequests a list of {@link CommodityRequest} being satifisfied
     *                 in this query
     * @return stream of {@link StatSnapshot.Builder}s.
     */
    @Nonnull
    Stream<Builder> createStatSnapshots(@Nonnull List<Record> statDBRecords, boolean fullMarket,
                    @Nonnull List<CommodityRequest> commodityRequests);

}
