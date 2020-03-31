package com.vmturbo.history.stats.priceindex;

import static com.vmturbo.history.db.HistorydbIO.GENERIC_STATS_TABLE;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;

/**
 * A {@link TopologyPriceIndexVisitor} that saves the price indices to the appropriate tables
 * in the history database.
 */
public class DBPriceIndexVisitor implements TopologyPriceIndexVisitor {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    private final HistorydbIO historydbIO;

    private final Map<EntityType, Map<EnvironmentType, MarketStatsData>> mktStatsByEntityTypeAndEnv =
        new HashMap<>();

    private final Set<Integer> notFoundEntityTypes = new HashSet<>();
    private final Set<EntityType> noTableEntityTypes = new HashSet<>();

    // source of loaders for whatever tables are involved
    private final SimpleBulkLoaderFactory loaders;

    private DBPriceIndexVisitor(@Nonnull final HistorydbIO historydbIO,
                                @Nonnull final TopologyInfo topologyInfo,
                                @Nonnull final SimpleBulkLoaderFactory loaders) {
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.loaders = loaders;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final Integer entityType,
                      final EnvironmentType environmentType,
                      final Map<Long, Double> priceIdxByEntityId) throws InterruptedException {
        final Optional<EntityType> dbEntityTypeOpt = historydbIO.getEntityType(entityType);
        if (dbEntityTypeOpt.isPresent() && dbEntityTypeOpt.get().persistsPriceIndex()) {
            final Map<EnvironmentType, MarketStatsData> mktStatsByEnv =
                    mktStatsByEntityTypeAndEnv.computeIfAbsent(dbEntityTypeOpt.get(), k -> new HashMap<>(3));
            final MarketStatsData marketDataForType =
                    mktStatsByEnv.computeIfAbsent(environmentType, k ->
                            new MarketStatsData(dbEntityTypeOpt.get().getName(),
                                    environmentType,
                                    StringConstants.PRICE_INDEX,
                                    StringConstants.PRICE_INDEX,
                                    RelationType.METRICS));
            final Optional<Table<?>> dbTable = dbEntityTypeOpt.get().getLatestTable();
            if (dbTable.isPresent()) {
                for (final Entry<Long, Double> priceIndexEntry : priceIdxByEntityId.entrySet()) {
                    final Long oid = priceIndexEntry.getKey();
                    final Double priceIndex = priceIndexEntry.getValue();
                    marketDataForType.accumulate(priceIndex, priceIndex, priceIndex, priceIndex);

                Record record = dbTable.get().newRecord();
                    record.set(GENERIC_STATS_TABLE.SNAPSHOT_TIME, new Timestamp(topologyInfo.getCreationTime()));
                    record.set(GENERIC_STATS_TABLE.UUID, Long.toString(oid));
                    record.set(GENERIC_STATS_TABLE.PROPERTY_TYPE, StringConstants.PRICE_INDEX);
                    record.set(GENERIC_STATS_TABLE.PROPERTY_SUBTYPE, StringConstants.PRICE_INDEX);
                    record.set(GENERIC_STATS_TABLE.RELATION, RelationType.METRICS);
                    double clipped = historydbIO.clipValue(priceIndex);
                    record.set(GENERIC_STATS_TABLE.AVG_VALUE, clipped);
                    record.set(GENERIC_STATS_TABLE.MIN_VALUE, clipped);
                    record.set(GENERIC_STATS_TABLE.MAX_VALUE, clipped);
                    @SuppressWarnings("unchecked")
                    final BulkLoader<Record> loader = loaders.getLoader((Table<Record>)dbTable.get());
                    loader.insert(record);
                }
            } else {
                noTableEntityTypes.add(dbEntityTypeOpt.get());
            }
        } else if (!dbEntityTypeOpt.isPresent()) {
            // keep track of entity types we couldn't map to DB entity types
            notFoundEntityTypes.add(entityType);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onComplete() throws InterruptedException {

        // Insert the accumulated price index stats.
        final List<MarketStatsData> mktStatsData = mktStatsByEntityTypeAndEnv.values().stream()
            .flatMap(mktStatsByEnv -> mktStatsByEnv.values().stream())
            .collect(Collectors.toList());
        final BulkLoader loader = loaders.getLoader(MarketStatsLatest.MARKET_STATS_LATEST);
        for (MarketStatsData data : mktStatsData) {
            final MarketStatsLatestRecord marketStatsRecord
                = historydbIO.getMarketStatsRecord(data, topologyInfo);
            loader.insert(marketStatsRecord);
        }

        if (!notFoundEntityTypes.isEmpty()) {
            logger.error("History DB Entity Types not found for entity types: {}",
                    notFoundEntityTypes);
        }
        if (!noTableEntityTypes.isEmpty()) {
            logger.error("Entity types lack '_latest' tables: {}",
                    noTableEntityTypes.stream()
                            .map(EntityType::getName)
                            .collect(Collectors.joining(", ")));
        }
    }

    /**
     * Factory class for {@link DBPriceIndexVisitor}s.
     */
    public static class DBPriceIndexVisitorFactory {

        private final HistorydbIO historydbIO;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO DB stuff
         */
        public DBPriceIndexVisitorFactory(@Nonnull final HistorydbIO historydbIO) {
            this.historydbIO = historydbIO;
        }

        /**
         * Create a new {@link DBPriceIndexVisitor}.
         *
         * @param topologyInfo Information about the topology we're receiving price indices for.
         * @param loaders source of table-specific writer objects
         * @return The {@link DBPriceIndexVisitor}.
         */
        public DBPriceIndexVisitor newVisitor(@Nonnull final TopologyInfo topologyInfo,
                                              @Nonnull SimpleBulkLoaderFactory loaders) {
            return new DBPriceIndexVisitor(historydbIO, topologyInfo, loaders);
        }
    }
}
