package com.vmturbo.history.stats.priceindex;

import java.util.ArrayList;
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
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;

/**
 * A {@link TopologyPriceIndexVisitor} that saves the price indices to the appropriate tables
 * in the history database.
 */
public class DBPriceIndexVisitor implements TopologyPriceIndexVisitor {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    // the number of entities for which stats are persisted in a single DB Insert operations
    private final int writeTopologyChunkSize;

    private final HistorydbIO historydbIO;

    private final Map<EntityType, Map<EnvironmentType, MarketStatsData>> mktStatsByEntityTypeAndEnv =
        new HashMap<>();

    private final Set<Integer> notFoundEntityTypes = new HashSet<>();

    // accumulate a batch of SQL statements to insert the commodity rows; execute in batches
    private List<Query> commodityInsertStatements = Lists.newArrayList();

    private DBPriceIndexVisitor(@Nonnull final HistorydbIO historydbIO,
                               @Nonnull final TopologyInfo topologyInfo,
                               final int writeTopologyChunkSize) {
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.writeTopologyChunkSize = writeTopologyChunkSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final Integer entityType,
               final EnvironmentType environmentType,
               final Map<Long, Double> priceIdxByEntityId) throws VmtDbException {
        final Optional<EntityType> dbEntityTypeOpt = historydbIO.getEntityType(entityType);
        if (dbEntityTypeOpt.isPresent()) {
            final Map<EnvironmentType, MarketStatsData> mktStatsByEnv =
                mktStatsByEntityTypeAndEnv.computeIfAbsent(dbEntityTypeOpt.get(), k -> new HashMap<>(3));
            final MarketStatsData marketDataForType =
                mktStatsByEnv.computeIfAbsent(environmentType, k ->
                    new MarketStatsData(dbEntityTypeOpt.get().getClsName(),
                        environmentType,
                        StringConstants.PRICE_INDEX,
                        StringConstants.PRICE_INDEX,
                        RelationType.METRICS));
            final org.jooq.Table<?> dbTable = dbEntityTypeOpt.get().getLatestTable();
            for (final Entry<Long, Double> priceIndexEntry : priceIdxByEntityId.entrySet()) {
                final Long oid = priceIndexEntry.getKey();
                final Double priceIndex = priceIndexEntry.getValue();
                marketDataForType.accumulate(priceIndex, priceIndex, priceIndex, priceIndex);

                final InsertSetMoreStep<?> insertStmt = historydbIO.getCommodityInsertStatement(dbTable);
                historydbIO.initializeCommodityInsert(StringConstants.PRICE_INDEX,
                    topologyInfo.getCreationTime(),
                    oid, RelationType.METRICS, null, null, null,
                    null, insertStmt, dbTable);
                // set the values specific to used component of commodity and write
                historydbIO.setCommodityValues(StringConstants.PRICE_INDEX, priceIndex,
                    insertStmt, dbTable);
                commodityInsertStatements.add(insertStmt);
                if (commodityInsertStatements.size() >= writeTopologyChunkSize) {
                    // execute a batch of updates - FORCED implies repeat until successful
                    historydbIO.execute(BasedbIO.Style.FORCED, commodityInsertStatements);
                    commodityInsertStatements = new ArrayList<>(writeTopologyChunkSize);
                }
            }
        } else {
            notFoundEntityTypes.add(entityType);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onComplete() throws VmtDbException {
        if (!notFoundEntityTypes.isEmpty()) {
            logger.error("History DB Entity Types not found for entity types: {}",
                notFoundEntityTypes);
        }

        // now execute the remaining batch of updates, if any
        if (!commodityInsertStatements.isEmpty()) {
            historydbIO.execute(BasedbIO.Style.FORCED, commodityInsertStatements);
        }

        // Insert the accumulated price index stats.
        final List<Query> insertStmts = mktStatsByEntityTypeAndEnv.values().stream()
            .flatMap(mktStatsByEnv -> mktStatsByEnv.values().stream())
            .map(marketStatsData -> historydbIO.getMarketStatsInsertStmt(
                marketStatsData, topologyInfo))
            .collect(Collectors.toList());
        if (!insertStmts.isEmpty()) {
            logger.info("Inserting aggregate market price index entries for types: {}",
                Joiner.on(",").join(mktStatsByEntityTypeAndEnv.keySet().stream()
                    .map(EntityType::getClsName)
                    .iterator()));
            historydbIO.execute(BasedbIO.Style.FORCED, insertStmts);
        }

    }

    /**
     * Factory class for {@link DBPriceIndexVisitor}s.
     */
    public static class DBPriceIndexVisitorFactory {
        /**
         * The number of entities for which stats are persisted in a single DB Insert operations
         */
        private final int writeTopologyChunkSize;

        private final HistorydbIO historydbIO;

        public DBPriceIndexVisitorFactory(@Nonnull final HistorydbIO historydbIO,
                                          final int writeTopologyChunkSize) {
            this.writeTopologyChunkSize = writeTopologyChunkSize;
            this.historydbIO = historydbIO;
        }

        /**
         * Create a new {@link DBPriceIndexVisitor}.
         *
         * @param topologyInfo Information about the topology we're receiving price indices for.
         * @return The {@link DBPriceIndexVisitor}.
         */
        public DBPriceIndexVisitor newVisitor(@Nonnull final TopologyInfo topologyInfo) {
            return new DBPriceIndexVisitor(historydbIO, topologyInfo, writeTopologyChunkSize);
        }
    }
}
