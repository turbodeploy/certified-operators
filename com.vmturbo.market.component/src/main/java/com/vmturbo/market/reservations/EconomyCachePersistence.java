package com.vmturbo.market.reservations;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.component.db.tables.EconomyCache;
import com.vmturbo.market.component.db.tables.records.EconomyCacheRecord;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.EconomyCacheDTOs.EconomyCacheDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyCacheDTOs.EconomyCacheDTO.CommTypeEntry;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * The writer and reader of reservation persistence.
 */
public class EconomyCachePersistence {

    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * prefix for initial placement log messages.
     */
    private final String logPrefix = "FindInitialPlacement: ";

    /**
     *  Set up the size limit for the batch insertion when persisting economy cache.
     */
    private static final int BATCH_SIZE = 4;

    /**
     * The database context.
     */
    private final DSLContext dsl;

    /**
     * Constructor.
     *
     * @param dsl The data bse context.
     */
    public EconomyCachePersistence(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Loads the {@link EconomyCacheDTO} persisted in database.
     *
     * @param isHistorical whether to load historical economy cache or real time economy cache.
     * @return a protobuf object for the economyCache.
     * @throws Exception in case db read or protobuf conversion has an exception.
     */
    public Optional<EconomyCacheDTO> loadEconomyCacheDTO(final boolean isHistorical) throws Exception {
        final Instant startTime = Instant.now();
        List<EconomyCacheRecord> result;
        // Positive id slices are historical economy cache.
        if (isHistorical) {
            result = dsl.selectFrom(EconomyCache.ECONOMY_CACHE).where(
                    EconomyCache.ECONOMY_CACHE.ID.greaterThan(0)).orderBy(
                    EconomyCache.ECONOMY_CACHE.ID.sortAsc()).fetch();
        } else { // Negative id slices are real time economy cache.
            result = dsl.selectFrom(EconomyCache.ECONOMY_CACHE).where(
                    EconomyCache.ECONOMY_CACHE.ID.lessThan(0)).orderBy(
                    EconomyCache.ECONOMY_CACHE.ID.sortDesc()).fetch();
        }
        // Maybe the market component did not live long enough to have persisted data.
        if (result.isEmpty()) {
            logger.warn(logPrefix + "No economy cache data found from table for "
                    + (isHistorical ? " historical" : "real time"));
            return Optional.empty();
        }
        // Reconstruct the blob obtained from db into one protobuf message.
        // The message already sorted based on the correct order.
        List<Byte> bytes = new ArrayList();
        for (EconomyCacheRecord r : result) {
            if (r != null) {
                byte[] info = r.getValue(EconomyCache.ECONOMY_CACHE.ECONOMY);
                for (int i = 0; i < info.length; i++) {
                    bytes.add(info[i]);
                }
            }
        }
        final Instant completionTime = Instant.now();
        logger.info(logPrefix + "Time taken for " + (isHistorical ? " historical" : "real time")
                + " economy cache loading : " + startTime.until(completionTime,
                ChronoUnit.SECONDS) + " seconds.");
        return Optional.of(EconomyCacheDTO.parseFrom(Bytes.toArray(bytes)));
    }

    /**
     * Persist the given economy cache into table.
     * Note: currently only the historical economy cache is persisted.
     *
     * @param cachedEconomy the economy to be persisted.
     * @param commTypeMap the commodity type to commoditySpecification's type mapping.
     * @param isHistorical true if the economy given is historical.
     */
    public void saveEconomyCache(Economy cachedEconomy,
            BiMap<CommodityType, Integer> commTypeMap, boolean isHistorical) {
        if (cachedEconomy == null) {
            logger.warn(logPrefix + "Persistence can not be performed on non-existing economy");
            return;
        }
        Instant startTime = Instant.now();
        try {
            EconomyCacheDTO economyCacheDTO = convertToProto(cachedEconomy, commTypeMap);
            byte[] info = economyCacheDTO.toByteArray();
            // limit the blob size to be max allowed / (BATCH_SIZE + 1), the + 1 is to leave buffer
            // room for the query so that it is always smaller than max_allowed_package
            int maxSize = queryMaxAllowedPackageSize() / (BATCH_SIZE + 1);
            List<List<Byte>> chunkList = new ArrayList<>();
            List<Byte> chunk = new ArrayList<>();
            for (int chunkIndex = 0, protobufIndex = 0; protobufIndex < info.length; chunkIndex++, protobufIndex++) {
                if (chunkIndex < maxSize) {
                    chunk.add(info[protobufIndex]);
                } else {
                    chunkList.add(chunk);
                    chunk = new ArrayList<>();
                    chunkIndex = -1;
                    protobufIndex = protobufIndex - 1;
                }
            }
            chunkList.add(chunk);
            writeToEconomyCacheTable(chunkList, isHistorical);
            Instant completionTime = Instant.now();
            logger.info(logPrefix + "Inserted " + chunkList.size() + " chunks into economy cache table."
                    + " Time taken for " + (isHistorical ? " historical" : "real time") + " insertion : "
                    + startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds.");
        } catch (Exception ex) {
            logger.error( logPrefix + "Exception in saving" + (isHistorical ? " historical" : "real time")
                    + " economy cache information : ", ex);
        }
    }

    /**
     * Convert the given economy cache to a protobuf object.
     *
     * @param cachedEconomy the economy to be persisted.
     * @param commTypeMap the commodity type to commoditySpecification's type mapping.
     * @return EconomyCacheDTO a protobuf object for reservation economy cache.
     */
    @VisibleForTesting
    protected EconomyCacheDTO convertToProto(Economy cachedEconomy, BiMap<CommodityType, Integer> commTypeMap) {
        EconomyCacheDTO.Builder builder = EconomyCacheDTO.newBuilder();
        // All providers and placed reservation buyers are added to the EconomyCacheDTO.
        for (Trader trader : cachedEconomy.getTraders()) {
            builder.addTraders(AnalysisToProtobuf.traderTO(cachedEconomy, trader,
                    cachedEconomy.getTopology().getShoppingListOids(), new HashSet()));
        }
        for (Map.Entry<CommodityType, Integer> entry : commTypeMap.entrySet()) {
            CommTypeEntry.Builder commTypeBuilder = CommTypeEntry.newBuilder()
                    .setCommType(entry.getKey().getType())
                    .setCommSpecType(entry.getValue());
            if (entry.getKey().hasKey()) {
                commTypeBuilder.setKey(entry.getKey().getKey());
            }
            builder.addCommTypeEntry(commTypeBuilder.build());
        }
        return builder.build();
    }

    /**
     * Query db to fetch the max_allowed_package size. This limits the record size written to table.
     *
     * @return the max allowed package size
     */
    @VisibleForTesting
    protected int queryMaxAllowedPackageSize() {
        return JooqUtil.getMaxAllowedPacket(dsl);
    }

    /**
     * Delete the previous economy cache and insert a new one.
     *
     * @param chunkList the economy cache byte data.
     * @param isHistorical the economy cache given is historical or not.
     */
    @VisibleForTesting
    protected void writeToEconomyCacheTable(List<List<Byte>> chunkList, final boolean isHistorical) {
        List<EconomyCacheRecord> records = new ArrayList();
        // For a list of chunk that comes from historical economy cache, all the sequence id integers
        // will be positive, and the order is ascending. The real time economy cache chunks will
        // be assigned with negative integers and the order is descending. When reading and restoring
        // the economy caches, it can be reconstructed based on sequence id in case the EconomyCacheDTO
        // is sliced.
        int sequenceId = isHistorical ? 1 : -1;
        for (List<Byte> chunk : chunkList) {
            EconomyCacheRecord rec = dsl.newRecord(EconomyCache.ECONOMY_CACHE);
            rec.setId(sequenceId);
            rec.setEconomy(Bytes.toArray(chunk));
            records.add(rec);
            if (isHistorical) { // Increasingly assign id to historical slices
                sequenceId++;
            } else {
                sequenceId--; // Decreasingly assign id to real time slices
            }
        }
        // Remove all old economy cache records and insert new ones.
        // Positive id records consists of old historical economy cache, negative id records consists
        // of old real time economy cache.
        if (isHistorical) {
            dsl.transaction(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                transaction.deleteFrom(EconomyCache.ECONOMY_CACHE).where(EconomyCache.ECONOMY_CACHE.ID.greaterThan(0)).execute();
                Iterators.partition(records.iterator(), BATCH_SIZE).forEachRemaining(recordBatch -> {
                    transaction.batchInsert(recordBatch).execute();
                });
            });
        } else {
            dsl.transaction(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                transaction.deleteFrom(EconomyCache.ECONOMY_CACHE).where(EconomyCache.ECONOMY_CACHE.ID.lessThan(0)).execute();
                Iterators.partition(records.iterator(), BATCH_SIZE).forEachRemaining(recordBatch -> {
                    transaction.batchInsert(recordBatch).execute();
                });
            });
        }
    }
}
