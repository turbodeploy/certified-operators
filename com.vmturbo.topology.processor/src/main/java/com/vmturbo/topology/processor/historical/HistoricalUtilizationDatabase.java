package com.vmturbo.topology.processor.historical;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import com.vmturbo.topology.processor.db.tables.HistoricalUtilization;
import com.vmturbo.topology.processor.db.tables.records.HistoricalUtilizationRecord;

public class HistoricalUtilizationDatabase {
    private final Logger logger = LogManager.getLogger(HistoricalUtilizationDatabase.class);

    private final DSLContext dsl;

    // set up the size limit for the batch insertion
    private final static int BATCH_SIZE = 3;

    private Instant persistDataTimeStamp = null;

    public HistoricalUtilizationDatabase(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Check if at least one hour is passed from current time to the last time persisting info to db,
     * or if it is the first time to persist info.
     *
     * @param current current time point
     * @return true if at least one hour is passed
     */
    @VisibleForTesting
    protected boolean shouldPersistData(@Nonnull Instant current) {
        if (persistDataTimeStamp != null &&
            (persistDataTimeStamp.until(current, ChronoUnit.HOURS) < 1)) {
            return false;
        }
        return true;
    }

    /**
     * Persist the historical utilization blob to database.
     *
     * @param histInfo the historical info containing the historical values
     */
    public void saveInfo(HistoricalInfo histInfo) {
        Instant startTime = Instant.now();
        // skip persisting data when info is saved in the previous round but the elapsed time is less
        // than 1 hour
        if (!shouldPersistData(startTime)) {
            return;
        }
        try {
            byte[] info = Conversions.convertToDto(histInfo).toByteArray();
            // limit the blob size to be max allowed / (BATCH_SIZE + 1), the + 1 is to leave buffer
            // room for the query so that it is always smaller than max_allowed_package
            int maxSize = queryMaxAllowedPackageSize()/(BATCH_SIZE + 1);
            // Slice the 'info' byte array which contains the full topology historical utilization into small pieces
            // whose size is less than mysql's max_allowed_package size
            List<List<Byte>> chunkList = new ArrayList<>();
            List<Byte> chunk = new ArrayList<>();
            for (int chunkIndex = 0, protobufIndex = 0; protobufIndex < info.length;
                     chunkIndex++, protobufIndex++) {
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
            updateHistoricalUtilizationInTable(chunkList);
            Instant completionTime = Instant.now();
            logger.info("Inserted " + chunkList.size() + " chunks into historical_utilization table." +
                    " Time taken for insertion : " + startTime.until(completionTime, ChronoUnit.SECONDS)
                    + " seconds.");
            persistDataTimeStamp = completionTime;
            logger.info("Time taken for historical data insertion : " +
                    startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds.");
        } catch (Exception ex) {
            logger.error( "Exception in saving historical information : ", ex);
        }
    }

    /**
     * Query db to fetch the max_allowed_package size. This limits the record size written to table.
     *
     * @return the max allowed package size
     */
    @VisibleForTesting
    protected int queryMaxAllowedPackageSize() {
        // Query the max_allowed_package size
        Result<Record> maxPackageSize = dsl.fetch("SHOW VARIABLES LIKE 'max_allowed_packet';");
        return maxPackageSize.get(0).getValue("Value", int.class);
    }

    /**
     * Delete the previous cycle historical utilization data and insert new ones generated in current cycle.
     *
     * @param chunkList the historical utilization data
     */
    private void updateHistoricalUtilizationInTable(List<List<Byte>> chunkList) {
        List<HistoricalUtilizationRecord> records = new ArrayList<>();
        long sequenceId = 1;
        for (List<Byte> chunk : chunkList) {
            HistoricalUtilizationRecord rec = dsl.newRecord(HistoricalUtilization.HISTORICAL_UTILIZATION);
            rec.setId(sequenceId);
            rec.setInfo(Bytes.toArray(chunk));
            sequenceId++;
            records.add(rec);
        }
        // remove all old historical utilization records and insert new ones
        dsl.transaction(configuration -> {
            final DSLContext transaction = DSL.using(configuration);
            transaction.deleteFrom(HistoricalUtilization.HISTORICAL_UTILIZATION).execute();
            Iterators.partition(records.iterator(), BATCH_SIZE).forEachRemaining(recordBatch -> {
                transaction.batchInsert(recordBatch).execute();
            });
        });
    }

    /**
     * Query the historical utilization blob from database and return it.
     *
     * @return the byte array represents the full topology historical utilization.
     */
    public byte[] getInfo() {
        Instant startTime = Instant.now();
        List<Byte> bytes = new ArrayList<>();
        try {
            List<HistoricalUtilizationRecord> result = dsl.selectFrom(HistoricalUtilization.HISTORICAL_UTILIZATION)
                .orderBy(HistoricalUtilization.HISTORICAL_UTILIZATION.ID)
                .fetch();

            // Reconstruct the blob obtained from db into one protobuf message
            for (HistoricalUtilizationRecord r : result) {
                if (r != null) {
                    byte[] info = r.getValue(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO);
                    for (int i = 0; i < info.length; i++) {
                        bytes.add(info[i]);
                    }
                }
            }

            Instant completionTime = Instant.now();
            logger.info("Time taken for historical data retrieval : " +
                startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds.");
        } catch (Exception ex) {
            logger.error("Exception in historical information retrieval : ", ex);
        }
        return Bytes.toArray(bytes);
    }

}
