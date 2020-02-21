package com.vmturbo.topology.processor.historical;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import com.vmturbo.topology.processor.db.tables.HistoricalUtilization;
import com.vmturbo.topology.processor.db.tables.records.HistoricalUtilizationRecord;

public class HistoricalUtilizationDatabase {
    private final Logger logger = LogManager.getLogger(HistoricalUtilizationDatabase.class);

    private final DSLContext dsl;

    public HistoricalUtilizationDatabase(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Persist the historical utilization blob to database.
     *
     * @param histInfo the historical info containing the historical values
     */
    public void saveInfo(HistoricalInfo histInfo) {
        Instant startTime = Instant.now();
        try {
            HistoricalUtilizationRecord rec = new HistoricalUtilizationRecord();
            byte[] info = Conversions.convertToDto(histInfo).toByteArray();

            rec.setId(1L);
            rec.setInfo(info);
            dsl.insertInto(HistoricalUtilization.HISTORICAL_UTILIZATION).set(rec).onDuplicateKeyUpdate()
                    .set(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO, info).execute();
            Instant completionTime = Instant.now();
            logger.info("Time taken for historical data insertion : " +
                    startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds.");
        } catch (Exception ex) {
            logger.error( "Exception in saving historical information : ", ex);
        }
    }

    /**
     * Query the historical utilization blob from database and return it.
     *
     * @return the byte array represents the full topology historical utilization.
     */
    public byte[] getInfo() {
        Instant startTime = Instant.now();
        byte[] bytes = null;
        try {
            Result<Record1<byte[]>> result = dsl.select(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO)
                    .from(HistoricalUtilization.HISTORICAL_UTILIZATION)
                    .fetch();
            if (result.size() == 1) {
                Record record1 = result.get(0);
                if (record1 != null) {
                    bytes = record1.getValue(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO);
                }
            }
            Instant completionTime = Instant.now();
            logger.info("Time taken for historical data retrieval : " +
                    startTime.until(completionTime, ChronoUnit.SECONDS) + " seconds.");
        } catch (Exception ex) {
            logger.error("Exception in historical information retrieval : ", ex);
        }
        return bytes;
    }

}
