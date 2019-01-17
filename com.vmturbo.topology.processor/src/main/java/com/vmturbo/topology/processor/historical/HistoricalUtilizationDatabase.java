package com.vmturbo.topology.processor.historical;

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

    public void saveInfo(HistoricalInfo histInfo) {
        HistoricalUtilizationRecord rec = new HistoricalUtilizationRecord();
        HistoricalInfoRecord record = new HistoricalInfoRecord(Conversions.convertToDto(histInfo).toByteArray());

        rec.setId(1L);
        rec.setInfo(record.getInfo());
        dsl.insertInto(HistoricalUtilization.HISTORICAL_UTILIZATION).set(rec).onDuplicateKeyUpdate()
                .set(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO, record.getInfo()).execute();

    }

    public HistoricalInfoRecord getInfo() {
        HistoricalInfoRecord record = new HistoricalInfoRecord();
        Result<Record1<byte[]>> result = dsl.select(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO)
                .from(HistoricalUtilization.HISTORICAL_UTILIZATION)
                .fetch();
        if (result.size() == 1) {
            Record record1 = result.get(0);
            byte[] bytes = null;
            if (record1 != null) {
                bytes = record1.getValue(HistoricalUtilization.HISTORICAL_UTILIZATION.INFO);
            }
            record.setInfo(bytes);
        }
        return record;
    }

}
