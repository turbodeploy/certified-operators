package com.vmturbo.history.stats.writers;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jooq.DeleteConditionStep;
import org.jooq.Query;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;

/**
 * The class SystemLoadWriter stores the system load information to the DB.
 */
public class SystemLoadWriter {

    private final HistorydbIO historydbIO;

    public SystemLoadWriter(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }


    /**
     * The method creates a record for the table system_load,
     * taking as parameters the values for each attribute of the record.
     */
    private SystemLoadRecord createSystemLoadRecord(String slice, Timestamp snapshot, String uuid,
                               String producerUuid, String propertyType, String propertySubtype,
                               Double capacity, Double avgValue, Double minValue, Double maxValue,
                               RelationType relation, String commodityKey) {
        SystemLoadRecord record = new SystemLoadRecord();
        record.setSlice(slice);
        record.setSnapshotTime(snapshot);
        record.setUuid(uuid);
        record.setProducerUuid(producerUuid);
        record.setPropertyType(propertyType);
        record.setPropertySubtype(propertySubtype);
        record.setCapacity(capacity);
        record.setAvgValue(avgValue);
        record.setMinValue(minValue);
        record.setMaxValue(maxValue);
        record.setRelation(relation);
        record.setCommodityKey(commodityKey);
        return record;
    }

    /**
     * The method inserts a record in the table system_load,
     * taking as parameters the values for each attribute of the record.
     */
    public void insertSystemLoadRecord(String slice, Timestamp snapshot, String uuid,
                               String producerUuid, String propertyType, String propertySubtype,
                               Double capacity, Double avgValue, Double minValue, Double maxValue,
                               RelationType relation, String commodityKey)
                            throws VmtDbException {

        SystemLoadRecord record = createSystemLoadRecord(slice, snapshot, uuid, producerUuid,
                propertyType, propertySubtype, capacity, avgValue, minValue, maxValue, relation,
                commodityKey);
        Query query = HistorydbIO.getJooqBuilder().
                insertInto(SystemLoad.SYSTEM_LOAD).set(record);

        historydbIO.execute(query);
    }

    /**
     * The method deletes all the system load related records for a specific slice in the day
     * snapshot.
     *
     * @param slice The slice we want to delete.
     * @param snapshot The snapshot for which we want to delete all the records for that day.
     * @throws VmtDbException
     * @throws ParseException
     */
    public void deleteSystemLoadRecords(String slice, long snapshot)
                            throws VmtDbException, ParseException {

        Date snapshotDate = new Date(snapshot);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date today = sdf.parse(sdf.format(snapshotDate));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(today);
        calendar.add(Calendar.DATE, 1);
        Date tomorrow = calendar.getTime();

        DeleteConditionStep<SystemLoadRecord> queryBuilder = historydbIO.JooqBuilder()
                .delete(SystemLoad.SYSTEM_LOAD)
                .where(SystemLoad.SYSTEM_LOAD.SLICE.eq(slice)
                .and(SystemLoad.SYSTEM_LOAD.SNAPSHOT_TIME.between(new Timestamp(today.getTime()), new Timestamp(tomorrow.getTime()))));

        historydbIO.execute(queryBuilder);
    }
}
