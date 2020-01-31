package com.vmturbo.history.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jooq.DeleteConditionStep;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;

/**
 * The class SystemLoadWriter stores the system load information to the DB.
 */
public class SystemLoadDbUtil {

    private static double maxCapacity = 0.0;

    private SystemLoadDbUtil() {}

    /**
     * The method creates a record for the table system_load,
     * taking as parameters the values for each attribute of the record.
     * @param slice           the slice for the record
     * @param snapshot        snapshot time
     * @param uuid            uuid
     * @param producerUuid    producer uuid
     * @param propertyType    property type
     * @param propertySubtype property subtype
     * @param capacity        capacity
     * @param avgValue        average value
     * @param minValue        min value
     * @param maxValue        max value
     * @param relation        boubht/sold
     * @param commodityKey    commodity key
     * @return record for system_load DB table
     */
    public static SystemLoadRecord createSystemLoadRecord(String slice, Timestamp snapshot, String uuid,
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
     * The method deletes all the system load related records for a specific slice in the day
     * snapshot.
     *
     * @param slice The slice we want to delete.
     * @param snapshot The snapshot for which we want to delete all the records for that day.
     * @param historydbIO db methods
     * @throws VmtDbException if a db operation fails
     * @throws ParseException if interrupted
     */
    public static void deleteSystemLoadRecords(String slice, long snapshot, HistorydbIO historydbIO)
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
