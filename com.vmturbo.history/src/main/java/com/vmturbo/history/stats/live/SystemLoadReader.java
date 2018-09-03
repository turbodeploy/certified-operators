package com.vmturbo.history.stats.live;

import javax.annotation.Nonnull;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Date;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SelectConditionStep;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.history.utils.SystemLoadCommodities;

import jersey.repackaged.com.google.common.collect.Maps;

/**
 * The class SystemLoadReader reads the system load information from the DB.
 */
public class SystemLoadReader {

    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private SystemLoadHelper systemLoadHelper = null;

    public SystemLoadReader(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    public void setSystemLoadUtils(SystemLoadHelper systemLoadHelper) {
        this.systemLoadHelper = systemLoadHelper;
    }

    /**
     * The method loads the system load info of the current day in memory.
     *
     * @return The system load info (value, date) per slice.
     */
    public @Nonnull Map<String, Pair<Double, Date>> initSystemLoad() {
            Map<String, Pair<Double, Date>> systemLoad = Maps.newHashMap();
        try {
            final long snapshot = System.currentTimeMillis();
            Date snapshotDate = new Date(snapshot);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date today = null;
            try {
                today = sdf.parse(sdf.format(snapshotDate));
            }
            catch (ParseException e) {
                logger.error("SYSLOAD- Error when parsing snaphot date during initialization of system load");
            }
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(today);
            calendar.add(Calendar.DATE, 1);
            Date tomorrow = calendar.getTime();


            SelectConditionStep<SystemLoadRecord> queryBuilder = historydbIO.JooqBuilder()
                    .selectFrom(SystemLoad.SYSTEM_LOAD)
                    .where(SystemLoad.SYSTEM_LOAD.SNAPSHOT_TIME.between(new Timestamp(today.getTime()), new Timestamp(tomorrow.getTime())))
                    .and(SystemLoad.SYSTEM_LOAD.PROPERTY_TYPE.eq("system_load"));

            Map<String, Double[]> slice2used = Maps.newHashMap();
            Map<String, Double[]> slice2capacities = Maps.newHashMap();
            List<SystemLoadRecord> records = (List<SystemLoadRecord>) historydbIO.execute(queryBuilder);

            for (SystemLoadRecord record : records) {
                String slice = record.getSlice();
                Double used = record.getAvgValue();
                Double capacity = record.getCapacity();
                String propertySubtype = record.getPropertySubtype();
                SystemLoadCommodities commodity = SystemLoadCommodities.toSystemLoadCommodity(propertySubtype);

                if (slice2used.containsKey(slice)) {
                    Double[] tempUsed = slice2used.get(slice);
                    tempUsed[commodity.idx] = used;
                    slice2used.put(slice, tempUsed);
                } else {
                    Double[] tempUsed = new Double[SystemLoadCommodities.SIZE];
                    tempUsed[commodity.idx] = used;
                    slice2used.put(slice, tempUsed);
                }

                if (slice2capacities.containsKey(slice)) {
                    Double[] tempCapacities = slice2capacities.get(slice);
                    tempCapacities[commodity.idx] = capacity;
                    slice2capacities.put(slice, tempCapacities);
                } else {
                    Double[] tempCapacities = new Double[SystemLoadCommodities.SIZE];
                    tempCapacities[commodity.idx] = capacity;
                    slice2capacities.put(slice, tempCapacities);
                }
            }

            for (String slice : slice2used.keySet()) {

                if (!slice2capacities.containsKey(slice)) {
                    logger.error("SYSLOAD- Error in the system load data of DB");
                    continue;
                }

                Double[] used = slice2used.get(slice);
                Double[] capacities = slice2capacities.get(slice);

                Double previousLoad = systemLoadHelper.calcSystemLoad(used, capacities);

                systemLoad.put(slice, new Pair<>(previousLoad, snapshotDate));
            }
        }
        catch (VmtDbException e) {
            logger.error("Error when initializing system load : " + e);
        }

        return systemLoad;
    }

    /**
     * The method reads all the system load related records for a specific slice.
     *
     * @param slice The slice we want to read.
     * @return A list of the system load records.
     */
    public List<SystemLoadRecord> getSystemLoadInfo(String slice) {
        SelectConditionStep<SystemLoadRecord> queryBuilder = historydbIO.JooqBuilder()
                .selectFrom(SystemLoad.SYSTEM_LOAD)
                .where(SystemLoad.SYSTEM_LOAD.SLICE.eq(slice));

        List<SystemLoadRecord> records = null;

        try {
            records = (List<SystemLoadRecord>) historydbIO.execute(queryBuilder);
        }
        catch (VmtDbException e) {
            logger.error("Error when reading the system load info from the DB : " + e);
        }
        return records;
    }
}
