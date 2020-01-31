package com.vmturbo.history.stats.live;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import jersey.repackaged.com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SelectConditionStep;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.utils.SystemLoadCommodities;
import com.vmturbo.history.utils.SystemLoadHelper;

/**
 * The class SystemLoadReader reads the system load information from the DB.
 */
public class SystemLoadReader {

    // This is the number of days we look in the past to choose a system load snapshot.
    private static final int LOOPBACK_DAYS = 10;

    // When we order the system loads of the past days from smaller to bigger, we choose the
    // system load which is in the SYSTEM_LOAD_PERCENTILE position.
    private static final double SYSTEM_LOAD_PERCENTILE = 90;

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

                Optional<SystemLoadCommodities> optCommodity
                    = SystemLoadCommodities.toSystemLoadCommodity(propertySubtype);
                // ignore unrecognized commodities. They can appear during upgrades when we have
                // changed what is considered a system load commodity
                if (optCommodity.isPresent()) {
                    final SystemLoadCommodities commodity = optCommodity.get();
                    if (commodity != null) {

                        if (slice2used.containsKey(slice)) {
                            Double[] tempUsed = slice2used.get(slice);
                            tempUsed[commodity.ordinal()] = used;
                            slice2used.put(slice, tempUsed);
                        } else {
                            Double[] tempUsed = new Double[SystemLoadCommodities.SIZE];
                            tempUsed[commodity.ordinal()] = used;
                            slice2used.put(slice, tempUsed);
                        }

                        if (slice2capacities.containsKey(slice)) {
                            Double[] tempCapacities = slice2capacities.get(slice);
                            tempCapacities[commodity.ordinal()] = capacity;
                            slice2capacities.put(slice, tempCapacities);
                        } else {
                            Double[] tempCapacities = new Double[SystemLoadCommodities.SIZE];
                            tempCapacities[commodity.ordinal()] = capacity;
                            slice2capacities.put(slice, tempCapacities);
                        }
                    }
                }
            }

            for (String slice : slice2used.keySet()) {

                if (!slice2capacities.containsKey(slice)) {
                    logger.error("SYSLOAD- Error in the system load data of DB");
                    continue;
                }

                Double[] used = slice2used.get(slice);
                Double[] capacities = slice2capacities.get(slice);

                Double previousLoad = systemLoadHelper.calcSystemLoad(used, capacities, slice);

                systemLoad.put(slice, new Pair<>(previousLoad, snapshotDate));
            }
        }
        catch (VmtDbException e) {
            logger.error("Error when initializing system load : " + e);
        }

        return systemLoad;
    }

    /**
     * The method reads all the system load related records for a specific slice and the
     * chosen system load which have a VM associated with them.
     * We calculate the system loads for the past LOOPBACK_DAYS, we order then and we choose
     * the system load in the SYSTEM_LOAD_PERCENTILE position.
     *
     * @param slice The slice we want to read.
     * @return A list of the system load records.
     */
    public List<SystemLoadRecord> getSystemLoadInfo(String slice) {
        final long snapshot = System.currentTimeMillis();
        Date snapshotDate = new Date(snapshot);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date today = null;
        try {
            today = sdf.parse(sdf.format(snapshotDate));
        } catch (ParseException e) {
            logger.error("SYSLOAD- Error when parsing snaphot date during initialization of system load");
            return new ArrayList<SystemLoadRecord>();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(today);

        List<Pair<Integer, Double>> systemLoads = new ArrayList<Pair<Integer, Double>>();

        // Loading the "system_load" records from past LOOPBACK_DAYS and calculating the system
        // load value for each of these dates
        for (int i = 0; i < LOOPBACK_DAYS; i++) {
            calendar.add(Calendar.DATE, -i);            // calculate the start time of the day i+1 days before
            Date endTime = calendar.getTime();
            calendar.add(Calendar.DATE, -1);    // calcualte the end time of the day i+1 days before
            Date startTime = calendar.getTime();
            calendar.setTime(today);                    // reset to today to prepare for next iteration calculations

            SelectConditionStep<SystemLoadRecord> systemLoadQueryBuilder = historydbIO.JooqBuilder()
                    .selectFrom(SystemLoad.SYSTEM_LOAD)
                    .where(SystemLoad.SYSTEM_LOAD.SNAPSHOT_TIME.between(new Timestamp(startTime.getTime()), new Timestamp(endTime.getTime())))
                    .and(SystemLoad.SYSTEM_LOAD.PROPERTY_TYPE.eq("system_load"))
                    .and(SystemLoad.SYSTEM_LOAD.SLICE.eq(slice));

            List<SystemLoadRecord> systemLoadRecords = null;

            try {
                systemLoadRecords = (List<SystemLoadRecord>) historydbIO.execute(systemLoadQueryBuilder);
            } catch (VmtDbException e) {
                logger.error("Error when reading the system load records from the DB : " + e);
            }

            double systemLoad = 0;

            // Calculating the system load value
            for (SystemLoadRecord record : systemLoadRecords) {
                double totalUsed = record.getAvgValue();
                double totalCapacity = record.getCapacity();
                double utilization = totalUsed / totalCapacity;
                if (utilization > systemLoad) {
                        systemLoad = utilization;
                }
            }

            systemLoads.add(i, new Pair(i, systemLoad));
        }

        // Sorting the system load values of the past LOOPBACK_DAYS to choose the system load snapshot
        // which is at the SYSTEM_LOAD_PERCENTILE of the ordered system loads.
        // If 2 days have the same system load we choose the closest day to the present.
        systemLoads.sort((p1, p2) -> {
            final double load1 = p1.second;
            final double load2 = p2.second;
            final int day1 = p1.first;
            final int day2 = p2.first;

            int loadCompare = Double.compare(load1, load2);
            return loadCompare == 0 ? Integer.compare(day2, day1) : loadCompare; // earlier day has bigger number integer in Pair
        });

        final int index = (int) ((systemLoads.size() - 1) * SYSTEM_LOAD_PERCENTILE / 100);
        final Pair<Integer, Double> load = systemLoads.get(index);

        // Loading and returning all the system load records for the chosen snapshot
        calendar.add(Calendar.DATE, -load.first);
        Date endTime = calendar.getTime();
        calendar.add(Calendar.DATE, -1);
        Date startTime = calendar.getTime();

        logger.debug("System load chosen for the date {}.", startTime.toString());

        // Query to not return values where uuid is null and we will not get records with vmId = 0. We are only interested in records
        // which have a VM associated with them but some records, for example, represent cluster info like total capacity or system load
        // which are not required by the caller.
        SelectConditionStep<SystemLoadRecord> queryBuilder = historydbIO.JooqBuilder()
                .selectFrom(SystemLoad.SYSTEM_LOAD)
                .where(SystemLoad.SYSTEM_LOAD.SNAPSHOT_TIME.between(new Timestamp(startTime.getTime()), new Timestamp(endTime.getTime())))
                .and(SystemLoad.SYSTEM_LOAD.SLICE.eq(slice))
                .and(SystemLoad.SYSTEM_LOAD.UUID.isNotNull());

        List<SystemLoadRecord> records = null;

        try {
            records = (List<SystemLoadRecord>) historydbIO.execute(queryBuilder);
        } catch (VmtDbException e) {
            logger.error("Error when reading the system load info from the DB : " + e);
        }
        return records;
    }
}
