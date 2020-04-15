package com.vmturbo.history.stats.live;

import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.SelectConditionStep;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;

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

    /**
     * Create a new instance.
     *
     * @param historydbIO DB utilites.
     */
    public SystemLoadReader(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Get previously saved overall system load values for for all slices.
     *
     * @param fromInclusive inclusive lower bound on snapshot time
     * @param toInclusive   inclusive upper bound on snapshot time
     * @return list of slice system load values within specified range
     */
    public Map<Long, Double> getSystemLoadValues(final Timestamp fromInclusive, final Timestamp toInclusive) {
        try (Connection conn = historydbIO.connection()) {
            final Result<Record2<String, Double>> results =
                    historydbIO.using(conn).select(SYSTEM_LOAD.SLICE, SYSTEM_LOAD.AVG_VALUE)
                            .from(SYSTEM_LOAD)
                            .where(SYSTEM_LOAD.SNAPSHOT_TIME.between(fromInclusive, toInclusive)
                                    .and(SYSTEM_LOAD.PROPERTY_TYPE.eq(StringConstants.SYSTEM_LOAD))
                                    .and(SYSTEM_LOAD.PROPERTY_SUBTYPE.eq(StringConstants.SYSTEM_LOAD)))
                            .fetch();
            return results.stream().collect(ImmutableMap.toImmutableMap(
                    rec -> Long.valueOf(rec.value1()),
                    Record2::value2));
        } catch (SQLException | VmtDbException e) {
            logger.error("Failed to retrieve system load values between {} and {}",
                    fromInclusive, toInclusive, e);
            return ImmutableMap.of();
        }
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
        Date today;
        try {
            today = sdf.parse(sdf.format(snapshotDate));
        } catch (ParseException e) {
            logger.error("SYSLOAD- Error when parsing snaphot date during initialization of system load");
            return new ArrayList<>();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(today);

        List<Pair<Integer, Double>> systemLoads = new ArrayList<>();

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
                systemLoadRecords = (List<SystemLoadRecord>)historydbIO.execute(systemLoadQueryBuilder);
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

            systemLoads.add(i, Pair.of(i, systemLoad));
        }

        // Sorting the system load values of the past LOOPBACK_DAYS to choose the system load snapshot
        // which is at the SYSTEM_LOAD_PERCENTILE of the ordered system loads.
        // If 2 days have the same system load we choose the closest day to the present.
        systemLoads.sort((p1, p2) -> {
            final double load1 = p1.getRight();
            final double load2 = p2.getRight();
            final int day1 = p1.getLeft();
            final int day2 = p2.getLeft();

            int loadCompare = Double.compare(load1, load2);
            return loadCompare == 0 ? Integer.compare(day2, day1) : loadCompare; // earlier day has bigger number integer in Pair
        });

        final int index = (int)((systemLoads.size() - 1) * SYSTEM_LOAD_PERCENTILE / 100);
        final Pair<Integer, Double> load = systemLoads.get(index);

        // Loading and returning all the system load records for the chosen snapshot
        calendar.add(Calendar.DATE, -load.getLeft());
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
            records = (List<SystemLoadRecord>)historydbIO.execute(queryBuilder);
        } catch (VmtDbException e) {
            logger.error("Error when reading the system load info from the DB : " + e);
        }
        return records;
    }
}
