package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;
import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.history.db.bulk.RollupKey.RollupTimestamps;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory.RollupKeyTransfomer;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.RelationTypeConverter;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.PropertySubType;

/**
 * Test the record transformer that adds rollup keys to entity stats records.
 */
public class RollupKeyTest {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private VmStatsLatestRecord record;
    private Instant snapshot_time;
    private String rollupAttrs;
    private RollupKeyTransfomer<VmStatsLatestRecord> transformer;

    /**
     * Set up for tests.
     */
    @Before
    public void before() {
        snapshot_time = Instant.now();
        record = new VmStatsLatestRecord();
        record.setSnapshotTime(Timestamp.from(snapshot_time));
        record.setUuid("entity");
        record.setProducerUuid(null);
        record.setPropertyType("CPU");
        record.setPropertySubtype(PropertySubType.Used.getApiParameterName());
        record.setRelation(RelationType.COMMODITIES);
        record.setCommodityKey(null);
        String relTypeAttr = String.valueOf(
                new RelationTypeConverter().to((RelationType)record.getRelation()));
        rollupAttrs = "entity-CPUused" + relTypeAttr + "-";
        transformer = new RollupKeyTransfomer<VmStatsLatestRecord>();
    }

    /**
     * Check generation of hour rollup keys by creating a record with the current time and ensuring
     * that transformed record has the correct hourKey value.
     */
    @Test
    public void testHourKey() {
        String hourString = RollupKey.rollupTimestamps(record).hour();
        String expected = DigestUtils.md5Hex(hourString + rollupAttrs);
        final VmStatsLatestRecord transformed
                = transformer.transform(record, VM_STATS_LATEST, VM_STATS_LATEST).get();
        assertEquals(expected, record.getHourKey());
    }

    /**
     * Check generation of day rollup keys by creating a record with the current time and ensuring
     * that transformed record has the correct dayKey value.
     */
    @Test
    public void testDayKey() {
        String dayString = RollupKey.rollupTimestamps(record).day();
        String expected = DigestUtils.md5Hex(dayString + rollupAttrs);
        final VmStatsLatestRecord transformed
                = transformer.transform(record, VM_STATS_LATEST, VM_STATS_LATEST).get();
        assertEquals(expected, record.getDayKey());
    }

    /**
     * Check generation of month rollup keys by creating a record with the current time and ensuring
     * that transformed record has the correct monthKey value.
     */
    @Test
    public void testMonthKey() {
        String monthString = RollupKey.rollupTimestamps(record).month();
        String expected = DigestUtils.md5Hex(monthString + rollupAttrs);
        final VmStatsLatestRecord transformed
                = transformer.transform(record, VM_STATS_LATEST, VM_STATS_LATEST).get();
        assertEquals(expected, record.getMonthKey());
    }


    private static final ZoneId UTC = ZoneId.of("Z");

    private static final DateTimeFormatter hourFormmatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:00:00")
            .withZone(UTC);

    private static final DateTimeFormatter dayFormatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd 00:00:00")
            .withZone(UTC);

    /**
     * Test that rollup timestamps are being generated propertly.
     *
     * <p>We test a large collection of timestamps to ensure there are unlikely to be faulty edge
     * cases.</p>
     */
    @Test
    public void testTimetstampTruncation() {
        Instant t0 = Instant.parse("2019-10-01T00:00:00Z");
        // monthly rollups use midnight of the last day of the month
        String m0String = "2019-10-31 00:00:00";
        // test timestamps rollup timestamps, on first day, middle day, and final day of a month
        checkRollups(t0, m0String); // 10/1
        checkRollups(t0.plus(14, ChronoUnit.DAYS), m0String); // 10/15
        checkRollups(t0.plus(30, ChronoUnit.DAYS), m0String); // 10/31
    }

    private void checkRollups(final Instant t0, final String m0String) {
        // check timestamps at one-minute intervals throughout the day
        for (int i = 0; i < TimeUnit.DAYS.toSeconds(1); i += TimeUnit.MINUTES.toSeconds(1)) {
            final Instant t = t0.plus(i, ChronoUnit.SECONDS);
            final RollupTimestamps rollups = RollupKey.getRollupTimestamps(t);
            final LocalDateTime ldt = LocalDateTime.ofInstant(t, UTC);
            assertEquals(hourFormmatter.format(ldt), rollups.hour());
            assertEquals(dayFormatter.format(ldt), rollups.day());
            assertEquals(m0String, rollups.month());
        }
    }
}
