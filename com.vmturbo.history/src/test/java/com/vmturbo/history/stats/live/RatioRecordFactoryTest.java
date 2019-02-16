package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import java.sql.Timestamp;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;

/**
 * Unit tests for {@link RatioRecordFactory}.
 */
public class RatioRecordFactoryTest {

    private RatioRecordFactory recordFactory = new RatioRecordFactory();

    private static final Timestamp TIMESTAMP = new Timestamp(1_000_000);

    @Test
    public void testVmsPerHost() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_HOST,
            ImmutableMap.of(StringConstants.VIRTUAL_MACHINE, 10,
                StringConstants.PHYSICAL_MACHINE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(5.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testVmsPerHostMissingHostCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_HOST,
            // No PM count in the map.
            ImmutableMap.of(StringConstants.VIRTUAL_MACHINE, 10));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testVmsPerHostMissingVmCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_HOST,
            // No VM count in the map.
            ImmutableMap.of(StringConstants.PHYSICAL_MACHINE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }


    @Test
    public void testVmsPerStorage() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_STORAGE,
            ImmutableMap.of(StringConstants.VIRTUAL_MACHINE, 10,
                StringConstants.STORAGE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(5.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testVmsPerStorageMissingStorageCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_STORAGE,
            // No PM count in the map.
            ImmutableMap.of(StringConstants.VIRTUAL_MACHINE, 10));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testVmsPerStorageMissingVmCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_VMS_PER_STORAGE,
            // No VM count in the map.
            ImmutableMap.of(StringConstants.STORAGE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_VMS_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testContainersPerHost() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_HOST,
            ImmutableMap.of(StringConstants.CONTAINER, 10,
                StringConstants.PHYSICAL_MACHINE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(5.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testContainersPerHostMissingHostCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_HOST,
            // No PM count in the map.
            ImmutableMap.of(StringConstants.CONTAINER, 10));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testContainersPerHostMissingContainerCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_HOST,
            // No VM count in the map.
            ImmutableMap.of(StringConstants.PHYSICAL_MACHINE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_HOST));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }


    @Test
    public void testContainersPerStorage() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_STORAGE,
            ImmutableMap.of(StringConstants.CONTAINER, 10,
                StringConstants.STORAGE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(5.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testContainersPerStorageMissingStorageCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_STORAGE,
            // No PM count in the map.
            ImmutableMap.of(StringConstants.CONTAINER, 10));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }

    @Test
    public void testContainersPerStorageMissingContainerCount() {
        final MarketStatsLatestRecord record = (MarketStatsLatestRecord)recordFactory.makeRatioRecord(TIMESTAMP,
            StringConstants.NUM_CNT_PER_STORAGE,
            // No VM count in the map.
            ImmutableMap.of(StringConstants.STORAGE, 2));

        assertThat(record.getSnapshotTime(), is(TIMESTAMP));
        assertThat(record.getPropertyType(), is(StringConstants.NUM_CNT_PER_STORAGE));
        assertThat(record.getAvgValue(), closeTo(0.0, 0.00001));
        assertThat(record.getRelation(), is(RelationType.METRICS));
    }
}