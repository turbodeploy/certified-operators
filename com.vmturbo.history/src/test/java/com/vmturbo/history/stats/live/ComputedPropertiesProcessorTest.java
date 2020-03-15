package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_LATEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jooq.Record;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.RecordsProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.StatsRecordsProcessor;

/**
 * Tests for {@link ComputedPropertiesProcessor} class.
 */
public class ComputedPropertiesProcessorTest {
    private static final Timestamp TIMESTAMP_1 = new Timestamp(1_000_000);
    private static final Timestamp TIMESTAMP_2 = new Timestamp(2_000_000);

    /**
     * Test the stats filter is not changed when there are no computed property types in the request.
     */
    @Test
    public void testNoFilterChangeWithNoComputedProperties() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName("randomCommodity"))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();

        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, new StatsRecordsProcessor(PM_STATS_LATEST));
        final StatsFilter statsFilterWithCounts = processor.getAugmentedFilter();
        // A filter with no ratios should be returned as-is.
        assertThat(statsFilterWithCounts, is(statsFilter));
    }

    /**
     * Test that the processed stats filter is correct when we request a computed property without
     * its prereqs, and we also have another property in the original request.
     */
    @Test
    public void testComputedPropertyRequestIsPropertyAugmented() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName("randomCommodity"))
            .addCommodityRequests(CommodityRequest.newBuilder()
                    .setCommodityName(NUM_VMS_PER_HOST)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();

        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, new StatsRecordsProcessor(PM_STATS_LATEST));
        final StatsFilter statsFilterWithCounts = processor.getAugmentedFilter();

        // Assert that the commodity requests match - we should insert two new commodities.
        assertThat(statsFilterWithCounts.getCommodityRequestsList(),
            containsInAnyOrder(
                // The "random commodity" should still be there, but the ratio commodity shouldn't.
                statsFilter.getCommodityRequests(0),
                CommodityRequest.newBuilder()
                    .setCommodityName(StringConstants.NUM_VMS)
                    .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                        .setProperty(StringConstants.ENVIRONMENT_TYPE)
                        .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue()))
                    .build(),
                CommodityRequest.newBuilder()
                    .setCommodityName(StringConstants.NUM_HOSTS)
                    .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                        .setProperty(StringConstants.ENVIRONMENT_TYPE)
                        .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue()))
                    .build()));
    }

    /**
     * Test that records retrieved because of metric properties added to the original filter
     * are removed during the post-processing phase.
     */
    @Test
    public void testThatAddedRecordsAreRemoved() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                    .setCommodityName(NUM_VMS_PER_HOST)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();
        final RecordsProcessor recordsProcesor = new StatsRecordsProcessor(VM_STATS_LATEST);
        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, recordsProcesor);

        final MarketStatsLatestRecord ts1NumVmsRecord = new MarketStatsLatestRecord();
        ts1NumVmsRecord.setSnapshotTime(TIMESTAMP_1);
        ts1NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts1NumVmsRecord.setAvgValue(10.0);
        ts1NumVmsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts2NumVmsRecord = new MarketStatsLatestRecord();
        ts2NumVmsRecord.setSnapshotTime(TIMESTAMP_2);
        ts2NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts2NumVmsRecord.setAvgValue(20.0);
        ts2NumVmsRecord.setSnapshotTime(TIMESTAMP_2);

        final MarketStatsLatestRecord ts1NumHostsRecord = new MarketStatsLatestRecord();
        ts1NumHostsRecord.setSnapshotTime(TIMESTAMP_1);
        ts1NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts1NumHostsRecord.setAvgValue(1.0);
        ts1NumHostsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts2NumHostsRecord = new MarketStatsLatestRecord();
        ts2NumHostsRecord.setSnapshotTime(TIMESTAMP_2);
        ts2NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts2NumHostsRecord.setAvgValue(2.0);
        ts2NumHostsRecord.setSnapshotTime(TIMESTAMP_2);

        Record fakeRecord = mock(Record.class);
        final List<Record> result = processor.processResults(
                Arrays.asList(ts1NumVmsRecord, ts2NumVmsRecord, ts1NumHostsRecord, ts2NumHostsRecord),
                null);
        assertThat(result.size(), is(2));
        assertThat(result.get(0).get(PROPERTY_TYPE, String.class), is(NUM_VMS_PER_HOST));
        assertThat(result.get(1).get(PROPERTY_TYPE, String.class), is(NUM_VMS_PER_HOST));
    }

    /**
     * Test that when postprocessing results from multiple environmetns, added metrics
     * properties are removed as required.
     */
    @Test
    public void testThatMetricsPropertiesAreRemovedInHybridEnvironmentRequest() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                    .setCommodityName(NUM_VMS_PER_HOST))
            .build();
        final RecordsProcessor recordsProcessor = new StatsRecordsProcessor(VM_STATS_LATEST);
        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, recordsProcessor);

        final MarketStatsLatestRecord onPremVmRecord = new MarketStatsLatestRecord();
        onPremVmRecord.setPropertyType(StringConstants.NUM_VMS);
        onPremVmRecord.setAvgValue(10.0);
        onPremVmRecord.setEnvironmentType(EnvironmentType.ON_PREM);
        onPremVmRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord cloudVmRecord = new MarketStatsLatestRecord();
        cloudVmRecord.setPropertyType(StringConstants.NUM_VMS);
        cloudVmRecord.setEnvironmentType(EnvironmentType.CLOUD);
        cloudVmRecord.setAvgValue(20.0);
        cloudVmRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord onPremHostRecord = new MarketStatsLatestRecord();
        onPremHostRecord.setPropertyType(StringConstants.NUM_HOSTS);
        onPremHostRecord.setEnvironmentType(EnvironmentType.ON_PREM);
        onPremHostRecord.setAvgValue(1.0);
        onPremHostRecord.setSnapshotTime(TIMESTAMP_1);

        // There are no hosts in the cloud, but for testing sake we assume there are.
        final MarketStatsLatestRecord cloudHostRecord = new MarketStatsLatestRecord();
        cloudHostRecord.setPropertyType(StringConstants.NUM_HOSTS);
        cloudHostRecord.setEnvironmentType(EnvironmentType.CLOUD);
        cloudHostRecord.setAvgValue(2.0);
        cloudHostRecord.setSnapshotTime(TIMESTAMP_1);

        final List<Record> result = processor.processResults(
                Arrays.asList(onPremVmRecord, cloudVmRecord, onPremHostRecord, cloudHostRecord),
                null);
        assertThat(result.size(), is(1));
        assertThat(result.get(0).get(PROPERTY_TYPE, String.class), is(NUM_VMS_PER_HOST));
    }

    /**
     * Test that when a request already includes metric properties needed for a computed property,
     * they are preserved in the results by post-processing.
     */
    @Test
    public void testProcessResultEntityCountsKeepsRequestedCounts() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                    .setCommodityName(NUM_VMS_PER_HOST)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            // Ask for num vms explicitly.
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();
        final StatsRecordsProcessor recordsProcessor = mock(StatsRecordsProcessor.class);
        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, recordsProcessor);

        final MarketStatsLatestRecord ts1NumVmsRecord = new MarketStatsLatestRecord();
        ts1NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts1NumVmsRecord.setAvgValue(10.0);
        ts1NumVmsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts1NumHostsRecord = new MarketStatsLatestRecord();
        ts1NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts1NumHostsRecord.setAvgValue(1.0);
        ts1NumHostsRecord.setSnapshotTime(TIMESTAMP_1);

        final Record fakeRecord = mock(Record.class);
        when(recordsProcessor.getRecordGroupKey(any())).thenReturn(TIMESTAMP_1);
        when(recordsProcessor.createRecord(any(Timestamp.class), any(), any())).thenReturn(fakeRecord);

        final List<Record> result = processor.processResults(
                Arrays.asList(ts1NumVmsRecord, ts1NumHostsRecord), null);

        // The result should have the fake record, but also the VM record, since it was explicitly
        // asked for.
        assertThat(result, containsInAnyOrder(fakeRecord, ts1NumVmsRecord));
    }

    /**
     * Test that when metrics properties are in the original request and its missing for some
     * of the record sets, zero-valued records are added.
     */
    @Test
    public void testProcessResultFillsInMissingEntityCounts() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            // Ask for num vms explicitly.
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();
        final ComputedPropertiesProcessor processor = new ComputedPropertiesProcessor(
                statsFilter, new StatsRecordsProcessor(PM_STATS_LATEST));

        final List<Record> result = processor.processResults(Collections.emptyList(), TIMESTAMP_1);

        // The result should have a VM record with a count of zero, since it was explicitly
        // asked for.
        assertEquals(1, result.size());
    }

}
