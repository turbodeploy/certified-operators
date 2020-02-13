package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jooq.Record;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor.FullMarketRatioProcessorFactory;

public class FullMarketRatioProcessorTest {
    private static final Timestamp TIMESTAMP_1 = new Timestamp(1_000_000);
    private static final Timestamp TIMESTAMP_2 = new Timestamp(2_000_000);

    private RatioRecordFactory ratioRecordFactory = mock(RatioRecordFactory.class);

    private FullMarketRatioProcessorFactory processorFactory =
        new FullMarketRatioProcessorFactory(ratioRecordFactory);

    @Test
    public void testNoRatios() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName("randomCommodity"))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();

        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);
        final StatsFilter statsFilterWithCounts = processor.getFilterWithCounts();
        // A filter with no ratios should be returned as-is.
        assertThat(statsFilterWithCounts, is(statsFilter));
    }

    @Test
    public void testAddExtraCommodityRequests() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName("randomCommodity"))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS_PER_HOST)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();

        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);
        final StatsFilter statsFilterWithCounts = processor.getFilterWithCounts();

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

    @Test
    public void testProcessResultEntityCounts() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS_PER_HOST)
                .addPropertyValueFilter(PropertyValueFilter.newBuilder()
                    .setProperty(StringConstants.ENVIRONMENT_TYPE)
                    .setValue(UIEnvironmentType.ON_PREM.getApiEnumStringValue())))
            .build();
        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);

        final MarketStatsLatestRecord ts1NumVmsRecord = new MarketStatsLatestRecord();
        ts1NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts1NumVmsRecord.setAvgValue(10.0);
        ts1NumVmsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts2NumVmsRecord = new MarketStatsLatestRecord();
        ts2NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts2NumVmsRecord.setAvgValue(20.0);
        ts2NumVmsRecord.setSnapshotTime(TIMESTAMP_2);

        final MarketStatsLatestRecord ts1NumHostsRecord = new MarketStatsLatestRecord();
        ts1NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts1NumHostsRecord.setAvgValue(1.0);
        ts1NumHostsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts2NumHostsRecord = new MarketStatsLatestRecord();
        ts2NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts2NumHostsRecord.setAvgValue(2.0);
        ts2NumHostsRecord.setSnapshotTime(TIMESTAMP_2);

        Record fakeRecord = mock(Record.class);
        when(ratioRecordFactory.makeRatioRecord(any(), any(), any())).thenReturn(fakeRecord);
        final List<Record> result = processor.processResults(
            Arrays.asList(ts1NumVmsRecord, ts2NumVmsRecord, ts1NumHostsRecord, ts2NumHostsRecord),
            TIMESTAMP_2);

        verify(ratioRecordFactory).makeRatioRecord(TIMESTAMP_1,
            StringConstants.NUM_VMS_PER_HOST,
            ImmutableMap.of(
                StringConstants.VIRTUAL_MACHINE, 10,
                StringConstants.PHYSICAL_MACHINE, 1));

        verify(ratioRecordFactory).makeRatioRecord(TIMESTAMP_2,
            StringConstants.NUM_VMS_PER_HOST,
            ImmutableMap.of(
                StringConstants.VIRTUAL_MACHINE, 20,
                StringConstants.PHYSICAL_MACHINE, 2));


        // The result should only contain the fake records - the count records should get
        // filtered out because we didn't explicitly request for them.
        assertThat(result, containsInAnyOrder(fakeRecord, fakeRecord));
    }

    @Test
    public void testProcessResultEntityCountsHybridEnvironment() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS_PER_HOST))
            .build();
        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);

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

        Record fakeRecord = mock(Record.class);
        when(ratioRecordFactory.makeRatioRecord(any(), any(), any())).thenReturn(fakeRecord);
        final List<Record> result = processor.processResults(
            Arrays.asList(onPremVmRecord, cloudVmRecord, onPremHostRecord, cloudHostRecord),
            TIMESTAMP_2);

        verify(ratioRecordFactory).makeRatioRecord(TIMESTAMP_1,
            StringConstants.NUM_VMS_PER_HOST,
            ImmutableMap.of(
                StringConstants.VIRTUAL_MACHINE, 30,
                StringConstants.PHYSICAL_MACHINE, 3));

        // The result should only contain the fake records - the count records should get
        // filtered out because we didn't explicitly request for them.
        assertThat(result, containsInAnyOrder(fakeRecord));
    }

    @Test
    public void testProcessResultEntityCountsKeepsRequestedCounts() {
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.NUM_VMS_PER_HOST)
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
        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);

        final MarketStatsLatestRecord ts1NumVmsRecord = new MarketStatsLatestRecord();
        ts1NumVmsRecord.setPropertyType(StringConstants.NUM_VMS);
        ts1NumVmsRecord.setAvgValue(10.0);
        ts1NumVmsRecord.setSnapshotTime(TIMESTAMP_1);

        final MarketStatsLatestRecord ts1NumHostsRecord = new MarketStatsLatestRecord();
        ts1NumHostsRecord.setPropertyType(StringConstants.NUM_HOSTS);
        ts1NumHostsRecord.setAvgValue(1.0);
        ts1NumHostsRecord.setSnapshotTime(TIMESTAMP_1);

        final Record fakeRecord = mock(Record.class);
        when(ratioRecordFactory.makeRatioRecord(any(), any(), any())).thenReturn(fakeRecord);

        final List<Record> result = processor.processResults(
            Arrays.asList(ts1NumVmsRecord, ts1NumHostsRecord),
            TIMESTAMP_2);

        // The result should have the fake record, but also the VM record, since it was explicitly
        // asked for.
        assertThat(result, containsInAnyOrder(fakeRecord, ts1NumVmsRecord));
    }

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
        final FullMarketRatioProcessor processor = processorFactory.newProcessor(statsFilter);

        final List<Record> result = processor.processResults(Collections.emptyList(), TIMESTAMP_2);

        // The result should have a VM record with a count of zero, since it was explicitly
        // asked for.
        assertEquals(1, result.size());
    }

}