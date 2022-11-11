package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingDataInjector.BillingScriptEvent;
import com.vmturbo.cost.component.savings.BillingDataInjector.Commodity;
import com.vmturbo.cost.component.savings.ScenarioGenerator.Interval;
import com.vmturbo.cost.component.savings.ScenarioGenerator.Segment;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

public class ScenarioGeneratorTest {

    private static final String vm1 = "vm1";
    private static final String vol1 = "vol1";
    private static final String db1 = "db1";

    private static final Map<String, Long> uuidMap = ImmutableMap.of(vm1, 70000L,
            vol1, 10000L, db1, 20000L);

    /**
     * Test the logic of subtracting an interval from another. It is used for adjusting the segment
     * start and/or finish timestamps after considering downtime.
     */
    @Test
    public void testDowntimeLogic() {
        // Downtime interval
        long powerOffTimeMillis = getMillis(2022, 4, 10, 10, 0);
        long powerOnTimeMillis = getMillis(2022, 4, 10, 20, 0);

        Interval powerOffInterval = new Interval(powerOffTimeMillis, powerOnTimeMillis);

        long start = getMillis(2022, 4, 10, 7, 0);
        long end = getMillis(2022, 4, 10, 8, 0);
        Segment s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM", 0, 0.0);
        List<Segment> segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(end, segments.get(0).end);

        start = getMillis(2022, 4, 10, 7, 0);
        end = getMillis(2022, 4, 10, 11, 0);
        s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM",  0, 0.0);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(powerOffTimeMillis, segments.get(0).end);

        start = getMillis(2022, 4, 10, 7, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM",  0, 0.0);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(2, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(powerOffTimeMillis, segments.get(0).end);
        Assert.assertEquals(powerOnTimeMillis, segments.get(1).start);
        Assert.assertEquals(end, segments.get(1).end);

        start = getMillis(2022, 4, 10, 13, 0);
        end = getMillis(2022, 4, 10, 14, 0);
        s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM",  0, 0.0);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(0, segments.size());

        start = getMillis(2022, 4, 10, 13, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM",  0, 0.0);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(powerOnTimeMillis, segments.get(0).start);
        Assert.assertEquals(end, segments.get(0).end);

        start = getMillis(2022, 4, 10, 21, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L, Collections.emptyList(), 0, "VM",  0, 0.0);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(end, segments.get(0).end);
    }

    private long getMillis(int year, int month, int dayOfMonth, int hour, int minute) {
        LocalDateTime dateTime = LocalDateTime.of(year, month, dayOfMonth, hour, minute);
        return TimeUtil.localTimeToMillis(dateTime, Clock.systemUTC());
    }

    /**
     * Test the generation of bill records from script events. Both scale and power events are tested.
     * Scenario:
     * 1. On Apr 25, 2022, scale vm1 from tierA to tierB at 7am.
     * 2. On Apr 25, 2022, power off vm1 at 10pm.
     * 3. On Apr 26, 2022, power on vm1 at 2am.
     * 2. On Apr 26, 2022, power off vm1 at 10am.
     * 3. On Apr 26, 2022, power on vm1 at 1pm.
     * Expect 2 bill records on Apr 25, one for each tier. The segment on dest tier has 15 hours
     * because there are 2 hours downtime.
     * Expect 1 bill record on Apr 26, with 19 hours of usage because vm1 was powered off for 5 hours.
     */
    @Test
    public void testGenerateBillRecords() {
        final double sourceTierRate = 2.0;
        final double destinationTierRate = 1.0;
        List<BillingScriptEvent> events = new ArrayList<>();

        events.add(createScaleEvent(getMillis(2022, 4, 25, 7, 0),
                vm1, sourceTierRate, destinationTierRate, Collections.emptyList(), 0,
                "RESIZE"));
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vm1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 2, 0),
                vm1, true));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 10, 0),
                vm1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 13, 0),
                vm1, true));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createBillingRecord(scenarioStart, uuidMap.get(vm1), sourceTierRate,
                7, 14);
        BillingRecord record2 = createBillingRecord(scenarioStart, uuidMap.get(vm1), destinationTierRate,
                15, 15);
        BillingRecord record3 = createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), destinationTierRate,
                19, 19);
        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vm1))));
    }

    /**
     * Test Ultra Disk creation of One Billing Record, for Ultra Disk segments.
    */
    @Test
    public void testGenerateUltraDisksCommScaleBillRecords() {
        // In this test source and destination tier are the same.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 500, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 2000, 0.7),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 800, 0.8),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 200, 0.6)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "ULTRA");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "ULTRA", "ULTRA", scaleCommodities, 0,
                "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 1000 + 15 * 2000, 7 * 0.5 + 15 * 0.7, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 500 + 15 * 800, 7 * 0.6 + 15 * 0.8, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record3 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 100 + 15 * 200, 7 * 0.4 + 15 * 0.6,  CommodityType.IO_THROUGHPUT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test Ultra Disk creation of One Billing Record, for Ultra Disk segments, when not all commodities change.
     */
    @Test
    public void testGenerateUltraDisksCommScaleBillRecordsPartialCommoditiesUpdate() {
        // In this test source and destination tier are the same.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 500, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 2000, 0.7),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 200, 0.6)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "ULTRA");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "ULTRA", "ULTRA", scaleCommodities, 0,
                "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 1000 + 15 * 2000, 7 * 0.5 + 15 * 0.7, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                (7 + 15) * 500, (7 + 15) * 0.6, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record3 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 100 + 15 * 200, 7 * 0.4 + 15 * 0.6, CommodityType.IO_THROUGHPUT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test creation of One Billing Record, for Managed Premium Disk segments.
     */
    @Test
    public void testGenerateManagedPremiumScaleBillRecord() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 32, 0.6, 32, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_AMOUNT", 32, 0.6, 64, 0.8)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "PREMIUM");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "PREMIUM", "PREMIUM", scaleCommodities,
                0, "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "PREMIUM",
                7 * 32 + 15 * 64, 7 * 0.6 + 15 * 0.8, CommodityType.STORAGE_AMOUNT_VALUE);

        expectedRecords.add(record1);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test creation of one Billing Record for each Standard Tier segment.
     */
    @Test
    public void testGenerateManagedStandardScaleBillRecords() {
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 32, 0.6, 32, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_AMOUNT", 32, 0.6, 48, 0.8)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
               vol1, initiaCommodities, "CREATE-VOL", "STANDARDSDD");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 12, 0),
                vol1, "STANDARDSDD", "STANDARDHDD", scaleCommodities,
                0, "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();

        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "STANDARDSDD",
                32 * 12, 0.6 * 12, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "STANDARDHDD",
                48 * 10, 0.8 * 10, CommodityType.STORAGE_AMOUNT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test creation of one Billing Record for each Premium and Standard Tier segment.
     * This would involve both tier and commodity changes.
     */
    @Test
    public void testGenerateManagedPremiumToStandardScaleBillRecords() {
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 32, 0.6, 32, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_AMOUNT", 32, 0.6, 48, 0.8)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "PREMIUM");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "PREMIUM", "STANDARDSDD", scaleCommodities,
                0, "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();

        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "PREMIUM",
                32 * 7, 0.6 * 7, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "STANDARDSDD",
                48 * 15, 0.8 * 15, CommodityType.STORAGE_AMOUNT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test successive resizes to different storage types.
     */
    @Test
    public void testSuccessiveVolumeResizesToDifferentStorageTiers() {
        // In this test source and destination tier are the same.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 32, 0.6, 32, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        // This test passes in commodities that are not individually scaled for non-ultra disks scale ations.
        // We will still create bill records based only on STORAGE_AMOUNT, for all non-ultra Azure disk types.
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 2000, 0.7),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 200, 0.6)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "PREMIUM");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "PREMIUM", "STANDARDSDD", scaleCommodities,
                0, "RESIZE-VOL");
        BillingScriptEvent event3 = createScaleVolumeEvent(getMillis(2022, 4, 25, 12, 0),
                vol1, "STANDARDSDD", "ULTRA", scaleCommodities, 0,
                "RESIZE-VOL");

        events.add(event1);
        events.add(event2);
        events.add(event3);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();

        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "PREMIUM",
                32 * 7, 0.6 * 7, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "STANDARDSDD",
                32 * 5, 0.6 * 5, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record3 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                10 * 2000, 10 * 0.7, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record4 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                10 * 32, 10 * 0.6, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record5 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                10 * 200, 10 * 0.6,  CommodityType.IO_THROUGHPUT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        expectedRecords.add(record5);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test the generation of bill records from script events. Both scale and power events are tested.
     * Scenario:
     * 1. On Apr 25, 2022, scale vm1 from tierA to tierB at 7am.
     * 2. On Apr 26, 2022, scale vm1 from tierB to tierC at 10am.
     * 3. On Apr 26, 2022, power off vm1 at 1pm.
     * 4. On Apr 26, 2022, power on vm1 at 3pm.
     * 3. On Apr 27, 2022, revert the last scale action at 1pm.
     * Expect 2 bill records on Apr 25, for tierA and tierB respectively.
     * Expect 2 bill records on Apr 26, for tierB and tierC respectively.
     * Expect 2 bill records on Apr 27, for tierC and tierB respectively.
     * Expect 1 bill records on Apr 28, for tierB.
     */
    @Test
    public void testGenerateBillRecordsForRevert() {
        final double tierARate = 2.0;
        final double tierBRate = 1.0;
        final double tierCRate = 3.0;
        List<BillingScriptEvent> events = new ArrayList<>();

        events.add(createScaleEvent(getMillis(2022, 4, 25, 7, 0),
                vm1, tierARate, tierBRate, Collections.emptyList(), 0, "RESIZE"));
        events.add(createScaleEvent(getMillis(2022, 4, 26, 10, 0),
                vm1, tierBRate, tierCRate, Collections.emptyList(), 0, "RESIZE"));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 14, 0),
                vm1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 16, 0),
                vm1, true));
        events.add(createRevertEvent(getMillis(2022, 4, 27, 13, 0),
                vm1, "REVERT"));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 29, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());

        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createBillingRecord(scenarioStart, uuidMap.get(vm1), tierARate,
                7, 7 * 2);
        BillingRecord record2 = createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                17, 17);
        BillingRecord record3 = createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                10, 10);
        BillingRecord record4 = createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierCRate,
                12, 12 * 3);
        BillingRecord record5 = createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierCRate,
                13, 13 * 3);
        BillingRecord record6 = createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierBRate,
                11, 11);
        BillingRecord record7 = createBillingRecord(scenarioStart.plusDays(3), uuidMap.get(vm1), tierBRate,
                24, 24);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        expectedRecords.add(record5);
        expectedRecords.add(record6);
        expectedRecords.add(record7);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vm1))));
    }

    /**
     * Test Revert of volume scale action -- in this test, an ultra disk scale.
     */
    @Test
    public void testRevertVolumeScaleBillRecords() {
        // In this test source and destination tier are the same.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 500, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 2000, 0.7),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 800, 0.8),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 200, 0.6)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "ULTRA");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "ULTRA", "ULTRA", scaleCommodities, 0,
                "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 1, 0),
                vol1, true));
        events.add(createRevertEvent(getMillis(2022, 4, 26, 13, 0),
               vol1, "REVERT-VOL"));

        List<Commodity> scaleCommodities2 = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 3000, 0.9),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 2000, 1.5),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 500, 0.7)));
        events.add(createScaleVolumeEvent(getMillis(2022, 4, 27, 10, 0),
                vol1, "ULTRA", "ULTRA", scaleCommodities2, 0,
                "RESIZE-VOL"));
        events.add(createRevertEvent(getMillis(2022, 4, 27, 16, 0),
                vol1, "REVERT-VOL"));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioSecondDay = LocalDateTime.of(2022, 4, 26, 0, 0);
        LocalDateTime scenarioThirdDay = LocalDateTime.of(2022, 4, 27, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 28, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 1000 + 15 * 2000, 7 * 0.5 + 15 * 0.7, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 500 + 15 * 800, 7 * 0.6 + 15 * 0.8, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record3 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 100 + 15 * 200, 7 * 0.4 + 15 * 0.6,  CommodityType.IO_THROUGHPUT_VALUE);
        BillingRecord record4 = createVolumeBillingRecord(scenarioSecondDay, uuidMap.get(vol1), "ULTRA",
                12 * 2000 + 11 * 1000, 12 * 0.7 + 11 * 0.5, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record5 = createVolumeBillingRecord(scenarioSecondDay, uuidMap.get(vol1), "ULTRA",
                12 * 800 + 11 * 500, 12 * 0.8 + 11 * 0.6, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record6 = createVolumeBillingRecord(scenarioSecondDay, uuidMap.get(vol1), "ULTRA",
                12 * 200 + 11 * 100, 12 * 0.6 + 11 * 0.4,  CommodityType.IO_THROUGHPUT_VALUE);
        BillingRecord record7 = createVolumeBillingRecord(scenarioThirdDay, uuidMap.get(vol1), "ULTRA",
                10 * 1000 + 6 * 3000 + 8 * 1000, 10 * 0.5 + 6 * 0.9 + 8 * 0.5, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record8 = createVolumeBillingRecord(scenarioThirdDay, uuidMap.get(vol1), "ULTRA",
                10 * 500 + 6 * 2000 + 8 * 500, 10 * 0.6 + 6 * 1.5 + 8 * 0.6, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record9 = createVolumeBillingRecord(scenarioThirdDay, uuidMap.get(vol1), "ULTRA",
                10 * 100 + 6 * 500 + 8 * 100, 10 * 0.4 + 6 * 0.7 + 8 * 0.4,  CommodityType.IO_THROUGHPUT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        expectedRecords.add(record5);
        expectedRecords.add(record6);
        expectedRecords.add(record7);
        expectedRecords.add(record8);
        expectedRecords.add(record9);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test External Modification of volume scale action, -- in this test, an ultra disk scale.
     */
    @Test
    public void testExtModVolumeScaleBillRecords() {
        // In this test source and destination tier are the same.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 1000, 0.5),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 500, 0.6),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 100, 0.4)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 1000, 0.5, 2000, 0.7),
                new Commodity("STORAGE_AMOUNT", 500, 0.6, 800, 0.8),
                new Commodity("IO_THROUGHPUT", 100, 0.4, 200, 0.6)));
        BillingScriptEvent event1 = createVolumeEvent(getMillis(2022, 4, 25, 0, 0),
                vol1, initiaCommodities, "CREATE-VOL", "ULTRA");
        BillingScriptEvent event2 = createScaleVolumeEvent(getMillis(2022, 4, 25, 7, 0),
                vol1, "ULTRA", "ULTRA", scaleCommodities, 0,
                "RESIZE-VOL");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                vol1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 1, 0),
                vol1, true));
        List<Commodity> extModCommodities = new ArrayList<Commodity>(Arrays.asList(new Commodity("STORAGE_ACCESS", 2000, 0.7, 3000, 0.9),
                new Commodity("STORAGE_AMOUNT", 800, 0.8, 1000, 1.0),
                new Commodity("IO_THROUGHPUT", 200, 0.6, 700, 1.4)));
        events.add(createScaleVolumeEvent(getMillis(2022, 4, 26, 13, 0),
                vol1, "ULTRA", "ULTRA", extModCommodities, 0,
                "EXTMOD-VOL"));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioNextDay = LocalDateTime.of(2022, 4, 26, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 1000 + 15 * 2000, 7 * 0.5 + 15 * 0.7, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record2 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 500 + 15 * 800, 7 * 0.6 + 15 * 0.8, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record3 = createVolumeBillingRecord(scenarioStart, uuidMap.get(vol1), "ULTRA",
                7 * 100 + 15 * 200, 7 * 0.4 + 15 * 0.6,  CommodityType.IO_THROUGHPUT_VALUE);
        BillingRecord record4 = createVolumeBillingRecord(scenarioNextDay, uuidMap.get(vol1), "ULTRA",
                12 * 2000 + 11 * 3000, 12 * 0.7 + 11 * 0.9, CommodityType.STORAGE_ACCESS_VALUE);
        BillingRecord record5 = createVolumeBillingRecord(scenarioNextDay, uuidMap.get(vol1), "ULTRA",
                12 * 800 + 11 * 1000, 12 * 0.8 + 11 * 1.0, CommodityType.STORAGE_AMOUNT_VALUE);
        BillingRecord record6 = createVolumeBillingRecord(scenarioNextDay, uuidMap.get(vol1), "ULTRA",
                12 * 200 + 11 * 700, 12 * 0.6 + 11 * 1.4,  CommodityType.IO_THROUGHPUT_VALUE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        expectedRecords.add(record5);
        expectedRecords.add(record6);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(vol1))));
    }

    /**
     * Test creation of One Billing Record per tier, for DTU DB involving tier change only .(storage for tier not exceeded)
     */
    @Test
    public void testGenerateDtuDbBillRecordStorageNotExceeded() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        BillingScriptEvent event1 = createDbEvent(getMillis(2022, 4, 25, 0, 0),
                db1, Collections.emptyList(), "CREATE-DB", "BASICD", 0.0);
        BillingScriptEvent event2 = createScaleDbEvent(getMillis(2022, 4, 25, 7, 0),
                db1, "BASIC", "S0", 0.0, 0.0,
                Collections.emptyList(), 0, "RESIZE-DB");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                db1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        // For the purposes of simulation rate is a function of provider type in the scale event.  Reverse compute expected on-demand rates.
        final double expectedSrcOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("BASIC") / 10000;
        final double expectedDestOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("S0") / 10000;
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "BASIC",
                7.0, 7 * expectedSrcOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE);
        BillingRecord record2 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "S0",
                15.0, 15 * expectedDestOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(db1))));
    }

    /**
     * Test creation of Billing Records, for DTU DB involving tier change and excess storage.(storage for tier exceeded)
     */
    @Test
    public void testGenerateDtuDbBillRecordStorageExceeded() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 0, 0.0, 0, 0.0)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 0, 0.0, 16, 3.0)));
        BillingScriptEvent event1 = createDbEvent(getMillis(2022, 4, 25, 0, 0),
                db1, initiaCommodities, "CREATE-DB", "BASIC", 0.0);
        BillingScriptEvent event2 = createScaleDbEvent(getMillis(2022, 4, 25, 7, 0),
                db1, "BASIC", "S4",
                0, 0,
                scaleCommodities, 0, "RESIZE-DB");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                db1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        // For the purposes of simulation rate is a function of provider type in the scale event.  Reverse compute expected on-demand rates.
        final double expectedSrcOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("BASIC") / 10000;
        final double expectedDestOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("S4") / 10000;
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "BASIC",
                7, 7 * expectedSrcOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE);
        BillingRecord record2 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "S4",
                15, 15 * expectedDestOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE);
        BillingRecord record3 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 0 +  15 * 16, 7 * 0 +  15 * 3.0, CommodityType.STORAGE_AMOUNT_VALUE, CostCategory.STORAGE);

        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(db1))));
    }

    /**
     * Test creation of Billing Records, for DTU DB involving Premium tier (no change in tier) and excess storage.(storage for tier exceeded)
     * For example P2 --> P4
     */
    @Test
    public void testGenerateDtuDbPremiumTierBillRecordStorageExceeded() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 0, 0.0, 0, 0.0)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 0, 0.0, 24, 1.0)));
        BillingScriptEvent event1 = createDbEvent(getMillis(2022, 4, 25, 0, 0),
                db1, initiaCommodities, "CREATE-DB", "P6", 0.0);
        // P2 to S4 where storage amount is exceeded for example
        BillingScriptEvent event2 = createScaleDbEvent(getMillis(2022, 4, 25, 7, 0),
                db1, "P6", "P6",
                0, 0,
                scaleCommodities, 0, "RESIZE-DB");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                db1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        // src and destination on demand rates would be the same in this scenario.
        // For the purposes of simulation rate is a function of provider type in the scale event.  Reverse compute expected on-demand rates.
        final double expectedSrcAndDestOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("P6") / 10000;
        Set<BillingRecord> expectedRecords = new HashSet<>();
        BillingRecord record1 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "P6",
                7 + 15, (7 + 15) * expectedSrcAndDestOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE);
        BillingRecord record2 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 0 + 15 * 24, 7 * 0 + 15 * 1.0, CommodityType.STORAGE_AMOUNT_VALUE, CostCategory.STORAGE);
        expectedRecords.add(record1);
        expectedRecords.add(record2);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(db1))));
    }

    /**
     * Test creation of Billing Records, for VCORE GP DB involving tier change and storage amount change.
     * For example GP_Gen5_8 to GP_Gen5_4
     */
    @Test
    public void testGenerateVcoreGPDbBillRecord() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 72, 2.0, 72, 2.0)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 72, 2.0, 24, 1.8)));
        BillingScriptEvent event1 = createDbEvent(getMillis(2022, 4, 25, 0, 0),
                db1, initiaCommodities, "CREATE-DB", "GP_GEN5_8", 0.02);
        // S2 to S3 where storage amount is exceeded for example
        BillingScriptEvent event2 = createScaleDbEvent(getMillis(2022, 4, 25, 7, 0),
                db1, "GP_GEN5_8", "GP_GEN5_4",
                0.02, 0.01,
                scaleCommodities, 0, "RESIZE-DB");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                db1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 26, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();
        // For the purposes of simulation rate is a function of provider type in the scale event.  Reverse compute expected on-demand rates.
        final double expectedSrcOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("GP_GEN5_8") / 10000;
        final double expectedDestOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType("GP_GEN5_4") / 10000;
        BillingRecord record2 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "GP_GEN5_8",
                7.0, 7 * expectedSrcOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE);
        BillingRecord record1 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "GP_GEN5_4",
                15.0, 15 * expectedDestOnDemandRate, CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE);
        BillingRecord record3 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 72 + 15 * 24, 7 * 2.0 + 15 * 1.8, CommodityType.STORAGE_AMOUNT_VALUE, CostCategory.STORAGE);
        BillingRecord record4 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 8 + 15 * 4, 7 * 0.02 + 15 * 0.01, CommodityType.UNKNOWN_VALUE, CostCategory.LICENSE);
        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        assertTrue(CollectionUtils.isEqualCollection(expectedRecords, result.get(uuidMap.get(db1))));
    }

    /**
     * Test creation of Billing Records, for VCORE HP Hyperscale  DB involving tier change and storage amount change.
     * For example HS_Gen5_2 to HS_Gen5_4
     */
    @Test
    public void testGenerateVcoreHSDbBillRecord() {
        // In this test source and destination tier OID are the same, but commodities are different.
        List<BillingScriptEvent> events = new ArrayList<>();

        // Initial commodities for  the entity are the same at source and destination (no scaling has happened yet in this case).
        List<Commodity> initiaCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 72, 2.0, 72, 2.0)));
        List<Commodity> scaleCommodities = new ArrayList<Commodity>(Arrays.asList(
                new Commodity("STORAGE_AMOUNT", 72, 2.0, 24, 1.8)));
        BillingScriptEvent event1 = createDbEvent(getMillis(2022, 4, 25, 0, 0),
                db1, initiaCommodities, "CREATE-DB", "HS_GEN5_2", 0.02);
        // S2 to S3 where storage amount is exceeded for example
        BillingScriptEvent event2 = createScaleDbEvent(getMillis(2022, 4, 25, 7, 0),
                db1, "HS_GEN5_2", "HS_GEN5_4",
                0.02, 0.01,
                scaleCommodities, 0, "RESIZE-DB");
        events.add(event1);
        events.add(event2);
        events.add(createPowerEvent(getMillis(2022, 4, 25, 22, 0),
                db1, false));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 26, 0, 0);
        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        Assert.assertEquals(1, result.size());
        Set<BillingRecord> expectedRecords = new HashSet<>();

        // Assume total cost of compute 101.3 (7 * 4.4 + 15 * 4.7), rates divided up proportionally for test.
        // Where 4.4 and 4.7 are source and destination on-demand rates computed from provider type for purposes of simulation;
        BillingRecord record1 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "HS_GEN5_2",
                7 * 2, (101.3  * 7 * 2) / (7 * 2 + 15 * 4), CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE);
        BillingRecord record2 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "HS_GEN5_4",
                15 * 4, (101.3 * 15 * 4) / (7 * 2 + 15 * 4), CommodityType.UNKNOWN_VALUE, CostCategory.COMPUTE);
        BillingRecord record3 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 72 + 15 * 24, 7 * 2.0 + 15 * 1.8, CommodityType.STORAGE_AMOUNT_VALUE, CostCategory.STORAGE);
        BillingRecord record4 = createDbBillingRecord(scenarioStart, uuidMap.get(db1), "NONE",
                7 * 2 + 15 * 4, 7 * 0.02 + 15 * 0.01, CommodityType.UNKNOWN_VALUE, CostCategory.LICENSE);
        expectedRecords.add(record1);
        expectedRecords.add(record2);
        expectedRecords.add(record3);
        expectedRecords.add(record4);
        validateResults(expectedRecords, result.get(uuidMap.get(db1)));
    }

    /**
     * Scenario:
     * Execute a scale action that expects an RI coverage in the destination tier.
     * Expect 2 bill records to be generated after the action, one for the on-demand cost, and one
     * for the RI coverage.
     */
    @Test
    public void testRICoverageAfterAction() {
        final double tierARate = 2.0;
        final double tierBRate = 1.0;

        List<BillingScriptEvent> events = new ArrayList<>();
        events.add(createScaleEvent(getMillis(2022, 4, 25, 8, 0),
                vm1, tierARate, tierBRate, Collections.emptyList(), 0.8,
                "RESIZE"));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 27, 0, 0);

        Set<BillingRecord> expectedRecords = new HashSet<>();
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierARate,
                8, 8 * 2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                16 * 0.8, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                16 * 0.2, 16 * 0.2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                24 * 0.8, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                24 * 0.2, 24 * 0.2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));

        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        validateResults(expectedRecords, result.get(uuidMap.get(vm1)));
    }

    /**
     * Scenario:
     *  1. On Apr 25, 2022, scale vm1 from tierA to tierB at 7am. Expect no RI coverage after action.
     *  2. On Apr 27, 2022 10am, vm1 become fully covered by RI.
     *
     *  <p>Expected result:
     *  1. On Apr 25, expect one record before action and one after action, both are on-demand.
     *  2. On Apr 26, full day running on cost of tierB.
     *  3. On Apr 27, expect two records - one for on-demand and one for reserved. The record for
     *     the reserved portion should have usageAmount 14 hours. On-demand record should have usage
     *     amount of 10 hours.
     *  4. On Apr 28, full day running on reserved. Cost is $0.
     */
    @Test
    public void testRICoverageChange() {
        final double tierARate = 2.0;
        final double tierBRate = 1.0;

        List<BillingScriptEvent> events = new ArrayList<>();
        events.add(createScaleEvent(getMillis(2022, 4, 25, 7, 0),
                vm1, tierARate, tierBRate, Collections.emptyList(), 0,
                "RESIZE"));
        events.add(createRICoverageEvent(getMillis(2022, 4, 27, 10, 0),
                vm1, 1));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 29, 0, 0);

        Set<BillingRecord> expectedRecords = new HashSet<>();
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierARate,
                7, 7 * 2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                17, 17, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                24, 24, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierBRate,
                14, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierBRate,
                10, 10, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(3), uuidMap.get(vm1), tierBRate,
                24, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));

        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        validateResults(expectedRecords, result.get(uuidMap.get(vm1)));
    }

    /**
     * Scenario:
     *  1. On Apr 25, 2022, scale vm1 from tierA to tierB at 7am. Expect RI coverage of 0.8 after action.
     *  2. On Apr 26, 2022, Turn off vm1 for 2 hours.
     *  2. On Apr 27, 2022 10am, RI coverage becomes 0.
     *
     *  <p>Expected result:
     *  1. On Apr 25, expect one record (on-demand) before action and two (on-demand + reserved) after action.
     *  2. On Apr 26, 22 * 0.2 = 4.4 hours running on-demand. 22 * 0.8 = 17.6 hours running on RI ($0).
     *  3. On Apr 27, Before RI changed to 0% at 10am:
     *                a) reserved = 10 * 0.8 = 8hours ($0);
     *                b) on-demand = 10 * 0.2 = 2 hours ($2)
     *                After RI changed to 0% at 10am, on-demand = 14 hours ($14)
     *                Total on-demand = 2 + 14 = 16 (in one record)
     *  4. On Apr 28, full day running on-demand. Cost is $24.
     */
    @Test
    public void testRICoverageChangeToZero() {
        final double tierARate = 2.0;
        final double tierBRate = 1.0;

        List<BillingScriptEvent> events = new ArrayList<>();
        events.add(createScaleEvent(getMillis(2022, 4, 25, 7, 0),
                vm1, tierARate, tierBRate, Collections.emptyList(), 0.8,
                "RESIZE"));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 14, 0),
                vm1, false));
        events.add(createPowerEvent(getMillis(2022, 4, 26, 16, 0),
                vm1, true));
        events.add(createRICoverageEvent(getMillis(2022, 4, 27, 10, 0),
                vm1, 0));

        LocalDateTime scenarioStart = LocalDateTime.of(2022, 4, 25, 0, 0);
        LocalDateTime scenarioEnd = LocalDateTime.of(2022, 4, 29, 0, 0);

        Set<BillingRecord> expectedRecords = new HashSet<>();
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierARate,
                7, 7 * 2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                17 * 0.2, 17 * 0.2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart, uuidMap.get(vm1), tierBRate,
                17 * 0.8, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                22 * 0.2, 22 * 0.2, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(1), uuidMap.get(vm1), tierBRate,
                22 * 0.8, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierBRate,
                10 * 0.8, 0, PriceModel.RESERVED, CostCategory.COMMITMENT_USAGE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(2), uuidMap.get(vm1), tierBRate,
                16, 16, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));
        expectedRecords.add(createBillingRecord(scenarioStart.plusDays(3), uuidMap.get(vm1), tierBRate,
                24, 24, PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE));

        Map<Long, Set<BillingRecord>> result =
                ScenarioGenerator.generateBillRecords(events, uuidMap, scenarioStart, scenarioEnd);
        validateResults(expectedRecords, result.get(uuidMap.get(vm1)));
    }

    private BillingScriptEvent createVolumeEvent(long timestamp, String uuid, List<Commodity> initialCommodities,
                                                 String createType, String initialVolumeType) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = createType;
        event.sourceType = initialVolumeType;
        event.uuid = uuid;
        event.commodities = new ArrayList<>();
        if (initialCommodities != null) {
            event.commodities.addAll(initialCommodities);
        }
        event.expectedCloudCommitment = 0;
        return event;
    }

    private BillingScriptEvent createDbEvent(long timestamp, String uuid, List<Commodity> initialCommodities,
                                                 String createType, String initialDbType, double sourceLicenseRate) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = createType;
        event.sourceType = initialDbType;
        event.sourceNumVCores = ScenarioGenerator.getNumVcores(initialDbType);
        event.sourceLicenseRate = sourceLicenseRate;
        event.uuid = uuid;
        event.commodities = new ArrayList<>();
        if (initialCommodities != null) {
            event.commodities.addAll(initialCommodities);
        }
        event.expectedCloudCommitment = 0;
        return event;
    }

    private BillingScriptEvent createScaleEvent(long timestamp, String uuid, double sourceOnDemandRate,
            double destinationOnDemandRate, List<Commodity> scaleCommodities,
                                                double expectedRICoverage, String scaleType) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = scaleType;
        event.uuid = uuid;
        event.sourceOnDemandRate = sourceOnDemandRate;
        event.destinationOnDemandRate = destinationOnDemandRate;
        event.commodities = new ArrayList<>();
        if (scaleCommodities != null) {
            event.commodities.addAll(scaleCommodities);
        }
        event.expectedCloudCommitment = expectedRICoverage;
        return event;
    }

    private BillingScriptEvent createScaleVolumeEvent(long timestamp, String uuid, String sourceVolumeType, String destinationVolumeType, List<Commodity> scaleCommodities,
                                                double expectedRICoverage, String scaleType) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = scaleType;
        event.uuid = uuid;
        event.sourceType = sourceVolumeType;
        event.destinationType = destinationVolumeType;
        event.commodities = new ArrayList<>();
        if (scaleCommodities != null) {
            event.commodities.addAll(scaleCommodities);
        }
        event.expectedCloudCommitment = expectedRICoverage;
        return event;
    }

    private BillingScriptEvent createScaleDbEvent(long timestamp, String uuid, String sourceDbType, String destinationDbType,
                                                  double sourceLicenseRate, double destinationLicenseRate,
                                                  List<Commodity> scaleCommodities,
                                                      double expectedRICoverage, String scaleType) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = scaleType;
        event.uuid = uuid;
        event.sourceType = sourceDbType;
        event.destinationType = destinationDbType;
        event.sourceOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType(sourceDbType) / 10000;
        event.destinationOnDemandRate = (double)ScenarioGenerator.generateProviderIdFromType(destinationDbType) / 10000;
        event.sourceNumVCores = ScenarioGenerator.getNumVcores(sourceDbType);
        event.destinationNumVCores = ScenarioGenerator.getNumVcores(destinationDbType);
        event.sourceLicenseRate = sourceLicenseRate;
        event.destinationLicenseRate = destinationLicenseRate;
        event.commodities = new ArrayList<>();
        if (scaleCommodities != null) {
            event.commodities.addAll(scaleCommodities);
        }
        event.expectedCloudCommitment = expectedRICoverage;
        return event;
    }

    private BillingScriptEvent createRICoverageEvent(long timestamp, String uuid, double expectedRICoverage) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = "RI_COVERAGE";
        event.uuid = uuid;
        event.expectedCloudCommitment = expectedRICoverage;
        return event;
    }

    private BillingScriptEvent createPowerEvent(long timestamp, String uuid, boolean state) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = "POWER_STATE";
        event.uuid = uuid;
        event.state = state;
        return event;
    }

    private BillingScriptEvent createRevertEvent(long timestamp, String uuid, String revertType) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = revertType;
        event.uuid = uuid;
        return event;
    }

    private BillingRecord createBillingRecord(LocalDateTime date, long entityId, double tierRate,
            double usageAmount, double cost) {
        return createBillingRecord(date, entityId, tierRate, usageAmount, cost,
                PriceModel.ON_DEMAND, CostCategory.COMPUTE_LICENSE_BUNDLE);
    }

    private BillingRecord createBillingRecord(LocalDateTime date, long entityId, double tierRate,
            double usageAmount, double cost, PriceModel priceModel, CostCategory costCategory) {
        return new BillingRecord.Builder()
                .sampleTime(date)
                .entityId(entityId)
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .accountId(1L)
                .regionId(2L)
                .priceModel(priceModel)
                .costCategory(costCategory)
                .providerId(ScenarioGenerator.generateProviderIdFromRate(tierRate))
                .providerType(EntityType.COMPUTE_TIER_VALUE)
                .commodityType(CommodityType.UNKNOWN_VALUE)
                .usageAmount(usageAmount)
                .cost(cost)
                .serviceProviderId(100L)
                .build();
    }

    private BillingRecord createVolumeBillingRecord(LocalDateTime date, long entityId, String tierName,
                                                    double usageAmount, double cost, int commTypeValue) {
        return createVolumeBillingRecord(date, entityId, tierName, usageAmount, cost, commTypeValue,
                PriceModel.ON_DEMAND, CostCategory.STORAGE);
    }

    private BillingRecord createVolumeBillingRecord(LocalDateTime date, long entityId, String tierName,
                                              double usageAmount, double cost, int commTypeValue, PriceModel priceModel,
                                                    CostCategory costCategory) {
        return new BillingRecord.Builder()
                .sampleTime(date)
                .entityId(entityId)
                .entityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .priceModel(priceModel)
                .costCategory(costCategory)
                .providerId(ScenarioGenerator.generateProviderIdFromType(tierName))
                .providerType(EntityType.STORAGE_TIER_VALUE)
                .commodityType(commTypeValue)
                .usageAmount(usageAmount)
                .cost(cost)
                .regionId(2L)
                .accountId(1L)
                .serviceProviderId(100L)
                .build();
    }

    private BillingRecord createDbBillingRecord(LocalDateTime date, long entityId, String tierName,
                                                double usageAmount, double cost, int commTypeValue,
                                                CostCategory costCategory) {
        return createDbBillingRecord(date, entityId, tierName, usageAmount, cost, commTypeValue,
                PriceModel.ON_DEMAND, costCategory);
    }

    private BillingRecord createDbBillingRecord(LocalDateTime date, long entityId, String tierName,
                                                double usageAmount, double cost, int commTypeValue, PriceModel priceModel,
                                                CostCategory costCategory) {
        return new BillingRecord.Builder()
                .sampleTime(date)
                .entityId(entityId)
                .entityType(EntityType.DATABASE_VALUE)
                .priceModel(priceModel)
                .costCategory(costCategory)
                .providerId(ScenarioGenerator.generateProviderIdFromType(tierName))
                .providerType(EntityType.DATABASE_TIER_VALUE)
                .commodityType(commTypeValue)
                .usageAmount(usageAmount)
                .cost(cost)
                .regionId(2L)
                .accountId(1L)
                .serviceProviderId(100L)
                .build();
    }

    private void validateResults(Set<BillingRecord> expectedResults, Set<BillingRecord> results) {
        Assert.assertEquals(expectedResults.size(), results.size());
        Assert.assertEquals(expectedResults.stream().map(BillingRecord::getSampleTime).collect(
                        Collectors.toSet()),
                results.stream().map(BillingRecord::getSampleTime).collect(Collectors.toSet()));
        Map<BillRecordKey, BillingRecord> resultsMap =
                results.stream().collect(Collectors.toMap(this::getBillRecordKey, Function.identity()));
        expectedResults.forEach(r -> {
            BillingRecord record = resultsMap.get(getBillRecordKey(r));
            assertEquals(r.getPriceModel(), record.getPriceModel());
            assertEquals(r.getCost(), record.getCost(), 0.001);
            assertEquals(r.getUsageAmount(), record.getUsageAmount(), 0.001);
            assertEquals(r.getCostCategory(), record.getCostCategory());
        });
    }

    private BillRecordKey getBillRecordKey(BillingRecord record) {
        return new BillRecordKey(record.getProviderId(), record.getSampleTime(), record.getPriceModel(),
                record.getCostCategory());
    }

    static class BillRecordKey {
        long providerId;
        LocalDateTime sampleTime;
        PriceModel priceModel;
        CostCategory costCategory;

        BillRecordKey(long providerId, LocalDateTime sampleTime, PriceModel priceModel, CostCategory costCategory) {
            this.providerId = providerId;
            this.sampleTime = sampleTime;
            this.priceModel = priceModel;
            this.costCategory = costCategory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BillRecordKey that = (BillRecordKey)o;
            return providerId == that.providerId && sampleTime.equals(that.sampleTime)
                    && priceModel == that.priceModel && costCategory == that.costCategory;
        }

        @Override
        public int hashCode() {
            return Objects.hash(providerId, sampleTime, priceModel, costCategory);
        }
    }
}
