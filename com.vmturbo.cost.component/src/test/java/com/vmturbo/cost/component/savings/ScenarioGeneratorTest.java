package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingDataInjector.BillingScriptEvent;
import com.vmturbo.cost.component.savings.ScenarioGenerator.Interval;
import com.vmturbo.cost.component.savings.ScenarioGenerator.Segment;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

public class ScenarioGeneratorTest {

    private static final String vm1 = "vm1";

    private static final Map<String, Long> uuidMap = ImmutableMap.of(vm1, 70000L);

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
        Segment s1 = new Segment(start, end, 2, 1000L);
        List<Segment> segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(end, segments.get(0).end);

        start = getMillis(2022, 4, 10, 7, 0);
        end = getMillis(2022, 4, 10, 11, 0);
        s1 = new Segment(start, end, 2, 1000L);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(powerOffTimeMillis, segments.get(0).end);

        start = getMillis(2022, 4, 10, 7, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(2, segments.size());
        Assert.assertEquals(start, segments.get(0).start);
        Assert.assertEquals(powerOffTimeMillis, segments.get(0).end);
        Assert.assertEquals(powerOnTimeMillis, segments.get(1).start);
        Assert.assertEquals(end, segments.get(1).end);

        start = getMillis(2022, 4, 10, 13, 0);
        end = getMillis(2022, 4, 10, 14, 0);
        s1 = new Segment(start, end, 2, 1000L);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(0, segments.size());

        start = getMillis(2022, 4, 10, 13, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L);
        segments = s1.exclude(powerOffInterval);
        Assert.assertEquals(1, segments.size());
        Assert.assertEquals(powerOnTimeMillis, segments.get(0).start);
        Assert.assertEquals(end, segments.get(0).end);

        start = getMillis(2022, 4, 10, 21, 0);
        end = getMillis(2022, 4, 10, 22, 0);
        s1 = new Segment(start, end, 2, 1000L);
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
                vm1, sourceTierRate, destinationTierRate));
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
     * Test the generation of bill records from script events. Both scale and power events are tested.
     * Scenario:
     * 1. On Apr 25, 2022, scale vm1 from tierA to tierB at 7am.
     * 2. On Apr 26, 2022, scale vm1 from tierB to tierC at 10am.
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
                vm1, tierARate, tierBRate));
        events.add(createScaleEvent(getMillis(2022, 4, 26, 10, 0),
                vm1, tierBRate, tierCRate));
        events.add(createRevertEvent(getMillis(2022, 4, 27, 13, 0),
                vm1));

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
                14, 14 * 3);
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

    private BillingScriptEvent createScaleEvent(long timestamp, String uuid, double sourceOnDemandRate,
            double destinationOnDemandRate) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = "RESIZE";
        event.uuid = uuid;
        event.sourceOnDemandRate = sourceOnDemandRate;
        event.destinationOnDemandRate = destinationOnDemandRate;
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

    private BillingScriptEvent createRevertEvent(long timestamp, String uuid) {
        BillingScriptEvent event = new BillingScriptEvent();
        event.timestamp = timestamp;
        event.eventType = "REVERT";
        event.uuid = uuid;
        return event;
    }

    private BillingRecord createBillingRecord(LocalDateTime date, long entityId, double tierRate,
            double usageAmount, double cost) {
        return new BillingRecord.Builder()
                .sampleTime(date)
                .entityId(entityId)
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .priceModel(PriceModel.ON_DEMAND)
                .costCategory(CostCategory.COMPUTE)
                .providerId(ScenarioGenerator.generateProviderIdFromRate(tierRate))
                .providerType(EntityType.COMPUTE_TIER_VALUE)
                .usageAmount(usageAmount)
                .cost(cost)
                .build();
    }
}
