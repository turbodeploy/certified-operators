package com.vmturbo.cost.component.savings.calculator;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingRecord;
import com.vmturbo.cost.component.savings.GrpcActionChainStore;
import com.vmturbo.cost.component.savings.ScenarioGenerator;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * Test cases for bill-based savings calculator.
 */
public class CalculatorTest {

    private final long vmOid = 1234L;
    private final long volumeOid = 5555L;
    private final Calculator calculator;
    private final Clock clock = Clock.systemUTC();
    private final long lastProcessedDate = getTimestamp(date(2022, 3, 24));
    private final Comparator<ExecutedActionsChangeWindow> changeWindowComparator =
            GrpcActionChainStore.changeWindowComparator;

    public CalculatorTest() {
        long volumeExpiryMs = 10; // dummy value
        this.calculator = new Calculator(volumeExpiryMs, clock);
    }

    /**
     * Scenario: Calculate savings for a VM on the day when it had its first action.
     * Entity type: VM
     * Bill records for 2022-03-25.
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $1/hr.
     * Cost of VM on destination tier on the bill is $36 for this day.
     */
    @Test
    public void investmentsOnDayOfAction() {
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 1, targetProviderId));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(18)
                .timestamp(date(2022, 3, 25)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate investments for a VM on the day when it had its first action.
     * Entity type: VM
     * Bill records for 2022-03-25.
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $3/hr.
     * Cost of VM on destination tier on the bill is $36 for this day.
     */
    @Test
    public void savingsOnDayOfAction() {
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(18).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate savings for a VM on the day after it had its first action.
     * Entity type: VM
     * Bill records for 2022-03-26.
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $3/hr.
     * Cost of VM on 2022-03-26 is $48. (uptime = 100%)
     */
    @Test
    public void savingsOnDayAfterAction() {
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate savings for a VM on a day when it has partial uptime.
     * Entity type: VM
     * Bill records for 2022-03-26.
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $3/hr.
     * Cost of VM on 2022-03-26 is $24. (uptime = 50%)
     */
    @Test
    public void savingsOnDayPartialUptime() {
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 26), 12, 24, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(12).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate savings for a VM for two days (i.e. the set of bill records has records
     * for two days of costs.)
     * Entity type: VM
     * Bill records for 2022-03-25 and 2022-03-26.
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $3/hr.
     * Cost of VM on 2022-03-25 is $36.
     * Cost of VM on 2022-03-26 is $24.
     */
    @Test
    public void savingsTwoDays() {
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 12, 24, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(18).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(12).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Scenario: Calculate investment for a VM that had an action executed on the day before the bill
     * record date and also had an executed action on the day of the bill record.
     * Entity type: VM
     * Bill records for 2022-03-25.
     * Scale action executed on 2022-03-24T12:00, cost went from $1/hr to @$2/hr.
     * Scale action executed on 2022-03-25T06:00, cost went from $2/hr to @$3/hr.
     * Bill record for 2022-03-25 shows two records for the VM compute.
     * 1. first 6 hours: $2/hr
     * 2. rest of the day: $3/hr
     * Investments: 1 * 6 + 2 * 18 = 42
     */
    @Test
    public void twoSegmentsInADayInvestments() {
        final long targetProviderId1 = 2323232323L;
        final long targetProviderId2 = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 12, targetProviderId1));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 54, targetProviderId2));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 24, 12, 0), 1, targetProviderId1));
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderId2));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(42)
                .timestamp(date(2022, 3, 25)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate savings for a VM that had an action executed on the day before the bill
     * record date and also had an executed action on the day of the bill record.
     * Entity type: VM
     * Bill records for 2022-03-25.
     * Scale action executed on 2022-03-24T12:00, cost went from $3/hr to @$2/hr.
     * Scale action executed on 2022-03-25T06:00, cost went from $2/hr to @$1/hr.
     * Bill record for 2022-03-25 shows two records for the VM compute.
     * 1. first 6 hours: $2/hr
     * 2. rest of the day: $3/hr
     * Savings: 1 * 6 + 2 * 18 = 42
     */
    @Test
    public void twoSegmentsInADaySavings() {
        final long targetProviderId1 = 2323232323L;
        final long targetProviderId2 = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 12, targetProviderId1));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 18, targetProviderId2));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 24, 12, 0), 3, targetProviderId1));
        actionSpecs.add(
                createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderId2));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(42).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Scenario: Calculate savings and investment for a VM that ran on the same service tier in two
     * separate segments of a day.
     * Entity type: VM
     * Bill records for 2022-03-25.
     * Scale action executed on 2022-03-24T12:00, (A -> B), cost went from $1/hr to @$2/hr.
     * Scale action executed on 2022-03-25T06:00, (B -> C), cost went from $2/hr to @$3/hr.
     * Scale action executed on 2022-03-25T15:00, (C -> B), cost went from $3/hr to @$2/hr.
     * Bill record for 2022-03-25 shows two records for the VM compute.
     * 1. 15 hours on tier B: $2/hr (00:00 to 06:00, 15:00 to 24:00)
     * 2. 9 hours on tier C: $3/hr (06:00 to 15:00)
     * First 6 hours, invested $6, saved $0.
     * Next 9 hours, invested $2 * 9 = $18, saved $0.
     * Next 9 hours, invested $1 * 9 = $9, saved $1 * 9 = $9.
     * Total investments = 6 + 18 + 9 = $33
     * Total savings = $9
     */
    @Test
    public void scaleBackToOriginalTierOnSameDay() {
        final long targetProviderTierB = 2323232323L;
        final long targetProviderTierC = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 9, 27, targetProviderTierC));
        records.add(createVMBillRecord(date(2022, 3, 25), 15, 30, targetProviderTierB));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 24, 12, 0), 1, targetProviderTierB));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderTierC));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 25, 15, 0), 3, targetProviderTierB));
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(9).investments(33)
                .timestamp(date(2022, 3, 25)).build());
        Assert.assertEquals(expectedResults, result);
    }

    /**
     * Test watermark values.
     * 5 actions with source costs: 2,6,3,4,1
     * Test high and low watermarks before and after each action.
     * Then remove the first action to simulate the use case of expiring the first action.
     * Test the high and low watermarks before and after action again.
     */
    @Test
    public void testWatermarks() {
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 20, 12, 0), 2, 10L));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 21, 12, 0), 6, 20L));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 22, 12, 0), 3, 30L));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 23, 12, 0), 4, 40L));
        actionSpecs.add(createVMActionChangeWindow(LocalDateTime.of(2022, 3, 24, 12, 0), 1, 50L));
        SavingsGraph graph = new SavingsGraph(actionSpecs, TimeUnit.DAYS.toMillis(365));

        LocalDateTime datetime = date(2022, 3, 19, 13, 0);

        // Return null if timestamp is before the first action.
        ScaleActionDataPoint watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNull(watermark);

        datetime = date(2022, 3, 20, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(2, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 21, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 22, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 23, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 24, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(1, watermark.getLowWatermark(), 0);

        // Remove the first action.
        actionSpecs.remove(actionSpecs.first());
        graph = new SavingsGraph(actionSpecs, TimeUnit.DAYS.toMillis(365));

        datetime = date(2022, 3, 21, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(6, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 22, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(3, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 23, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(3, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 24, 13, 0);
        watermark = (ScaleActionDataPoint)graph.getDataPoint(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(1, watermark.getLowWatermark(), 0);
    }

    private LocalDateTime date(int year, int month, int day) {
        return LocalDateTime.of(year, month, day, 0, 0);
    }

    private LocalDateTime date(int year, int month, int day, int hour, int min) {
        return LocalDateTime.of(year, month, day, hour, min);
    }

    private long getTimestamp(LocalDateTime localDateTime) {
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private BillingRecord createVMBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId) {
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(vmOid)
                .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                .providerType(EntityType.COMPUTE_TIER.getValue())
                .costCategory(CostCategory.COMPUTE)
                .priceModel(PriceModel.ON_DEMAND)
                .build();
    }

    private BillingRecord createVolumeBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId) {
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(volumeOid)
                .entityType(EntityType.VIRTUAL_VOLUME.getValue())
                .providerType(EntityType.STORAGE_TIER.getValue())
                .costCategory(CostCategory.STORAGE)
                .priceModel(PriceModel.ON_DEMAND)
                .build();
    }

    private ExecutedActionsChangeWindow createVMActionChangeWindow(LocalDateTime actionTime, double sourceOnDemandRate, long destProviderId) {
        double dummyDestOnDemandRate = 2;
        long dummySourceProviderId = 22222222;
        return ScenarioGenerator.createVMActionChangeWindow(vmOid, actionTime, sourceOnDemandRate, dummyDestOnDemandRate, dummySourceProviderId,
                destProviderId, null, LivenessState.LIVE);
    }

    /**
     * Test various use cases for delete actions.
     * Deleted volume on 2022-03-25 6AM. The volume had no actions before this date.
     * Volume costs $0.05 per hour.
     * Vary the following parameters:
     * 1. current time
     * 2. last processed date
     * 3. volume delete action expiry date
     */
    @Test
    public void testDelete() {
        // Current date/time is 2022-03-26T09:00AM.
        // Last processed date is 2022-03-24.
        // Expected result:
        // 1. Savings on 2022-03-25 is 0.05 * 18 = 0.9
        List<SavingsValues> result = runDeleteTest(date(2022, 3, 26, 9, 0),
                date(2022, 3, 24), 365);
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0.9).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        validateResults(result, expectedResults);

        // Current date/time is 2022-03-27T09:00AM.
        // Last processed date is 2022-03-24.
        // Expected result:
        // 1. Savings on 2022-03-25 is 0.05 * 18 = 0.9
        // 2. Full day of savings on 2022-03-26 of 0.05 * 24 = 1.2
        result = runDeleteTest(date(2022, 3, 27, 9, 0),
                date(2022, 3, 24), 365);
        expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0.9).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        validateResults(result, expectedResults);

        // Current date/time is 2022-03-28T09:00AM.
        // Last processed date is 2022-03-24.
        // Expected result:
        // 1. Savings on 2022-03-25 is 0.05 * 18 = 0.9
        // 2. Full day of savings on 2022-03-26 and 2022-03-27 of 0.05 * 24 = 1.2
        result = runDeleteTest(date(2022, 3, 28, 9, 0),
                date(2022, 3, 24), 365);
        expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0.9).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 27)).build());
        validateResults(result, expectedResults);

        // Current date/time is 2022-03-28T09:00AM.
        // Last processed date is 2022-03-25.
        // Expected result:
        // 1. Full day of savings on 2022-03-26 and 2022-03-27 of 0.05 * 24 = 1.2
        result = runDeleteTest(date(2022, 3, 28, 9, 0),
                date(2022, 3, 25), 365);
        expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 27)).build());
        validateResults(result, expectedResults);

        // Current date/time is 2022-03-28T09:00AM.
        // Last processed date is 2022-03-26.
        // Expected result:
        // 1. Full day of savings on 2022-03-27 of 0.05 * 24 = 1.2
        result = runDeleteTest(date(2022, 3, 28, 9, 0),
                date(2022, 3, 26), 365);
        expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 27)).build());
        validateResults(expectedResults, result);

        // Current date/time is 2022-03-28T09:00AM.
        // Last processed date is 2022-03-24.
        // Set volume action to expire in 2 day.
        // Expected result:
        // 1. Savings on 2022-03-25 is 0.05 * 18 = 0.9
        // 2. Full day of savings on 2022-03-26 of 0.05 * 24 = 1.2
        // 3. 6 hours of savings on 2022-03-27 of 0.05 * 6 = 0.3.  (6 hours because action expired at 6am on this day)
        //    No savings on 2022-03-27 because action expired.
        result = runDeleteTest(date(2022, 3, 28, 9, 0),
                date(2022, 3, 24), 2);
        expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0.9).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(1.2).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0.3).investments(0)
                .timestamp(date(2022, 3, 27)).build());
        validateResults(result, expectedResults);
    }

    /**
     * Create delete action on 2022-03-25.
     * Create bill record for the day of 2022-03-25. (the cost before the volume was deleted)
     *
     * @param referenceTime The day when the calculation was done.
     * @param lastProcessed The day when calculation was last performed.
     * @param volumeExpiryDays number of days for a delete action to expire
     * @return list of savings values
     */
    private List<SavingsValues> runDeleteTest(LocalDateTime referenceTime, LocalDateTime lastProcessed, int volumeExpiryDays) {
        long lastProcessedInMillis = TimeUtil.localTimeToMillis(lastProcessed, clock);
        long volumeExpiryMs = TimeUnit.DAYS.toMillis(volumeExpiryDays);
        Calculator calculator = new Calculator(volumeExpiryMs, clock);
        final long storageTierOid = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        double storageCostPerHour = 0.05;
        if (lastProcessed.isBefore(date(2022, 3, 25))) {
            records.add(createVolumeBillRecord(date(2022, 3, 25), 6, storageCostPerHour * 6, storageTierOid));
        }
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeDeleteActionSpec(volumeOid, LocalDateTime.of(2022, 3, 25, 6, 0),
                0.05, storageTierOid));
        return calculator.calculate(volumeOid, records, actionSpecs, lastProcessedInMillis, referenceTime);
    }

    private void validateResults(List<SavingsValues> results, List<SavingsValues> expectedResults) {
        Assert.assertEquals(expectedResults.size(), results.size());
        Assert.assertEquals(expectedResults.stream().map(SavingsValues::getTimestamp).collect(Collectors.toList()),
                results.stream().map(SavingsValues::getTimestamp).collect(Collectors.toList()));
        Map<LocalDateTime, SavingsValues> resultsMap =
                results.stream().collect(Collectors.toMap(SavingsValues::getTimestamp, Function.identity()));
        expectedResults.forEach(r -> {
            SavingsValues savingsValues = resultsMap.get(r.getTimestamp());
            Assert.assertEquals(r.getSavings(), savingsValues.getSavings(), 0.0001);
            Assert.assertEquals(r.getInvestments(), savingsValues.getInvestments(), 0.0001);
        });
    }

    /**
     * Scenario:
     * Entity type: VM
     * Bill records for 2022-03-25 and 2022-03-26.
     * Scale action executed on 2022-03-25T06:00.
     * VM was reverted to original tier at 2022-03-27T09:00.
     * Original cost of VM: $3/hr.
     * Destination cost of scale action is $2/hr.
     * 2022-03-25: VM ran on destination tier for 18 hours. cost = 2*18 = 36.
     *             Cost on source tier: 3 * 6 = 18
     *             Savings = 1 * 18 = 18
     * 2022-03-26: VM ran on destination tier for full day. cost = 2 * 24 = 48.
     * 2022-03-27: VM reverted to original tier at 9am.
     *             Cost on destination tier: 2 * 9 = 18
     *             Cost on source tier: 3 * 15 = 45
     *             Savings = 1 * 9 = 9
     */
    @Test
    public void testRevert() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 18, sourceProviderId));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 9, 18, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 15, 45, sourceProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        ScenarioGenerator.createVMActionChangeWindow(vmOid, LocalDateTime.of(2022, 3, 25, 6, 0),
                3, 2, sourceProviderId,
                targetProviderId, LocalDateTime.of(2022, 3, 27, 9, 0), LivenessState.REVERTED);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 3, 25, 6, 0),
                3, 2, sourceProviderId, targetProviderId,
                LocalDateTime.of(2022, 3, 27, 9, 0),
                LivenessState.REVERTED));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(18).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(9).investments(0)
                .timestamp(date(2022, 3, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }
}
