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
        return createVMBillRecord(dateTime, usageAmount, cost, providerId, PriceModel.ON_DEMAND);
    }

    private BillingRecord createVMBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId, PriceModel priceModel) {
        CostCategory costCategory = priceModel == PriceModel.ON_DEMAND
                ? CostCategory.COMPUTE_LICENSE_BUNDLE : CostCategory.COMMITMENT_USAGE;
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(vmOid)
                .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                .providerType(EntityType.COMPUTE_TIER.getValue())
                .costCategory(costCategory)
                .priceModel(priceModel)
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
                destProviderId, null, LivenessState.LIVE, 0);
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
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 18, sourceProviderId));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 9, 18, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 15, 45, sourceProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 3, 25, 6, 0),
                3, 2, sourceProviderId, targetProviderId,
                LocalDateTime.of(2022, 3, 27, 9, 0),
                LivenessState.REVERTED, 0));

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

    /**
     * Scenario:
     * Entity type: VM
     * Bill records for 2022-03-25 and 2022-03-26.
     * Scale action executed on 2022-03-25T06:00.
     * VM was scaled to an unsolicited service tier at 2022-03-27T09:00.
     * Original cost of VM: $3/hr.
     * Destination cost of scale action is $2/hr.
     * 2022-03-25: VM ran on destination tier for 18 hours. cost = 2*18 = 36.
     *             Cost on source tier: 3 * 6 = 18
     *             Savings = 1 * 18 = 18
     * 2022-03-26: VM ran on destination tier for full day. cost = 2 * 24 = 48.
     * 2022-03-27: VM scaled to an unsolicited service tier at 9am.
     *             Cost on destination tier: 2 * 9 = 18
     *             Cost on source tier: 3 * 15 = 45
     *             Savings = 1 * 9 = 9
     */
    @Test
    public void testExternalModification() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        final long unsolicitedTier = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 18, sourceProviderId));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 9, 18, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 15, 45, unsolicitedTier));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 3, 25, 6, 0),
                3, 2, sourceProviderId, targetProviderId,
                LocalDateTime.of(2022, 3, 27, 9, 0),
                LivenessState.EXTERNAL_MODIFICATION, 0));

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

    /**
     * Scenario:
     * Entity type: VM
     * Scale action executed on 2022-03-25T06:00.
     * Original cost of VM: $3/hr.
     * Destination cost of scale action is $2/hr.
     * Bill records:
     * 2022-03-25: VM ran on destination tier for 18 hours. cost = 2*18 = 36.
     *             Cost on source tier: 3 * 6 = 18
     *             Savings = 1 * 18 = 18
     * 2022-03-26: VM ran on destination tier for full day. cost should be 2 * 24 = 48, but the bill
     *             shows 30. Savings = 3 * 24 - 30 = 42
     * 2022-03-27: VM ran on destination tier for full day. cost = 2 * 24 = 48. Savings = 3 * 24 - 48 = 24
     * Later, the cost for 3-26 is updated to 48. savings should be recalculated to $24 for the day.
     */
    @Test
    public void testBillCostUpdate() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 18, sourceProviderId));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 30, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 27), 24, 48, targetProviderId));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 3, 25, 6, 0),
                3, 2, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(18).investments(0)
                .timestamp(date(2022, 3, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(42).investments(0)
                .timestamp(date(2022, 3, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 3, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));

        records.clear();
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        expectedResults.clear();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 3, 26)).build());

        result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Apr 25 8am: Scale VM from 3 -> 5, not expecting RI coverage after the action.
     * Apr 26 6am: the VM is 100% covered by RI.
     * Cannot claim savings in this situation. Savings = 0. Investment = 0.
     */
    @Test
    public void testRILimitSavingsToZero() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 4, 25), 8, 8 * 3, sourceProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 25), 16, 16 * 5, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 6, 6 * 5, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 10, 0, targetProviderId, PriceModel.RESERVED));
        records.add(createVMBillRecord(date(2022, 4, 27), 24, 0, targetProviderId, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 4, 25, 8, 0),
                3, 5, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(32)
                .timestamp(date(2022, 4, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(0)
                .timestamp(date(2022, 4, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(0)
                .timestamp(date(2022, 4, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Apr 25 8am: Scale VM from 5 -> 3, not expecting RI coverage after the action.
     * Apr 26 6am: the VM is 100% covered by RI.
     * Cannot claim savings more than $2/hour.
     */
    @Test
    public void testRILimitSavingsOnScaleDown() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 4, 25), 8, 8 * 5, sourceProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 25), 16, 16 * 3, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 6, 6 * 3, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 18, 0, targetProviderId, PriceModel.RESERVED));
        records.add(createVMBillRecord(date(2022, 4, 27), 24, 0, targetProviderId, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 4, 25, 8, 0),
                5, 3, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(32).investments(0)
                .timestamp(date(2022, 4, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(48).investments(0)
                .timestamp(date(2022, 4, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(48).investments(0)
                .timestamp(date(2022, 4, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }


    /**
     * Apr 25 8am: Scale VM from 3 -> 5, expecting 50% RI coverage after the action.
     * Apr 26 6am: the VM is 100% covered by RI.
     * No need to restrict savings.
     */
    @Test
    public void testRICoverageChangeAfterScaleUp() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 4, 25), 8, 8 * 3, sourceProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 25), 16, 16 * 5, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 6, 6 * 5, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 18, 0, targetProviderId, PriceModel.RESERVED));
        records.add(createVMBillRecord(date(2022, 4, 27), 24, 0, targetProviderId, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 4, 25, 8, 0),
                3, 5, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 0.5));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(32)
                .timestamp(date(2022, 4, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(18 * 3 - 2 * 6).investments(0)
                .timestamp(date(2022, 4, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24 * 3).investments(0)
                .timestamp(date(2022, 4, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Apr 25 8am: Scale VM from 5 -> 3, expecting 50% RI coverage after the action.
     * Apr 26 6am: the VM is 100% covered by RI.
     * No need to restrict savings.
     */
    @Test
    public void testRICoverageChangeAfterScaleDown() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 4, 25), 8, 8 * 5, sourceProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 25), 16, 16 * 3, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 6, 6 * 3, targetProviderId, PriceModel.ON_DEMAND));
        records.add(createVMBillRecord(date(2022, 4, 26), 18, 0, targetProviderId, PriceModel.RESERVED));
        records.add(createVMBillRecord(date(2022, 4, 27), 24, 0, targetProviderId, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 4, 25, 8, 0),
                5, 3, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 0.5));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(32).investments(0)
                .timestamp(date(2022, 4, 25)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(2 * 6 + 5 * 18).investments(0)
                .timestamp(date(2022, 4, 26)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(5 * 24).investments(0)
                .timestamp(date(2022, 4, 27)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Action chain test case 1:
     * 1. June 1: Scale VM from 4 -> 3, not expecting RI coverage.
     * 2. June 2: Scale VM from 3 -> 5, not expecting RI coverage.
     * 3. June 3: Bill record shows 24 hours fully covered by RI.
     * Expect no savings and no investments.
     * There are no savings because both actions did not expect RI coverage. So we can't claim
     * savings even when the VM is covered by RI.
     */
    @Test
    public void testRICoverageActionChain1() {
        final long tierA = 1212121212L;
        final long tierB = 2323232323L;
        final long tierC = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 6, 3), 24, 0, tierC, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 1, 8, 0),
                4, 3, tierA, tierB,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                LivenessState.SUPERSEDED, 0));
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                3, 5, tierB, tierC,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 6, 3)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Action chain test case 2:
     * 1. June 1: Scale VM from 4 -> 3, expecting 50% RI coverage.
     * 2. June 2: Scale VM from 3 -> 5, not expecting RI coverage.
     * 3. June 3: Bill record shows 24 hours fully covered by RI.
     * Expect no savings and no investments.
     * The first action is supposed to have 4 dollars of savings because it expects RI.
     * However, the second action cannot claim savings because it is a scale up action and it did
     * not expect RI. In this situation, we decided to let the first action claim savings up to $3.
     * It was a design decision.
     */
    @Test
    public void testRICoverageActionChain2() {
        final long tierA = 1212121212L;
        final long tierB = 2323232323L;
        final long tierC = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 6, 3), 24, 0, tierC, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 1, 8, 0),
                4, 3, tierA, tierB,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                LivenessState.SUPERSEDED, 0.5));
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                3, 5, tierB, tierC,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 6, 3)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Action chain test case 3:
     * Action 1: June 1 scale VM from $8 → $5. Not expecting RI coverage after the action.
     * Action 2: June 2 scale VM from $5 → $6. Not expecting RI coverage after the action.
     * Action 3: June 3 scale VM from $6 → $4. Not expecting RI coverage after the action.
     * On June 4, the bill record shows VM is 100% covered by RI, hence cost is $0. What are the savings and investments for June 4?
     * Investment: $0
     * Savings: Adjust the cost for June 4 to $4. So savings is $4 for June 4.
     */
    @Test
    public void testRICoverageActionChain3() {
        final long tierA = 1212121212L;
        final long tierB = 2323232323L;
        final long tierC = 3434343434L;
        final long tierD = 4545454545L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 6, 4), 24, 0, tierD, PriceModel.RESERVED));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 1, 8, 0),
                8, 5, tierA, tierB,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                LivenessState.SUPERSEDED, 0));
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 2, 10, 0),
                5, 6, tierB, tierC,
                LocalDateTime.of(2022, 6, 3, 14, 0),
                LivenessState.SUPERSEDED, 0));
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 6, 3, 14, 0),
                6, 4, tierC, tierD,
                null, LivenessState.LIVE, 0));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(4 * 24).investments(0)
                .timestamp(date(2022, 6, 4)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, LocalDateTime.now(clock));
        Assert.assertTrue(expectedResults.containsAll(result));
    }
}
