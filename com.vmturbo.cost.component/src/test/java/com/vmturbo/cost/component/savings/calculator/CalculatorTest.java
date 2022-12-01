package com.vmturbo.cost.component.savings.calculator;

import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.BillingRecord;
import com.vmturbo.cost.component.savings.GrpcActionChainStore;
import com.vmturbo.cost.component.savings.ScenarioGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.trax.TraxConfiguration;

/**
 * Test cases for bill-based savings calculator.
 */
public class CalculatorTest {

    private final long vmOid = 1234L;
    private final long volumeOid = 5555L;
    private final long dbOid = 8888L;
    private final Calculator calculator;
    private final Clock clock = Clock.systemUTC();
    private final long lastProcessedDate = getTimestamp(date(2022, 3, 24));
    private final Comparator<ExecutedActionsChangeWindow> changeWindowComparator =
            GrpcActionChainStore.changeWindowComparator;
    private final StoragePriceStructure storagePriceStructure = mock(StoragePriceStructure.class);

    @BeforeClass
    public static void setup() {
        // Added this line for convenience. Change this log level to DEBUG or TRACE to get more
        // output when running test cases manually.
        Configurator.setAllLevels("com.vmturbo.cost.component.savings", Level.INFO);
        // Change trax verbosity level manually to get see the trax output.
        TraxConfiguration.configureTopics(Calculator.TRAX_TOPIC, Verbosity.OFF);
    }

    public CalculatorTest() {
        long volumeExpiryMs = 10; // dummy value
        when(storagePriceStructure.getEndRangeInPriceTier(anyDouble(), anyLong(), anyLong(), anyLong()))
                .thenAnswer(invocation ->
                        invocation.getArgumentAt(0, Double.class));
        this.calculator = new Calculator(volumeExpiryMs, clock, storagePriceStructure);
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 26));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 26));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 27));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 27));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 27));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 26));
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 26));
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
     * Scale action executed on 2022-03-24T12:00, (A -> B), cost went from $1/hr to $2/hr.
     * Scale action executed on 2022-03-25T06:00, (B -> C), cost went from $2/hr to $3/hr.
     * Scale action executed on 2022-03-25T15:00, (C -> B), cost went from $3/hr to $2/hr.
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
        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 26));
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
        return createVMBillRecord(dateTime, usageAmount, cost, providerId, priceModel, costCategory);
    }

    private BillingRecord createVMBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId, PriceModel priceModel, CostCategory costCategory) {
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(vmOid)
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .accountId(1L)
                .regionId(2L)
                .providerType(EntityType.VIRTUAL_MACHINE_VALUE)
                .commodityType(CommodityType.UNKNOWN_VALUE)
                .costCategory(costCategory)
                .priceModel(priceModel)
                .serviceProviderId(100L)
                .build();
    }

    private BillingRecord createVolumeBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId, int commType) {
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(volumeOid)
                .entityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .accountId(1L)
                .regionId(2L)
                .providerType(EntityType.STORAGE_TIER_VALUE)
                .commodityType(commType)
                .costCategory(CostCategory.STORAGE)
                .priceModel(PriceModel.ON_DEMAND)
                .serviceProviderId(100L)
                .build();
    }

    private BillingRecord createDBBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId, int commType, int providerType, CostCategory costCategory) {
        return new BillingRecord.Builder()
                .sampleTime(dateTime)
                .usageAmount(usageAmount)
                .cost(cost)
                .providerId(providerId)
                .entityId(dbOid)
                .entityType(EntityType.DATABASE_VALUE)
                .accountId(1L)
                .regionId(2L)
                .providerType(providerType)
                .commodityType(commType)
                .costCategory(costCategory)
                .priceModel(PriceModel.ON_DEMAND)
                .serviceProviderId(100L)
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
        Calculator calculator = new Calculator(volumeExpiryMs, clock, storagePriceStructure);
        final long storageTierOid = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        double storageCostPerHour = 0.05;
        if (lastProcessed.isBefore(date(2022, 3, 25))) {
            records.add(createVolumeBillRecord(date(2022, 3, 25), 6, storageCostPerHour * 6,
                    storageTierOid, CommodityType.STORAGE_AMOUNT_VALUE));
        }
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeDeleteActionSpec(volumeOid, LocalDateTime.of(2022, 3, 25, 6, 0),
                0.05, storageTierOid));
        return calculator.calculate(volumeOid, records, actionSpecs, lastProcessedInMillis, referenceTime);
    }

    private void validateResults(List<SavingsValues> results, List<SavingsValues> expectedResults) {
        Assert.assertEquals(expectedResults.size(), results.size());
        Assert.assertEquals(expectedResults.stream().map(SavingsValues::getTimestamp).collect(Collectors.toSet()),
                results.stream().map(SavingsValues::getTimestamp).collect(Collectors.toSet()));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 28));
        Assert.assertTrue(expectedResults.containsAll(result));

        records.clear();
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        expectedResults.clear();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(24).investments(0)
                .timestamp(date(2022, 3, 26)).build());

        result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 3, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 4, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 4, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 4, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 4, 28));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 6, 4));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 6, 4));
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

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs, lastProcessedDate, date(2022, 6, 5));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Scaled an Azure Windows VM to a new tier where we expect 100% RI coverage.
     * Although it is 100% covered, there is a license cost that we still need to pay.
     * Each day the VM will get 2 bill records - one for reserved compute cost which is $0, and
     * one for the license cost. Both records have usageAmount of 24 hours, but ensure we are not
     * adding up both values to get 48 hours. The algorithm excludes the usage amount of the licence
     * cost.
     */
    @Test
    public void testVMScaleWithLicenseCost() {
        final long sourceProviderId = 1212121212L;
        final long targetProviderId = 2323232323L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 4, 25), 24, 0, targetProviderId, PriceModel.RESERVED, CostCategory.COMPUTE));
        records.add(createVMBillRecord(date(2022, 4, 25), 24, 48, targetProviderId, PriceModel.ON_DEMAND, CostCategory.LICENSE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs =
                new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVMActionChangeWindow(vmOid,
                LocalDateTime.of(2022, 4, 20, 8, 0),
                1, 5, sourceProviderId, targetProviderId,
                null, LivenessState.LIVE, 1));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(vmOid).savings(0).investments(24)
                .timestamp(date(2022, 4, 25)).build());

        List<SavingsValues> result = calculator.calculate(vmOid, records, actionSpecs,
                getTimestamp(date(2022, 4, 24)), date(2022, 4, 27));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Scale volume from one provider to another. It is not an Azure ultra disk. There is one bill
     * record for each service tier per day.
     *
     * <p>Action 1: June 1 10am: scale volume to a different provider. Cost changed from $1/hr to #3/hr.
     *                        Disk size changed from 5GB to 8GB.
     * Action 2: June 3 3pm:  scale volume to a different provider. Cost changed from $3/hr to #2/hr.
     *                        Disk size changed from 8GB to 7GB.
     */
    @Test
    public void testVolumeScaleToDifferentProvider() {
        final long tierA = 1212121212L;
        final long tierB = 2323232323L;
        final long tierC = 3434343434L;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 5 * 10, 10, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 8 * 14, 3 * 14, tierB, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 8 * 24, 3 * 24, tierB, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 8 * 15, 3 * 15, tierB, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 7 * 9, 2 * 9, tierC, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                1, 3, tierA, tierB,
                LocalDateTime.of(2022, 6, 3, 15, 0), LivenessState.SUPERSEDED, null));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 15, 0),
                3, 2, tierB, tierC,
                null, LivenessState.LIVE, null));

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(14 * 2)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(24 * 2)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(9).investments(15 * 2 + 9)
                .timestamp(date(2022, 6, 3)).build());

        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        Assert.assertTrue(expectedResults.containsAll(result));
    }

    /**
     * Scale volume by changing IOPS and IO Throughput only. (Azure Ultra disk)
     * This ultra disk originally has storage of 16GB, IOPS of 100 and IO Throughput of 1MBPS.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps.
     * Action 2: June 3 1pm: Change disk commodities: IOPS 200 to 150.
     */
    @Test
    public void testVolumeScaleSameProviderChangeComm() {
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 100 * 10 + 200 * 14, (100 * 10 + 200 * 14) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 10 + 2 * 14, (10 + 2 * 14) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 200 * 24, (200 * 24) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 200 * 13 + 150 * 11, (200 * 13 + 150 * 11) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 200, 150));
        double sourceOnDemandRate2 = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate2 = (150 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                sourceOnDemandRate2, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day1Investment = destOnDemandRate * 14 - sourceOnDemandRate * 14;
        double day2Investment = destOnDemandRate * 24 - sourceOnDemandRate * 24;
        // On June 3, value of sourceOnDemandRate is the low watermark.
        double day3Investment = (destOnDemandRate - sourceOnDemandRate) * 13 + (destOnDemandRate2 - sourceOnDemandRate) * 11;
        // On June 3, value of sourceOnDemandRate2 is the high watermark.
        double day3Savings = (sourceOnDemandRate2 - destOnDemandRate2) * 11;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Savings).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        validateResults(result, expectedResults);
    }

    /**
     * Scale volume by changing storage amount, IOPS and IO Throughput. (Azure Ultra disk)
     * This ultra disk originally has storage of 14GB, IOPS of 100 and IO Throughput of 1MBPS.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps, storage amount 14GB to 30GB.
     * Action 2: June 3 1pm: Change disk commodities: IOPS 200 to 150.
     */
    @Test
    public void testVolumeScaleSameProviderChange3Comm() {
        when(storagePriceStructure.getEndRangeInPriceTier(anyDouble(), anyLong(), anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    double storageAmount = invocation.getArgumentAt(0, Double.class);
                    if (storageAmount > 8 && storageAmount <= 16) {
                        return 16;
                    } else if (storageAmount > 16 && storageAmount <= 32) {
                        return 32;
                    }
                    return storageAmount;
                });
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 100 * 10 + 200 * 14, (100 * 10 + 200 * 14) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 10 + 2 * 14, (10 + 2 * 14) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 10 + 32 * 14, (16 * 10 + 32 * 14) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 200 * 24, (200 * 24) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 32 * 24, (32 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 200 * 13 + 150 * 11, (200 * 13 + 150 * 11) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 32 * 24, (32 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 14 * 1024, 30 * 1024));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 32 * storageAmountRate); // cost per hour
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 200, 150));
        double sourceOnDemandRate2 = (200 * storageAccessRate + 2 * ioThroughputRate + 32 * storageAmountRate); // cost per hour
        double destOnDemandRate2 = (150 * storageAccessRate + 2 * ioThroughputRate + 32 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                sourceOnDemandRate2, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day1Investment = destOnDemandRate * 14 - sourceOnDemandRate * 14;
        double day2Investment = destOnDemandRate * 24 - sourceOnDemandRate * 24;
        // On June 3, value of sourceOnDemandRate is the low watermark.
        double day3Investment = (destOnDemandRate - sourceOnDemandRate) * 13 + (destOnDemandRate2 - sourceOnDemandRate) * 11;
        // On June 3, value of sourceOnDemandRate2 is the high watermark.
        double day3Savings = (sourceOnDemandRate2 - destOnDemandRate2) * 11;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Savings).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        validateResults(result, expectedResults);
    }

    /**
     * Test the case when the bill only included the costs for part of a day.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps.
     * Action 2: June 3 1pm: Change disk commodities: IOPS 200 to 150.
     * The bill for June 3 only include 20 hours of cost.
     */
    @Test
    public void testPartialBill() {
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 100 * 10 + 200 * 14, (100 * 10 + 200 * 14) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 10 + 2 * 14, (10 + 2 * 14) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 200 * 24, (200 * 24) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 200 * 13 + 150 * 7, (200 * 13 + 150 * 7) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 2 * 20, (2 * 20) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 16 * 20, (16 * 20) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 200, 150));
        double sourceOnDemandRate2 = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate2 = (150 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                sourceOnDemandRate2, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day1Investment = destOnDemandRate * 14 - sourceOnDemandRate * 14;
        double day2Investment = destOnDemandRate * 24 - sourceOnDemandRate * 24;
        // On June 3, value of sourceOnDemandRate is the low watermark.
        double day3Investment = (destOnDemandRate - sourceOnDemandRate) * 13 + (destOnDemandRate2 - sourceOnDemandRate) * 7;
        // On June 3, value of sourceOnDemandRate2 is the high watermark.
        double day3Savings = (sourceOnDemandRate2 - destOnDemandRate2) * 7;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Savings).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        validateResults(result, expectedResults);
    }

    /**
     * 1 scale action than changes commodity capacities on an ultra disk, followed by another action
     * that scales to another provider on the same day.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps.
     * Action 2: June 1 5pm: Scale disk to another provider. Storage is still 16GB.
     */
    @Test
    public void testFirstSegment2ProvidersOneDay() {
        final long tierA = 1212121212L;
        final long tierB = 2323232323L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        final double tierBRate = 0.022; // cost per hour
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 100 * 10 + 200 * 7, (100 * 10 + 200 * 7) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 10 + 2 * 7, (10 + 2 * 7) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 17, (16 * 17) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 7, 7 * tierBRate, tierB, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 17, 0),
                destOnDemandRate, tierBRate, tierA, tierB,
                null, LivenessState.LIVE, null));

        double day1Investment = (destOnDemandRate - sourceOnDemandRate) * 7 + (tierBRate - sourceOnDemandRate) * 7;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 2));
        validateResults(result, expectedResults);
    }

    /**
     * 2 scale actions on an ultra disk on the same day. First one changes 2 commodities and second
     * one changes only 1 commodity.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps.
     * Action 2: June 1 5pm: Scale IOPS to 300.
     */
    @Test
    public void testFirstSegment1Provider2ActionsOneDay() {
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 100 * 10 + 200 * 7 + 300 * 7, (100 * 10 + 200 * 7 + 300 * 7) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 10 + 2 * 14, (10 + 2 * 14) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 200, 300));

        double destOnDemandRate2 = (300 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 17, 0),
                destOnDemandRate, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day1Investment = (destOnDemandRate - sourceOnDemandRate) * 7 + (destOnDemandRate2 - sourceOnDemandRate) * 7;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 2));
        validateResults(result, expectedResults);
    }

    /**
     * First volume scale action is reverted. The second volume scale action was executed on the
     * following day. Make sure only a portion of the cost of commodities is assigned to the cost
     * after the action. i.e. the multiplier calculation need to take into account of the segment
     * of the terminated action.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 100 to 200; IO_Throughput 1MBps to 2MBps.
     * On June 1 4pm, the IOPS of the disk is externally changed to 100.
     * Action 2: June 2 5pm: Scale IOPS from 100 to 300.
     */
    @Test
    public void testActionAfterExternalModification() {
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 2), 100 * 17 + 300 * 7, (100 * 17 + 300 * 7) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 2 * 24, (2 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 200));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 1024, 2 * 1024));
        double sourceOnDemandRate = (100 * storageAccessRate + ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate = (200 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                LocalDateTime.of(2022, 6, 1, 16, 0), LivenessState.EXTERNAL_MODIFICATION, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 100, 300));

        double sourceOnDemandRate2 = (100 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        double destOnDemandRate2 = (300 * storageAccessRate + 2 * ioThroughputRate + 16 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 2, 17, 0),
                sourceOnDemandRate2, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day2Investment = (destOnDemandRate2 - sourceOnDemandRate) * 7;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 2)), date(2022, 6, 3));
        validateResults(result, expectedResults);
    }

    /**
     * Scale volume within one service tier by change storage amount only. 5GB is billed for 8GB.
     *
     * <p>Action 1: June 1 10am: scale volume from 5GB to 15GB. Cost changed from $1/hr to #3/hr.
     *                        Volumes 4-8GB cost $1/hr and volumes 8-16 costs $3/hr.
     * Action 2: June 3 3pm:  scale volume to a different provider. Cost changed from $3/hr to #1/hr.
     *                        Disk size changed from 15GB to 7GB.
     */
    @Test
    public void testVolumeScaleAdjustStorageAmount() {
        when(storagePriceStructure.getEndRangeInPriceTier(anyDouble(), anyLong(), anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    double storageAmount = invocation.getArgumentAt(0, Double.class);
                    if (storageAmount > 4 && storageAmount <= 8) {
                        return 8;
                    } else if (storageAmount > 8 && storageAmount <= 16) {
                        return 16;
                    }
                    return storageAmount;
                });
        final long tierA = 1212121212L;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 8 * 10 + 16 * 14, (8 * 10 + 16 * 14) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 16 * 24, (16 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 16 * 15 + 8 * 9, (16 * 15 + 8 * 9) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 5 * 1024, 15 * 1024));
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                8 * storageAmountRate, 16 * storageAmountRate, tierA, tierA,
                LocalDateTime.of(2022, 6, 3, 15, 0), LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 15 * 1024, 7 * 1024));
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 15, 0),
                16 * storageAmountRate, 8 * storageAmountRate, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        List<SavingsValues> expectedResults = new ArrayList<>();
        final double day1Investment = (16 - 8) * storageAmountRate * 14;
        final double day2Investment = (16 - 8) * storageAmountRate * 24;
        final double day3Investment = (16 - 8) * storageAmountRate * 15;
        final double day3Savings = (16 - 8) * storageAmountRate * 9;
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Savings).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());

        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        validateResults(result, expectedResults);
    }

    /**
     * Test volume scale action within the same service tier but cost per GB changes.
     * June 1 10am: Scale volume from 8GB to 64GB within the same service tier (e.g. Azure Premium SSD).
     * The cost for the 8GB volume costs $1.44/month. (1.44 / 8 = $0.18 per GB per month)
     * The cost for the 64GB volume costs $10.207/month. (10.207 / 64 = $0.15 per GB per month)
     * The cost per GB is different before and after the action.
     * June 2 1pm: Scale volume from 64GB to 16GB within the same service tier.
     * The cost for a 32GB costs $5.28/month. (5.28 / 32 = $0.165 per GB per month)
     * Note that there is only 1 segment on June 1 and 2 segments on June 2
     */
    @Test
    public void testVolumeScaleVaryingRate() {
        final long tierA = 1212121212L;
        // Cost for volume per hour before action (June 1)
        final double sourceRate = 1.44 / 730;
        // Cost for volume per hour after action (June 1)
        final double destinationRate = 10.207 / 730;
        // Cost for volume per hour after action on June 2.
        final double destinationRate2 = 5.28 / 730;
        // Cost of the volume on the day of the scale action
        double costForJune1 = 10 * sourceRate + 14 * destinationRate;
        double costForJune2 = 13 * destinationRate + 11 * destinationRate2;

        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 8 * 10 + 64 * 14, costForJune1, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 64 * 13 + 32 * 11, costForJune2, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 8 * 1024, 64 * 1024));
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceRate, destinationRate, tierA, tierA,
                LocalDateTime.of(2022, 6, 2, 13, 0),
                LivenessState.SUPERSEDED, resizeInfoList));
        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 64 * 1024, 32 * 1024));
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 2, 13, 0),
                destinationRate, destinationRate2, tierA, tierA, null, LivenessState.LIVE, resizeInfoList2));

        List<SavingsValues> expectedResults = new ArrayList<>();
        final double day1Investment = destinationRate * 14 - sourceRate * 14;
        final double day2Investment = (destinationRate * 13 - sourceRate * 13) + (destinationRate2 * 11 - sourceRate * 11);
        final double day2Savings = destinationRate * 11 - destinationRate2 * 11;
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day2Savings).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());

        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 2));
        validateResults(result, expectedResults);
    }

    /**
     * Test savings for scaling a DTU databases. No extra storage is involved in this use case.
     * June 1 10am: Scale DTU Premium P1 to P4.
     * June 3 1:30pm: Scale from Premium P4 to P2.
     */
    @Test
    public void testDtuScale() {
        final long p1DtuOid = 1212121212L; // P1
        final long p2DtuOid = 2323232323L; // P2
        final long p4DtuOid = 3434343434L; // P4
        final double p1DtuRate = 0.625; // cost per hour per DB
        final double p2DtuRate = 1.25; // cost per hour per DB
        final double p4DtuRate = 2.5; // cost per hour per DB
        Set<BillingRecord> records = new HashSet<>();

        records.add(createDBBillRecord(date(2022, 6, 1), 10, 10 * p1DtuRate, p1DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 1), 14, 14 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 2), 24, 24 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 3), 13.5, 13.5 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 3), 10.5, 10.5 * p2DtuRate, p2DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));

        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                p1DtuRate, p4DtuRate, p1DtuOid, p4DtuOid,
                LocalDateTime.of(2022, 6, 3, 13, 30),
                LivenessState.SUPERSEDED, null));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 3, 13, 30),
                p4DtuRate, p2DtuRate, p4DtuOid, p2DtuOid,
                null, LivenessState.LIVE, null));

        final double day1Investment = 2.5 * 14 - 0.625 * 14;
        final double day2Investment = 2.5 * 24 - 0.625 * 24;
        final double day3Investment = (2.5 - 0.625) * 13.5 + (1.25 - 0.625) * 10.5;
        final double day3Saving = (2.5 - 1.25) * 10.5;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Saving).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());

        List<SavingsValues> result = calculator.calculate(dbOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 3));

        validateResults(result, expectedResults);
    }

    /**
     * Test savings for scaling a DTU databases.
     * June 1 10am: Scale DTU Premium P1 to P4. Storage amount changed from 750GB to 1024GB.
     * June 3 1pm: Scale DTU disk from P4 to P2. Storage amount changed from 1024GB to 500GB.
     * Expected results:
     * June 1: no savings and investments because we are not calculating savings for databases on
     * the day when action was executed.
     * June 2: expect an investment because of the scale-up action from the previous day.
     * June 3: No savings or investments because an action was executed.
     * June 4: both savings and investments are expected.
     */
    @Test
    public void testDtuScaleExtraStorage() {
        final long p1DtuOid = 1212121212L; // P1
        final long p2DtuOid = 2323232323L; // P2
        final long p4DtuOid = 3434343434L; // P4
        final double p1DtuRate = 0.625; // cost per hour per DB
        final double p2DtuRate = 1.25; // cost per hour per DB
        final double p4DtuRate = 2.5; // cost per hour per DB
        final double storageRate = 0.34 / 730; // cost per GB per hour
        Set<BillingRecord> records = new HashSet<>();

        records.add(createDBBillRecord(date(2022, 6, 1), 10, 10 * p1DtuRate, p1DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 1), 14, 14 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 1), 10 * 250 + 14 * 524, (10 * 250 + 14 * 524) * storageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));
        records.add(createDBBillRecord(date(2022, 6, 2), 24, 24 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 2), 24 * 524, (24 * 524) * storageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));
        records.add(createDBBillRecord(date(2022, 6, 3), 13, 13 * p4DtuRate, p4DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 3), 11, 11 * p2DtuRate, p2DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));
        records.add(createDBBillRecord(date(2022, 6, 3), 13 * 524, (13 * 524) * storageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));
        records.add(createDBBillRecord(date(2022, 6, 4), 24, 24 * p2DtuRate, p2DtuOid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE_LICENSE_BUNDLE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 750 * 1024, 1024 * 1024));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        double beforeActionRate = p1DtuRate + 250 * storageRate;  // 250 GB of extra storage
        double afterActionRate = p4DtuRate + 524 * storageRate;  // 524 GB of extra storage
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                beforeActionRate, afterActionRate, p1DtuOid, p4DtuOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 1024 * 1024, 500 * 1024));
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                afterActionRate, p2DtuRate, p4DtuOid, p2DtuOid,
                null, LivenessState.LIVE, resizeInfoList2));

        final double day2Cost = 24 * p4DtuRate + 24 * 524 * storageRate;
        final double day2Investment = day2Cost - 24 * beforeActionRate;

        final double day4Cost = 24 * p2DtuRate;
        final double day4Investment = day4Cost - 24 * beforeActionRate;
        final double day4Savings = 24 * afterActionRate - day4Cost;

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 3)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(day4Savings).investments(day4Investment)
                .timestamp(date(2022, 6, 4)).build());

        List<SavingsValues> result = calculator.calculate(dbOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 4));

        validateResults(result, expectedResults);
    }

    /**
     * Test savings for database scale (from GP to GP).
     * June 1 10am: Scale GP_Gen5_2 to GP_Gen5_8. Scaled storage from 500GB to 1024GB.
     * June 3 1pm: Scale GP_Gen5_8 to GP_Gen5_4.
     */
    @Test
    public void testGpToGpScale() {
        final long gpGen5vc2Oid = 1212121212L; // GP_Gen5_2
        final long gpGen5vc8Oid = 2323232323L; // GP_Gen5_8
        final long gpGen5vc4Oid = 3434343434L; // GP_Gen5_8

        final double gpGen5Rate = 0.151992945326279; // cost per hour per vcore
        final double gpLicenseRate = 0.099898589065256; // cost per hour per vcore
        final double gpStorageRate = 0.114814814814815 / 730; // cost per GB per hour

        Set<BillingRecord> records = new HashSet<>();

        records.add(createDBBillRecord(date(2022, 6, 1), 10, 10 * 2 * gpGen5Rate, gpGen5vc2Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 1), 14, 14 * 8 * gpGen5Rate, gpGen5vc8Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 1), 2 * 10 + 8 * 14, (2 * 10 + 8 * 14) * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 1), 500 * 10 + 1024 * 14, (500 * 10 + 1024 * 14) * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 2), 24, 24 * 8 * gpGen5Rate, gpGen5vc8Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 2), 24 * 8, 24 * 8 * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 2), 1024 * 24, (1024 * 24) * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 3), 13, 13 * 8 * gpGen5Rate, gpGen5vc8Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 3), 11, 11 * 4 * gpGen5Rate, gpGen5vc4Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 3), 8 * 13 + 4 * 11, (8 * 13 + 4 * 11) * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 3), 1024 * 24, 1024 * 24 * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 4), 24, 24 * 4 * gpGen5Rate, gpGen5vc4Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 4), 24 * 4, 24 * 4 * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 4), 1024 * 24, (1024 * 24) * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 500 * 1024, 1024 * 1024));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        double beforeActionRate = gpGen5Rate * 2 + gpLicenseRate * 2 + gpStorageRate * 500;
        double afterActionRate = gpGen5Rate * 8 + gpLicenseRate * 8 + gpStorageRate * 1024;
        double afterActionRate2 = gpGen5Rate * 4 + gpLicenseRate * 4 + gpStorageRate * 1024; // rate after 2nd action
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                beforeActionRate, afterActionRate, gpGen5vc2Oid, gpGen5vc8Oid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                LivenessState.SUPERSEDED, resizeInfoList));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 3, 10, 0),
                afterActionRate, afterActionRate2, gpGen5vc8Oid, gpGen5vc4Oid, null,
                LivenessState.LIVE, null));

        final double day2Investment = 24 * afterActionRate - 24 * beforeActionRate;
        final double day4Investment = 24 * afterActionRate2 - 24 * beforeActionRate;
        final double day4Savings = 24 * afterActionRate - 24 * afterActionRate2;

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 3)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(day4Savings).investments(day4Investment)
                .timestamp(date(2022, 6, 4)).build());

        List<SavingsValues> result = calculator.calculate(dbOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 4));

        validateResults(result, expectedResults);
    }

    /**
     * Test savings for scale actions between GP and BC.
     * June 1 10am: Scale GP_Gen5_8 to BC_Gen5_12. Scaled storage from 500GB to 1024GB.
     * June 3 1pm: Scale BC_Gen5_12 to GP_Gen5_12.
     */
    @Test
    public void testGPToBCScale() {
        final long gpGen5vc8Oid = 2323232323L; // GP_Gen5_8
        final long bcGen5vc12Oid = 3434343434L; // BC_Gen5_12
        final long gpGen5vc12Oid = 4545454545L; // GP_Gen5_12

        final double gpGen5Rate = 0.151992945326279; // cost per hour per vcore
        final double gpLicenseRate = 0.099898589065256; // cost per hour per vcore
        final double gpStorageRate = 0.114814814814815 / 730; // cost per GB per hour

        final double bcGen5Rate = 0.791984126984127; // cost per hour per vcore
        final double bcLicenseRate = 0.12; // cost per hour per vcore
        final double bcStorageRate = 0.298076923076923 / 730; // cost per GB per hour

        Set<BillingRecord> records = new HashSet<>();

        records.add(createDBBillRecord(date(2022, 6, 1), 10, 10 * 8 * gpGen5Rate, gpGen5vc8Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 1), 14, 14 * 12 * bcGen5Rate, bcGen5vc12Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 1), 8 * 10 + 12 * 14, 8 * 10 * gpLicenseRate + 12 * 14 * bcLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 1), 500 * 10 + 1024 * 14, 500 * 10 * gpStorageRate + 1024 * 14 * bcStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 2), 24, 24 * 12 * bcGen5Rate, bcGen5vc12Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 2), 12 * 24, 12 * 24 * bcLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 2), 1024 * 24, 1024 * 24 * bcStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 3), 13, 13 * 12 * bcGen5Rate, bcGen5vc12Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 3), 11, 11 * 12 * gpGen5Rate, gpGen5vc12Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 3), 12 * 13 + 12 * 11, 12 * 13 * bcLicenseRate + 12 * 11 * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 3), 1024 * 24, 1024 * 13 * bcStorageRate + 1024 * 11 * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 4), 24, 24 * 12 * gpGen5Rate, gpGen5vc12Oid, CommodityType.UNKNOWN_VALUE,
                EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 4), 12 * 24, 12 * 24 * gpLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 4), 1024 * 24, 1024 * 24 * gpStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 500 * 1024, 1024 * 1024));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        double beforeActionRate = gpGen5Rate * 8 + gpLicenseRate * 8 + gpStorageRate * 500;
        double afterActionRate = bcGen5Rate * 12 + bcLicenseRate * 12 + bcStorageRate * 1024;
        double afterActionRate2 = gpGen5Rate * 12 + gpLicenseRate * 12 + gpStorageRate * 1024;
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                beforeActionRate, afterActionRate, gpGen5vc8Oid, bcGen5vc12Oid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                LivenessState.SUPERSEDED, resizeInfoList));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                afterActionRate, afterActionRate2, bcGen5vc12Oid, gpGen5vc12Oid,
                null, LivenessState.LIVE, null));

        final double day2Investment = 24 * afterActionRate - 24 * beforeActionRate;
        final double day4Investment = 24 * afterActionRate2 - 24 * beforeActionRate;
        final double day4Savings = 24 * afterActionRate - 24 * afterActionRate2;

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 3)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(day4Savings).investments(day4Investment)
                .timestamp(date(2022, 6, 4)).build());

        List<SavingsValues> result = calculator.calculate(dbOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 4));

        validateResults(result, expectedResults);
    }

    /**
     * Test savings calculation for scale actions within hyperscale service tier.
     * All records map to provider 0. Exclude savings on days where actions are taken.
     * June 1 10am: Scale HS_Gen5_2 to HS_Gen5_8. Scaled storage from 500GB to 1024GB.
     * June 3 1pm: Scale HS_Gen5_8 to HS_Gen5_4.
     */
    @Test
    public void testHyperScale() {
        final double hsGen5Rate = 0.18265625; // cost per hour per vcore
        final double hsLicenseRate = 9.5025; // cost per hour per vcore
        final double hsStorageRate = 0.098904761904762 / 730; // cost per GB per hour

        Set<BillingRecord> records = new HashSet<>();

        records.add(createDBBillRecord(date(2022, 6, 1), 24, (2 * 10 + 8 * 14) * hsGen5Rate,
                0, CommodityType.UNKNOWN_VALUE, EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 1), 2 * 10 + 8 * 14, (2 * 10 + 8 * 14) * hsLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 1), 500 * 10 + 1024 * 14, (500 * 10 + 1024 * 14) * hsStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 2), 24, (8 * 24) * hsGen5Rate,
                0, CommodityType.UNKNOWN_VALUE, EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 2), 8 * 24, 8 * 24 * hsLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 2), 1024 * 24, 1024 * 24 * hsStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 3), 24, (8 * 10 + 4 * 14) * hsGen5Rate,
                0, CommodityType.UNKNOWN_VALUE, EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 3), 8 * 10 + 4 * 14, (8 * 10 + 4 * 14) * hsLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 3), 1024 * 24, 1024 * 24 * hsStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        records.add(createDBBillRecord(date(2022, 6, 4), 24, (4 * 24) * hsGen5Rate,
                0, CommodityType.UNKNOWN_VALUE, EntityType.DATABASE_TIER_VALUE, CostCategory.COMPUTE));
        records.add(createDBBillRecord(date(2022, 6, 4), 4 * 24, 4 * 24 * hsLicenseRate,
                0, CommodityType.UNKNOWN_VALUE, 0, CostCategory.LICENSE));
        records.add(createDBBillRecord(date(2022, 6, 4), 1024 * 24, 1024 * 24 * hsStorageRate,
                0, CommodityType.STORAGE_AMOUNT_VALUE, 0, CostCategory.STORAGE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 500 * 1024, 1024 * 1024));
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        double beforeActionRate = hsGen5Rate * 2 + hsLicenseRate * 2 + hsStorageRate * 500;
        double afterActionRate = hsGen5Rate * 8 + hsLicenseRate * 8 + hsStorageRate * 1024;
        double afterActionRate2 = hsGen5Rate * 4 + hsLicenseRate * 4 + hsStorageRate * 1024;
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                beforeActionRate, afterActionRate, 0, 0,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                LivenessState.SUPERSEDED, resizeInfoList));

        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(dbOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                afterActionRate, afterActionRate2, 0, 0,
                null, LivenessState.LIVE, null));

        final double day2Investment = 24 * afterActionRate - 24 * beforeActionRate;
        final double day4Investment = 24 * afterActionRate2 - 24 * beforeActionRate;
        final double day4Savings = 24 * afterActionRate - 24 * afterActionRate2;

        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(0).investments(0)
                .timestamp(date(2022, 6, 3)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(dbOid).savings(day4Savings).investments(day4Investment)
                .timestamp(date(2022, 6, 4)).build());

        List<SavingsValues> result = calculator.calculate(dbOid, records, actionSpecs,
                getTimestamp(date(2022, 6, 1)), date(2022, 6, 4));

        validateResults(result, expectedResults);
    }

    /**
     * Scale volume by changing storage amount, IOPS and IO Throughput. (AWS GP3)
     * This volume originally has storage of 14GB, IOPS of 4000 and IO Throughput of 150MBPS.
     *
     * <p>Action 1: June 1 10am: Change disk commodities: IOPS 1000 to 4000; IO_Throughput 150MBps to 200MBps, storage amount 14GB to 30GB.
     * Action 2: June 3 1pm: Change disk commodities: IOPS 4000 to 2000.
     */
    @Test
    public void testAwsGp3Scale() {
        when(storagePriceStructure.getEndRangeInFreePriceTier(anyLong(), anyLong(), anyLong(), anyInt()))
                .thenAnswer(invocation -> {
                    int commType = invocation.getArgumentAt(3, Integer.class);
                    if (commType == CommodityType.IO_THROUGHPUT_VALUE) {
                        return 150;
                    } else if (commType == CommodityType.STORAGE_ACCESS_VALUE) {
                        return 3000;
                    }
                    return 0;
                });
        final long tierA = 1212121212L;
        final double storageAccessRate = 0.000068;
        final double ioThroughputRate = 0.00047;
        final double storageAmountRate = 0.000163159;
        Set<BillingRecord> records = new HashSet<>();
        records.add(createVolumeBillRecord(date(2022, 6, 1), 0 * 10 + 1000 * 14, (0 * 10 + 1000 * 14) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 0 * 10 + 50 * 14, (0 * 10 + 50 * 14) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 1), 14 * 10 + 30 * 14, (14 * 10 + 30 * 14) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 1000 * 24, (1000 * 24) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 50 * 24, (50 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 2), 30 * 24, (30 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 1000 * 13 + 0 * 11, (1000 * 13 + 0 * 11) * storageAccessRate, tierA, CommodityType.STORAGE_ACCESS_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 50 * 24, (50 * 24) * ioThroughputRate, tierA, CommodityType.IO_THROUGHPUT_VALUE));
        records.add(createVolumeBillRecord(date(2022, 6, 3), 30 * 24, (30 * 24) * storageAmountRate, tierA, CommodityType.STORAGE_AMOUNT_VALUE));

        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_AMOUNT, 14 * 1024, 30 * 1024));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 1000, 4000));
        resizeInfoList.add(ScenarioGenerator.createResizeInfo(CommodityType.IO_THROUGHPUT, 150 * 1024, 200 * 1024));
        double sourceOnDemandRate = (0 * storageAccessRate + 0 * ioThroughputRate + 14 * storageAmountRate); // cost per hour
        double destOnDemandRate = (1000 * storageAccessRate + 50 * ioThroughputRate + 30 * storageAmountRate); // cost per hour
        NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(changeWindowComparator);
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 1, 10, 0),
                sourceOnDemandRate, destOnDemandRate, tierA, tierA,
                null, LivenessState.SUPERSEDED, resizeInfoList));

        List<ResizeInfo> resizeInfoList2 = new ArrayList<>();
        resizeInfoList2.add(ScenarioGenerator.createResizeInfo(CommodityType.STORAGE_ACCESS, 4000, 2000));
        double sourceOnDemandRate2 = destOnDemandRate; // cost per hour
        double destOnDemandRate2 = (0 * storageAccessRate + 50 * ioThroughputRate + 30 * storageAmountRate); // cost per hour
        actionSpecs.add(ScenarioGenerator.createVolumeActionChangeWindow(volumeOid,
                LocalDateTime.of(2022, 6, 3, 13, 0),
                sourceOnDemandRate2, destOnDemandRate2, tierA, tierA,
                null, LivenessState.LIVE, resizeInfoList2));

        double day1Investment = destOnDemandRate * 14 - sourceOnDemandRate * 14;
        double day2Investment = destOnDemandRate * 24 - sourceOnDemandRate * 24;
        // On June 3, value of sourceOnDemandRate is the low watermark.
        double day3Investment = (destOnDemandRate - sourceOnDemandRate) * 13 + (destOnDemandRate2 - sourceOnDemandRate) * 11;
        // On June 3, value of sourceOnDemandRate2 is the high watermark.
        double day3Savings = (sourceOnDemandRate2 - destOnDemandRate2) * 11;
        List<SavingsValues> expectedResults = new ArrayList<>();
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day1Investment)
                .timestamp(date(2022, 6, 1)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(0).investments(day2Investment)
                .timestamp(date(2022, 6, 2)).build());
        expectedResults.add(new SavingsValues.Builder().entityOid(volumeOid).savings(day3Savings).investments(day3Investment)
                .timestamp(date(2022, 6, 3)).build());
        List<SavingsValues> result = calculator.calculate(volumeOid, records, actionSpecs,
                getTimestamp(date(2022, 5, 31)), date(2022, 6, 4));
        validateResults(result, expectedResults);
    }
}
