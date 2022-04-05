package com.vmturbo.cost.component.savings.calculator;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.cost.component.savings.BillingChangeRecord;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * Test cases for bill-based savings calculator.
 */
public class CalculatorTest {

    private final long vmOid = 1234L;
    private final AtomicLong recordId = new AtomicLong(0);

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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 1, targetProviderId));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 26), 24, 48, targetProviderId));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 26), 12, 24, targetProviderId));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 36, targetProviderId));
        records.add(createVMBillRecord(date(2022, 3, 26), 12, 24, targetProviderId));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 3, targetProviderId));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 12, targetProviderId1));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 54, targetProviderId2));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 24, 12, 0), 1, targetProviderId1));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderId2));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 6, 12, targetProviderId1));
        records.add(createVMBillRecord(date(2022, 3, 25), 18, 18, targetProviderId2));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 24, 12, 0), 3, targetProviderId1));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderId2));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        Set<BillingChangeRecord> records = new HashSet<>();
        records.add(createVMBillRecord(date(2022, 3, 25), 9, 27, targetProviderTierC));
        records.add(createVMBillRecord(date(2022, 3, 25), 15, 30, targetProviderTierB));
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 24, 12, 0), 1, targetProviderTierB));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 6, 0), 2, targetProviderTierC));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 25, 15, 0), 3, targetProviderTierB));
        List<SavingsValues> result = Calculator.calculate(vmOid, records, actionSpecs);
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
        NavigableSet<ActionSpec> actionSpecs =
                new TreeSet<>(Comparator.comparing(a -> a.getExecutionStep().getCompletionTime()));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 20, 12, 0), 2, 10L));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 21, 12, 0), 6, 20L));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 22, 12, 0), 3, 30L));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 23, 12, 0), 4, 40L));
        actionSpecs.add(createActionSpec(LocalDateTime.of(2022, 3, 24, 12, 0), 1, 50L));
        WatermarkGraph graph = new WatermarkGraph(actionSpecs);

        LocalDateTime datetime = date(2022, 3, 19, 13, 0);

        // Return null if timestamp is before the first action.
        Watermark watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNull(watermark);

        datetime = date(2022, 3, 20, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(2, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 21, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 22, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 23, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(2, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 24, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(1, watermark.getLowWatermark(), 0);

        // Remove the first action.
        actionSpecs.remove(actionSpecs.first());
        graph = new WatermarkGraph(actionSpecs);

        datetime = date(2022, 3, 21, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(6, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 22, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(3, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 23, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
        Assert.assertNotNull(watermark);
        Assert.assertEquals(6, watermark.getHighWatermark(), 0);
        Assert.assertEquals(3, watermark.getLowWatermark(), 0);

        datetime = date(2022, 3, 24, 13, 0);
        watermark = graph.getWatermark(getTimestamp(datetime));
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

    private BillingChangeRecord createVMBillRecord(LocalDateTime dateTime, double usageAmount,
            double cost, long providerId) {
        return new BillingChangeRecord.Builder()
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

    private ActionSpec createActionSpec(LocalDateTime actionTime, double sourceOnDemandRate, long destProviderId) {
        return ActionSpec.newBuilder()
                .setExecutionStep(ExecutionStep.newBuilder()
                        .setStatus(Status.SUCCESS)
                        .setCompletionTime(actionTime.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .build())
                .setRecommendation(Action.newBuilder()
                        .setInfo(ActionInfo.newBuilder()
                                .setScale(Scale.newBuilder()
                                        .addChanges(ChangeProvider.newBuilder()
                                                .setDestination(ActionEntity.newBuilder()
                                                        .setId(destProviderId)
                                                        .setType(EntityType.COMPUTE_TIER.getValue())
                                                        .build())
                                                .build())
                                        .setCloudSavingsDetails(CloudSavingsDetails.newBuilder()
                                                .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                                        .setOnDemandRate(CurrencyAmount.newBuilder()
                                                                .setAmount(sourceOnDemandRate)
                                                                .build())
                                                        .build())
                                                .build())
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(vmOid)
                                                .setType(EntityType.VIRTUAL_MACHINE.getValue())
                                                .build())
                                        .build())
                                .build())
                        .setId(67676767676L)
                        .setDeprecatedImportance(1d)
                        .setExplanation(Explanation.newBuilder()
                                .setScale(ScaleExplanation.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
