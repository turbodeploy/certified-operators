package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for MeterResolverStage.
 */
public class MeterResolverStageTest {
    private static final String COMPUTE_METER = "1da53aa5-3176-4fd8-9c35-61ea9c18437a";
    private static final String P30_METER = "6ecded8f-3f77-48d7-9fbe-001df000546f";
    private static final String BLOB_METER = "3375c48f-ff4c-4931-b71b-51b5545b586f";
    private static final String NORMAL_PLAN = "Azure plan";
    private static final String DEVTEST_PLAN = "Azure plan for DevTest";

    /**
     * Test the happy path for resolving several CSV files contained in a ZIP file.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("resolvertest.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        Collection<ResolvedMeter> result;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Collection<ResolvedMeter>> pipeline = makePipeline(context, false);

            result = pipeline.run(metersTestFile);

            assertEquals(3, result.size());
        }

        Map<MeterType, List<ResolvedMeter>> resolvedByType = result.stream().collect(Collectors.toMap(
            r -> r.getDescriptor().getType(), r -> Lists.newArrayList(r),
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                }
        ));

        assertEquals(2, resolvedByType.size());

        // Test resolved pricing for an instance type. It has both normal and DevTest
        // pricing. There are no volume discounts / tiered pricing.

        List<ResolvedMeter> instanceTypes = resolvedByType.get(MeterType.VMProfile);
        assertEquals(1, instanceTypes.size());
        ResolvedMeter instanceType = instanceTypes.get(0);
        assertEquals("Linux", instanceType.getDescriptor().getSubType());
        assertEquals(ImmutableList.of("eastus"), instanceType.getDescriptor().getRegions());
        assertEquals(ImmutableList.of("Standard_B1ls"), instanceType.getDescriptor().getSkus());
        assertEquals(2, instanceType.getPricing().size());
        TreeMap<Double, AzureMeter> normalInstanceType = instanceType.getPricing().get(NORMAL_PLAN);
        TreeMap<Double, AzureMeter> devTestInstanceType = instanceType.getPricing().get(DEVTEST_PLAN);
        assertNotNull(normalInstanceType);
        assertNotNull(devTestInstanceType);
        assertEquals(1, normalInstanceType.size());
        assertEquals(1, devTestInstanceType.size());
        assertEquals(0.0D, normalInstanceType.firstEntry().getKey(), 0.0D);
        assertEquals(0.0D, devTestInstanceType.firstEntry().getKey(), 0.0D);
        AzureMeter normalMeter = normalInstanceType.firstEntry().getValue();
        AzureMeter devTestMeter = devTestInstanceType.firstEntry().getValue();
        assertEquals(0.0D, normalMeter.getTierMinimumUnits(), 0.0);
        assertEquals(0.0D, devTestMeter.getTierMinimumUnits(), 0.0);
        assertEquals("1 Hour", normalMeter.getUnitOfMeasure());
        assertEquals("1 Hour", devTestMeter.getUnitOfMeasure());
        assertEquals(NORMAL_PLAN, normalMeter.getPlanName());
        assertEquals(DEVTEST_PLAN, devTestMeter.getPlanName());
        assertEquals(0.0052D, normalMeter.getUnitPrice(), 0.00001);
        assertEquals(0.0052D, devTestMeter.getUnitPrice(), 0.00001);

        // Two different kinds of storage are resolved.

        List<ResolvedMeter> storages = resolvedByType.get(MeterType.Storage);
        assertEquals(2, storages.size());

        // P30 managed storage with Normal pricing only, no DevTest, and no
        // tiered pricing.

        List<ResolvedMeter> p30s = storages.stream()
            .filter(r -> r.getDescriptor().getSkus().contains("UNMANAGED_PREMIUM"))
            .collect(Collectors.toList());

        assertEquals(1, p30s.size());
        ResolvedMeter p30 = p30s.get(0);
        assertEquals("P30", p30.getDescriptor().getSubType());
        assertEquals(ImmutableList.of("eastus"), p30.getDescriptor().getRegions());
        assertEquals(ImmutableList.of("UNMANAGED_PREMIUM"), p30.getDescriptor().getSkus());
        assertEquals(1, p30.getPricing().size());
        TreeMap<Double, AzureMeter> normalP30 = p30.getPricing().get(NORMAL_PLAN);
        assertNotNull(normalP30);
        assertEquals(1, normalP30.size());
        assertEquals(0.0D, normalP30.firstEntry().getKey(), 0.0D);
        normalMeter = normalP30.firstEntry().getValue();
        assertEquals(0.0D, normalMeter.getTierMinimumUnits(), 0.0);
        assertEquals("1/Month", normalMeter.getUnitOfMeasure());
        assertEquals(NORMAL_PLAN, normalMeter.getPlanName());
        assertEquals(135.17D, normalMeter.getUnitPrice(), 0.00001);

        // Unmanaged Standard LRS has 3 tiers of pricing

        List<ResolvedMeter> unmanagedLrses = storages.stream()
            .filter(r -> r.getDescriptor().getSkus().contains("UNMANAGED_STANDARD_LRS"))
            .collect(Collectors.toList());
        assertEquals(1, unmanagedLrses.size());
        ResolvedMeter unmanagedLrs = unmanagedLrses.get(0);
        // We just want "many", as this may grow or shrink over time as regions are added
        // or perhaps removed. No need to be too specific here and introduce test failures
        // when we update the regions list.
        assertTrue(unmanagedLrs.getDescriptor().getRegions().size() > 10);

        assertEquals(2, instanceType.getPricing().size());
        TreeMap<Double, AzureMeter> normalULRS = unmanagedLrs.getPricing().get(NORMAL_PLAN);
        TreeMap<Double, AzureMeter> devTestULRS = unmanagedLrs.getPricing().get(DEVTEST_PLAN);
        assertNotNull(normalULRS);
        assertNotNull(devTestULRS);
        assertEquals(3, normalULRS.size());
        assertEquals(3, devTestULRS.size());

        // Check the first price tier, 0..9 units

        Entry<Double, AzureMeter> normalEntry = normalULRS.firstEntry();
        Entry<Double, AzureMeter> devTestEntry = devTestULRS.firstEntry();

        assertEquals(0.0D, normalEntry.getKey(), 0.0D);
        assertEquals(0.0D, devTestEntry.getKey(), 0.0D);
        normalMeter = normalEntry.getValue();
        devTestMeter = devTestEntry.getValue();
        assertEquals(0.0D, normalMeter.getTierMinimumUnits(), 0.0);
        assertEquals(0.0D, devTestMeter.getTierMinimumUnits(), 0.0);
        assertEquals("1 GB/Month", normalMeter.getUnitOfMeasure());
        assertEquals("1 GB/Month", devTestMeter.getUnitOfMeasure());
        assertEquals(NORMAL_PLAN, normalMeter.getPlanName());
        assertEquals(DEVTEST_PLAN, devTestMeter.getPlanName());
        assertEquals(0.045D, normalMeter.getUnitPrice(), 0.00001);
        assertEquals(0.045D, devTestMeter.getUnitPrice(), 0.00001);

        // Next price tier, 10..99 units

        normalEntry = normalULRS.higherEntry(0.0D);
        devTestEntry = devTestULRS.higherEntry(0.0D);

        assertEquals(10.0D, normalEntry.getKey(), 0.0D);
        assertEquals(10.0D, devTestEntry.getKey(), 0.0D);
        normalMeter = normalEntry.getValue();
        devTestMeter = devTestEntry.getValue();
        assertEquals(10.0D, normalMeter.getTierMinimumUnits(), 0.0);
        assertEquals(10.0D, devTestMeter.getTierMinimumUnits(), 0.0);
        assertEquals("1 GB/Month", normalMeter.getUnitOfMeasure());
        assertEquals("1 GB/Month", devTestMeter.getUnitOfMeasure());
        assertEquals(NORMAL_PLAN, normalMeter.getPlanName());
        assertEquals(DEVTEST_PLAN, devTestMeter.getPlanName());
        assertEquals(0.04D, normalMeter.getUnitPrice(), 0.00001);
        assertEquals(0.04D, devTestMeter.getUnitPrice(), 0.00001);

        // Highest price tier, 100+ units

        normalEntry = normalULRS.higherEntry(10.0D);
        devTestEntry = devTestULRS.higherEntry(10.0D);

        assertEquals(100.0D, normalEntry.getKey(), 0.0D);
        assertEquals(100.0D, devTestEntry.getKey(), 0.0D);
        normalMeter = normalEntry.getValue();
        devTestMeter = devTestEntry.getValue();
        assertEquals(100.0D, normalMeter.getTierMinimumUnits(), 0.0);
        assertEquals(100.0D, devTestMeter.getTierMinimumUnits(), 0.0);
        assertEquals("1 GB/Month", normalMeter.getUnitOfMeasure());
        assertEquals("1 GB/Month", devTestMeter.getUnitOfMeasure());
        assertEquals(NORMAL_PLAN, normalMeter.getPlanName());
        assertEquals(DEVTEST_PLAN, devTestMeter.getPlanName());
        assertEquals(0.035D, normalMeter.getUnitPrice(), 0.00001);
        assertEquals(0.035D, devTestMeter.getUnitPrice(), 0.00001);

        // Check stage status

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.RESOLVE_METERS);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("9 (81.8%) meters of interest (3 distinct IDs), 2 (18.2%) ignored",
            status.getStatusShortExplanation());
    }

    /**
     * Test failure handling if there is an error in one of the underlying streams. Because this
     * stage consumes the stream passed in, any errors in map or filter functions on the stream
     * will be caught in this stage, so although nothing in this stage itself can fail, it
     * can fail indirectly due to errors in earlier stages.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testIOFailure() throws Exception {
        Path metersZip = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("resolvertest.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Collection<ResolvedMeter>> pipeline = makePipeline(context, true);

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(metersZip);
            });
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.RESOLVE_METERS);
        assertEquals(StageStatus.FAILURE, status.getStatus());
    }

    /**
     * If the stage resolves no meters, it should fail since something is very wrong,
     * and we don't want the discovery to succeed and replace the cost component's existing
     * pricing data with nothing.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testNoMeters() throws Exception {
        Path metersZip = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("headeronly.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Collection<ResolvedMeter>> pipeline = makePipeline(context, false);

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(metersZip);
            });
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.RESOLVE_METERS);
        assertEquals(StageStatus.FAILURE, status.getStatus());
    }

    @Nonnull
    private PricingPipeline<Path, Collection<ResolvedMeter>> makePipeline(
            @Nonnull PricingPipelineContext context, boolean addBrokenStage) {
        PipelineDefinitionBuilder builder = PipelineDefinition.<MockAccount, Path,
            PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
            .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
            .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES));

        if (addBrokenStage) {
            builder.addStage(new BrokenInputStreamStage());
        }

        builder
            .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
            .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
            .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV));

        return new PricingPipeline<>(builder.finalStage(MeterResolverStage.newBuilder()
            .addResolver(new MeterDescriptorsFileResolver())
            .build(MockPricingProbeStage.RESOLVE_METERS)));
    }
}
