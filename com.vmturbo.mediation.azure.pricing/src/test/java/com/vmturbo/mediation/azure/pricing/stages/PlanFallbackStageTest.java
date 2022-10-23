package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.mediation.azure.pricing.controller.MCAPricingDiscoveryController;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests for PlanFallbackStage.
 */
public class PlanFallbackStageTest {
    private static final String NORMAL_PLAN = "Azure plan";
    private static final String DEVTEST_PLAN = "Azure plan for DevTest";

    /**
     * Test that the sample data has some DevTest pricing, but not as much pricing as
     * the regular plan.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testNoFallback() throws Exception {
        testScenario("resolvertest.zip", false, 3, 2);
    }

    /**
     * Test that the same sample data, when the plan fallback is applied, now produces as much
     * pricing for DevTest as regular.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testWithFallback() throws Exception {
        testScenario("resolvertest.zip", true, 3, 3);
    }

    /**
     * Test that when the plan fallback is applied, but no DevTest data was present at all,
     * none is added.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testNoDevTest() throws Exception {
        testScenario("nodevtest.zip", true, 3, 0);
    }

    /**
     * Test a scenario with the given parameters.
     *
     * @param filename the filename of the pricesheet to test against
     * @param addFallbackStage whether to add the fallback stage
     * @param expectedRegularPrices how many regular (non-DevTest) prices are expected
     * @param expectedDevTestPrices how many DevTest prices are expected
     * @throws Exception if the test fails
     */
    public void testScenario(String filename, boolean addFallbackStage, int expectedRegularPrices,
            int expectedDevTestPrices) throws Exception {
        Path metersTestFile = Paths.get(PlanFallbackStageTest.class.getClassLoader()
            .getResource(filename).getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        Collection<ResolvedMeter> result;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, Collection<ResolvedMeter>> pipeline = makePipeline(context,
                    addFallbackStage);

            result = pipeline.run(metersTestFile);

            assertEquals(3, result.size());
        }

        assertEquals(expectedRegularPrices, result.stream()
            .map(ResolvedMeter::getPricing)
            .filter(p -> p.containsKey(NORMAL_PLAN))
            .count());

        assertEquals(expectedDevTestPrices, result.stream()
            .map(ResolvedMeter::getPricing)
            .filter(p -> p.containsKey(DEVTEST_PLAN))
            .count());
    }

    @Nonnull
    private PricingPipeline<Path, Collection<ResolvedMeter>> makePipeline(
            @Nonnull PricingPipelineContext context, boolean addFallbackStage) {
        PipelineDefinitionBuilder builder = PipelineDefinition.<MockAccount, Path,
            PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
            .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER,
                    () -> (IPropertyProvider)IProbePropertySpec::getDefaultValue)
            .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
            .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
            .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
            .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
            .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV));

        if (addFallbackStage) {
            return new PricingPipeline<>(builder
                .addStage(MeterResolverStage.newBuilder()
                    .addResolver(new MeterDescriptorsFileResolver())
                    .build(MockPricingProbeStage.RESOLVE_METERS))
                .finalStage(new PlanFallbackStage(MockPricingProbeStage.PLAN_FALLBACK,
                    MCAPricingDiscoveryController.MCA_PLAN_FALLBACK_MAP)));
        } else {
            return new PricingPipeline<>(builder
                .finalStage(MeterResolverStage.newBuilder()
                    .addResolver(new MeterDescriptorsFileResolver())
                    .build(MockPricingProbeStage.RESOLVE_METERS)));
        }
    }
}
