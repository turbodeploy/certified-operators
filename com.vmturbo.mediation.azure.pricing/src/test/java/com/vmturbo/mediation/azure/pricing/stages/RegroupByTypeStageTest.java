package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;

/**
 * Tests for RegroupByTypeStage.
 */
public class RegroupByTypeStageTest {
    /**
     * Test the success path generates correct data. This stage should not be able to fail.
     *
     * @throws Exception if the test fails
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("resolvertest.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // Check the grouping by type

        Map<MeterType, List<ResolvedMeter>> metersByType = workspace.getResolvedMeterByMeterType();

        // There should be entries for storage and VMProfile

        assertEquals(ImmutableSet.of(MeterType.Storage, MeterType.VMProfile),
                metersByType.keySet());

        // One compute tier

        List<ResolvedMeter> vmprofiles = metersByType.get(MeterType.VMProfile);
        assertEquals(1, vmprofiles.size());
        assertEquals("Standard_B1ls", vmprofiles.get(0).getDescriptor().getSkus().get(0));

        // Two storages

        List<ResolvedMeter> storages = metersByType.get(MeterType.Storage);
        assertEquals(2, storages.size());
        String storagesString = storages.stream()
            .map(ResolvedMeter::getDescriptor)
            .map(AzureMeterDescriptor::getSkus)
            .map(skus -> skus.get(0))
            .sorted()
            .collect(Collectors.joining(","));
        assertEquals("UNMANAGED_PREMIUM,UNMANAGED_STANDARD_LRS", storagesString);
    }

    @Nonnull
    private PricingPipeline<Path, PricingWorkspace> makePipeline(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
                .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV))
                .addStage(MeterResolverStage.newBuilder()
                    .addResolver(new MeterDescriptorsFileResolver())
                    .build(MockPricingProbeStage.RESOLVE_METERS))
                .finalStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS)));
    }
}
