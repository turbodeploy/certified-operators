package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.BOMAwareReadersStage;
import com.vmturbo.mediation.azure.pricing.stages.ChainedCSVParserStage;
import com.vmturbo.mediation.azure.pricing.stages.MCAMeterDeserializerStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStageTest;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for DBStorageMeterProcessingStageTest.
 */
public class DBStorageMeterProcessingStageTest {

    /**
     * Test success.
     *
     * @throws PipelineStageException when there is an exception.
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("DBStorage.zip")
                .getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(
                        MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipelineForDBStorage(context);

            pipeline.run(metersTestFile);
        }

        ProbeStageDetails status = tracker.getStageDetails(
                MockPricingProbeStage.DB_STORAGE_METER_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals(
                "DB Storage prices added for [PlanId, Region]: {azure plan=3, azure plan for devtest=4}",
                status.getStatusShortExplanation());
    }

    /**
     * Perform tests for the abstractDbTierMeterProcessor and map populated by
     * DBStorageMeterProcessingStage.
     *
     * @throws Exception when there is an exception.
     */
    @Test
    public void testAbstractDbTierMeterClassAndContext() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("DBStorage.zip")
                .getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(
                        MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipelineForAbstract(context);

            pipeline.run(metersTestFile);
        }
    }

    private PricingPipeline<Path, PricingWorkspace> makePipelineForDBStorage(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(
                PipelineDefinition.<MockAccount, Path, PricingPipelineContext<MockPricingProbeStage>>newBuilder(
                                context)
                        .addStage(new SelectZipEntriesStage("*.csv",
                                MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                        .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                        .addStage(
                                new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                        .addStage(new ChainedCSVParserStage<>(
                                MockPricingProbeStage.CHAINED_CSV_PARSERS))
                        .addStage(new MCAMeterDeserializerStage(
                                MockPricingProbeStage.DESERIALIZE_CSV))
                        .addStage(MeterResolverStage.newBuilder()
                                .addResolver(new MeterDescriptorsFileResolver())
                                .build(MockPricingProbeStage.RESOLVE_METERS))
                        .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
                        .finalStage(new DBStorageMeterProcessingStage(
                                MockPricingProbeStage.DB_STORAGE_METER_PROCESSOR)));
    }

    private PricingPipeline<Path, PricingWorkspace> makePipelineForAbstract(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(
                PipelineDefinition.<MockAccount, Path, PricingPipelineContext<MockPricingProbeStage>>newBuilder(
                                context)
                        .addStage(new SelectZipEntriesStage("*.csv",
                                MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                        .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                        .addStage(
                                new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                        .addStage(new ChainedCSVParserStage<>(
                                MockPricingProbeStage.CHAINED_CSV_PARSERS))
                        .addStage(new MCAMeterDeserializerStage(
                                MockPricingProbeStage.DESERIALIZE_CSV))
                        .addStage(MeterResolverStage.newBuilder()
                                .addResolver(new MeterDescriptorsFileResolver())
                                .build(MockPricingProbeStage.RESOLVE_METERS))
                        .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
                        .addStage(new DBStorageMeterProcessingStage(
                                MockPricingProbeStage.DB_STORAGE_METER_PROCESSOR))
                        .finalStage(new FakeDBTierMeterProcessingStage(MockPricingProbeStage.ABSTRACT_DB_METER_PROCESSOR)));
    }
}
