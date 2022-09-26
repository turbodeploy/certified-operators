package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for AzureMCAMeterSerializerStage.
 */
public class MCAMeterDeserializerStageTest {
    private static final String COMPUTE_METER = "1da53aa5-3176-4fd8-9c35-61ea9c18437a";
    private static final String P30_METER = "6ecded8f-3f77-48d7-9fbe-001df000546f";
    private static final String BLOB_METER = "3375c48f-ff4c-4931-b71b-51b5545b586f";
    private static final String NORMAL_PLAN = "Azure plan";
    private static final String DEVTEST_PLAN = "Azure plan for DevTest";


    /**
     * Test the happy path for reading several CSV files contained in a ZIP file.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(MCAMeterDeserializerStageTest.class.getClassLoader()
                .getResource("tinysample.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);
        List<String> meterStrings;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<AzureMeter>> pipeline = makePipeline(context);

            meterStrings = pipeline.run(metersTestFile)
                    .map(Objects::toString).collect(Collectors.toList());

            assertEquals(10, meterStrings.size());
        }

        List<String> expectedMeters = ImmutableList.of(
                COMPUTE_METER + " " + DEVTEST_PLAN + " $0.00520000 / 1 Hour @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                COMPUTE_METER + " " + NORMAL_PLAN + " $0.00520000 / 1 Hour @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                P30_METER + " " + DEVTEST_PLAN + " $135.170 / 1/Month @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                P30_METER + " " + NORMAL_PLAN + " $135.170 / 1/Month @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + DEVTEST_PLAN + " $0.0600000 / 1 GB/Month @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + NORMAL_PLAN + " $0.0600000 / 1 GB/Month @ >= 0.00000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + DEVTEST_PLAN + " $0.0500000 / 1 GB/Month @ >= 10.0000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + NORMAL_PLAN + " $0.0500000 / 1 GB/Month @ >= 10.0000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + DEVTEST_PLAN + " $0.0400000 / 1 GB/Month @ >= 100.000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z",
                BLOB_METER + " " + NORMAL_PLAN + " $0.0400000 / 1 GB/Month @ >= 100.000 effective from 2022-07-01T00:00Z to 2022-08-01T00:00Z"
        );

        for (int i = 0; i < meterStrings.size(); i++) {
            assertEquals("entry #" + i, expectedMeters.get(i), meterStrings.get(i));
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.DESERIALIZE_CSV);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Processed 10 meters of which 0 (0.00000%) had errors",
            status.getStatusShortExplanation());
    }

    @Nonnull
    private PricingPipeline<Path, Stream<AzureMeter>> makePipeline(@Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
            PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
                .finalStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV)));
    }
}
