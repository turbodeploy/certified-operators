package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.PassthroughStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for OpenZipEntriesStage.
 */
public class OpenZipEntriesStageTest {
    /**
     * Test a zip file that has 3 files we care about, and one that we don't.
     * The files will be returned in sorted order.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testSuccess() throws Exception {
        Path threeCsvs = Paths.get(OpenZipEntriesStageTest.class.getClassLoader()
                .getResource("threecsv.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<InputStream>> pipeline = makePipeline(context, false);

            List<InputStream> streams = pipeline.run(threeCsvs).collect(Collectors.toList());
            assertEquals(3, streams.size());

            List<String> strings = streams.stream().map(stream -> {
                        try {
                            return IOUtils.toString(stream, "UTF-8");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                .collect(Collectors.toList());

            assertEquals("FILENAME,MESSAGE\nA.CSV,\"Hi\"\n", strings.get(0));
            assertEquals("FILENAME,MESSAGE\nB.CSV,\"WORLD\"\n", strings.get(1));
            assertEquals("FILENAME,MESSAGE\nC.CSV,\"!\"\n", strings.get(2));
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.OPEN_ZIP_ENTRIES);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Opened 3 files", status.getStatusShortExplanation());
    }

    /**
     * Test handling of failure to open a zip entry.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testOpenFailure() throws Exception {
        Path threeCsvs = Paths.get(OpenZipEntriesStageTest.class.getClassLoader()
                .getResource("threecsv.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<InputStream>> pipeline = makePipeline(context, true);

            RuntimeException ex = assertThrows(RuntimeException.class, () -> {
                 pipeline.run(threeCsvs).collect(Collectors.toList());
            });
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.OPEN_ZIP_ENTRIES);
        assertEquals(StageStatus.FAILURE, status.getStatus());
        assertEquals("Failed while trying to open no-such-file", status.getStatusShortExplanation());
    }

    @Nonnull
    private PricingPipeline<Path, Stream<InputStream>> makePipeline(PricingPipelineContext context,
            boolean brokenStage) {
        PipelineDefinitionBuilder builder = PipelineDefinition.<MockAccount, Path,
                                PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
            .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES));

        if (brokenStage) {
            builder.addStage(new BrokenEntryStage());
        }

        PricingPipeline<Path, Stream<InputStream>> pipeline = new PricingPipeline<>(builder
                .finalStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES
                        )));

        return pipeline;
    }

    /**
     * This stage inserts an entry that doesn't exist in the underlying zip file, in order
     * to introduce an error for testing.
     */
    private class BrokenEntryStage extends PassthroughStage<List<ZipEntry>> {
        @NotNull
        @Override
        public Status passthrough(List<ZipEntry> zipEntries) {
            zipEntries.add(1, new ZipEntry("no-such-file"));

            return Status.success("OK");
        }
    }
}
