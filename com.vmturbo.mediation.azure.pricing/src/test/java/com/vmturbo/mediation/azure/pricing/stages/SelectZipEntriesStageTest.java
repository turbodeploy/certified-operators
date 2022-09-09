package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for SelectZipEntriesStage.
 */
public class SelectZipEntriesStageTest {
    /**
     * Test that the stage fails if the input file cannot be opened as a zip file.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testNotAZipFile() throws Exception {
        Path notAZip = Paths.get(SelectZipEntriesStageTest.class.getClassLoader()
                .getResource("not-a-zip-file").getPath());

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<Path, List<ZipEntry>> pipeline = makePipeline(context);

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(notAZip);
            });

            ProbeStageDetails status = context.getStageTracker()
                .getStageDetails(MockPricingProbeStage.SELECT_ZIP_ENTRIES);

            assertEquals(StageStatus.FAILURE, status.getStatus());
        }
    }

    /**
     * Test for an empty zip file. The stage is expected to succeed, but with a warning.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testEmptyZipFile() throws Exception {
        Path emptyZip = Paths.get(SelectZipEntriesStageTest.class.getClassLoader()
                .getResource("empty.zip").getPath());

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<Path, List<ZipEntry>> pipeline = makePipeline(context);

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(emptyZip);
            });

            ProbeStageDetails status = context.getStageTracker()
                .getStageDetails(MockPricingProbeStage.SELECT_ZIP_ENTRIES);

            assertEquals(StageStatus.FAILURE, status.getStatus());
            assertEquals("Found no files (out of 0) matching *.csv",
                status.getStatusShortExplanation());
        }
    }

    /**
     * Test a zip file which is not empty, but contains no matching files.
     * This is expected to succeed but with a warning.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testNoMatches() throws Exception {
        Path noMatchesZip = Paths.get(SelectZipEntriesStageTest.class.getClassLoader()
                .getResource("nocsv.zip").getPath());

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<Path, List<ZipEntry>> pipeline = makePipeline(context);

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(noMatchesZip);
            });

            ProbeStageDetails status = context.getStageTracker()
                    .getStageDetails(MockPricingProbeStage.SELECT_ZIP_ENTRIES);

            assertEquals(StageStatus.FAILURE, status.getStatus());
            assertEquals("Found no files (out of 1) matching *.csv",
                    status.getStatusShortExplanation());
        }
    }

    /**
     * Test a zip file that has 3 files we care about, and one that we don't.
     * The files will be returned in sorted order.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void test3Matches() throws Exception {
        Path notAZip = Paths.get(SelectZipEntriesStageTest.class.getClassLoader()
                .getResource("threecsv.zip").getPath());

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<Path, List<ZipEntry>> pipeline = makePipeline(context);

            List<ZipEntry> entries = pipeline.run(notAZip);

            assertEquals(3, entries.size());
            List<String> fileNames = entries.stream().map(ZipEntry::getName)
                    .collect(Collectors.toList());
            assertEquals(ImmutableList.of("a.csv", "b.csv", "C.CSV"), fileNames);

            ProbeStageDetails status = context.getStageTracker()
                    .getStageDetails(MockPricingProbeStage.SELECT_ZIP_ENTRIES);

            assertEquals(StageStatus.SUCCESS, status.getStatus());
            assertEquals("Selected files matching *.csv (3 of 4 files)",
                    status.getStatusShortExplanation());
            assertEquals("Files selected for processing:\n"
                    + "a.csv (28 bytes)\n"
                    + "b.csv (31 bytes)\n"
                    + "C.CSV (27 bytes)", status.getStatusLongExplanation());
        }
    }

    @Nonnull
    private PricingPipeline<Path, List<ZipEntry>> makePipeline(PricingPipelineContext context) {
        PricingPipeline<Path, List<ZipEntry>> pipeline = new PricingPipeline<>(
                PipelineDefinition.<MockAccount, Path,
                                PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                        .finalStage(new SelectZipEntriesStage("*.csv",
                                MockPricingProbeStage.SELECT_ZIP_ENTRIES
                        )));
        return pipeline;
    }
}
