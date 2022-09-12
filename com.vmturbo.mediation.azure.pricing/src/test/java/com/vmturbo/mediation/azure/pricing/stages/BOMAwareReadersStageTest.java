package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Tests for BOMAwareReadersStage.
 */
public class BOMAwareReadersStageTest {
    /**
     * Test the happy path of converting InputStreams to Readers, for files with
     * all different kinds of ByteOrderMarks.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testSuccess() throws Exception {
        Path bomCsvs = Paths.get(BOMAwareReadersStageTest.class.getClassLoader()
                .getResource("bomcsvs.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<Reader>> pipeline = makePipeline(context, false);

            List<Reader> readers = pipeline.run(bomCsvs).collect(Collectors.toList());
            assertEquals(6, readers.size());

            List<String> strings = readers.stream().map(reader -> {
                try {
                    return IOUtils.toString(reader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());

            assertEquals("FILENAME,CHARSET\nnobom.csv,NONE\n", strings.get(0));
            assertEquals("FILENAME,CHARSET\nutf16be.csv,UTF-16 BE\n", strings.get(1));
            assertEquals("FILENAME,CHARSET\nutf16le.csv,UTF-16 LE\n", strings.get(2));
            assertEquals("FILENAME,CHARSET\nutf32be.csv,UTF-32 BE\n", strings.get(3));
            assertEquals("FILENAME,CHARSET\nutf32le.csv,UTF-32 LE\n", strings.get(4));
            assertEquals("FILENAME,CHARSET\nutf8.csv,UTF-8\n", strings.get(5));
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.OPEN_ZIP_ENTRIES);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Opened 6 files", status.getStatusShortExplanation());
    }

    /**
     * Test failure handling if there is an error in one of the underlying streams.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testFailure() throws Exception {
        Path bomCsvs = Paths.get(BOMAwareReadersStageTest.class.getClassLoader()
                .getResource("bomcsvs.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<Reader>> pipeline = makePipeline(context, true);

            RuntimeException ex = assertThrows(RuntimeException.class, () -> {
                pipeline.run(bomCsvs).collect(Collectors.toList());
            });
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.BOM_AWARE_READERS);
        assertEquals(StageStatus.FAILURE, status.getStatus());
        assertEquals("Failed while trying to convert stream to a reader",
            status.getStatusShortExplanation());
    }

    /**
     * This stage inserts an broken InputStream, in order to introduce an error for testing.
     */
    private static class BrokenInputStreamStage extends PricingPipeline.Stage<Stream<InputStream>,
            Stream<InputStream>, PricingPipelineContext<MockPricingProbeStage>> {
        @NotNull
        @Override
        protected StageResult<Stream<InputStream>> executeStage(
                @NotNull Stream<InputStream> inputStreamStream)
                throws PipelineStageException, InterruptedException {
            List<InputStream> streams = inputStreamStream.collect(Collectors.toList());
            streams.add(1, new BrokenInputStream());

            return StageResult.withResult(streams.stream()).andStatus(Status.success("OK"));
        }

        /**
         * An InputStream that always errors out.
         */
        private static class BrokenInputStream extends InputStream {
            @Override
            public int read() throws IOException {
                throw new IOException("This stream is broken");
            }
        }
    }

    @Nonnull
    private PricingPipeline<Path, Stream<Reader>> makePipeline(@Nonnull PricingPipelineContext context,
            boolean addBrokenStage) {
        PipelineDefinitionBuilder builder = PipelineDefinition.<MockAccount, Path,
            PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES));

        if (addBrokenStage) {
            builder.addStage(new BrokenInputStreamStage());
        }

        return new PricingPipeline<>(
            builder.finalStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS)));
    }
}
