package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
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
 * Tests for ChainedCSVParserStage.
 */
public class ChainedCSVParserStageTest {
    /**
     * Test the happy path for reading several CSV files contained in a ZIP file.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testSuccess() throws Exception {
        Path bomCsvs = Paths.get(ChainedCSVParserStageTest.class.getClassLoader()
                .getResource("bomcsvs.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<CSVRecord>> pipeline = makePipeline(context, false);

            List<CSVRecord> records = pipeline.run(bomCsvs).collect(Collectors.toList());
            assertEquals(6, records.size());

            assertEquals("nobom.csv,utf16be.csv,utf16le.csv,utf32be.csv,utf32le.csv,utf8.csv",
                    records.stream().map(r -> r.get("FILENAME")).collect(Collectors.joining(",")));

            assertEquals("NONE,UTF-16 BE,UTF-16 LE,UTF-32 BE,UTF-32 LE,UTF-8",
                    records.stream().map(r -> r.get("CHARSET")).collect(Collectors.joining(",")));
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.CHAINED_CSV_PARSERS);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Created CSV Parsers for 6 files", status.getStatusShortExplanation());
    }

    /**
     * Test failure handling if there is an error in one of the underlying readers.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testFailure() throws Exception {
        Path bomCsvs = Paths.get(ChainedCSVParserStageTest.class.getClassLoader()
                .getResource("bomcsvs.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<CSVRecord>> pipeline = makePipeline(context, true);

            RuntimeException ex = assertThrows(RuntimeException.class, () -> {
                pipeline.run(bomCsvs).collect(Collectors.toList());
            });
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.CHAINED_CSV_PARSERS);
        assertEquals(StageStatus.FAILURE, status.getStatus());
        assertEquals("Failed while trying to initialize CSV Parsing",
                status.getStatusShortExplanation());
    }

    /**
     * This stage inserts a broken InputStream, in order to introduce an error for testing.
     */
    private static class BrokenReaderStage extends PricingPipeline.Stage<Stream<Reader>,
            Stream<Reader>, MockPricingProbeStage> {
        /**
         * Construct a new BrokenReaderStage.
         */
        BrokenReaderStage() {
            super(MockPricingProbeStage.BROKEN_INPUT_STAGE);
        }

        @NotNull
        @Override
        protected StageResult<Stream<Reader>> executeStage(
                @NotNull Stream<Reader> readerStream) {
            List<Reader> streams = readerStream.collect(Collectors.toList());
            streams.add(1, new BrokenReader());

            return StageResult.withResult(streams.stream()).andStatus(Status.success("OK"));
        }

        /**
         * A Reader that always errors out.
         */
        private static class BrokenReader extends Reader {
            @Override
            public int read(@NotNull char[] cbuf, int off, int len) throws IOException {
                throw new IOException("This reader is broken");
            }

            @Override
            public void close() throws IOException {
            }
        }
    }

    @Nonnull
    private PricingPipeline<Path, Stream<CSVRecord>> makePipeline(@Nonnull PricingPipelineContext context,
            boolean addBrokenStage) {
        PipelineDefinitionBuilder builder = PipelineDefinition.<MockAccount, Path,
            PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS));

        if (addBrokenStage) {
            builder.addStage(new BrokenReaderStage());
        }

        return new PricingPipeline<>(builder
            .finalStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS)));
    }
}
