package com.vmturbo.mediation.azure.pricing.stages;

import java.io.IOException;
import java.io.Reader;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Stage for taking a set of Readers on CSV files and returning a single stream of CSV
 * records from all the files.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class ChainedCSVParserStage<E extends ProbeStageEnum>
        extends Stage<Stream<Reader>, Stream<CSVRecord>, E>
        implements AutoCloseable {
    private static final CSVFormat CSV_FORMAT = CSVFormat.EXCEL.builder()
        .setHeader().setSkipHeaderRecord(true).build();
    private Exception exceptionFromOpen = null;
    private String failedFilename = null;
    private int opened = 0;

    /**
     * Create a chained CSV reader stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public ChainedCSVParserStage(@Nonnull E probeStage) {
        super(probeStage);
    }

    @NotNull
    @Override
    protected StageResult<Stream<CSVRecord>> executeStage(@NotNull Stream<Reader> streams) {
        // As the pipeline stage returns a stream immediately where exceptions may
        // happen later, the pipeline stage returns immediate success, but the probe
        // stage will report later on any failures.

        getContext().autoClose(this);

        return StageResult.withResult(streams.flatMap(this::createCSVParser))
                .andStatus(Status.success("Created CSV Parsers"));
    }

    @Nonnull
    private Stream<CSVRecord> createCSVParser(@Nonnull Reader reader) {
        try {
            CSVParser parser = CSV_FORMAT.parse(reader);

            getContext().autoClose(parser);
            opened++;

            return parser.stream();
        } catch (IOException ex) {
            exceptionFromOpen = ex;
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        if (exceptionFromOpen == null) {
            getStageInfo().ok(String.format("Created CSV Parsers for %d files", opened));
        } else {
            getStageInfo().fail(exceptionFromOpen)
                .summary("Failed while trying to initialize CSV Parsing");
        }
    }
}