package com.vmturbo.mediation.azure.pricing.stages;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker.StageInfo;

/**
 * Stage for opening InputStreams as Readers, with automatic BOM detection and character set
 * handling.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class BOMAwareReadersStage<E extends ProbeStageEnum>
        extends Stage<Stream<InputStream>, Stream<Reader>, PricingPipelineContext<E>>
        implements AutoCloseable {
    private final E probeStage;
    private Exception exceptionFromOpen = null;
    private String failedFilename = null;
    private int opened = 0;

    /**
     * Create a Reader opening stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public BOMAwareReadersStage(@Nonnull E probeStage) {
        this.probeStage = probeStage;
    }

    @NotNull
    @Override
    protected StageResult<Stream<Reader>> executeStage(@NotNull Stream<InputStream> streams) {
        // As the pipeline stage returns a stream immediately where exceptions from opening
        // may happen later, the pipeline stage returns immediate success, but the probe
        // stage will report later on any failures.

        getContext().autoClose(this);

        return StageResult.withResult(streams.map(this::createReader))
                .andStatus(Status.success("Created stream of Readers"));
    }

    @Nonnull
    private Reader createReader(@Nonnull InputStream stream) {
        try {
            Reader reader;

            BOMInputStream bomStream = new BOMInputStream(stream,
                    ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
                    ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE
            );
            if (bomStream.hasBOM()) {
                reader = new InputStreamReader(bomStream, bomStream.getBOMCharsetName());
            } else {
                reader = new InputStreamReader(bomStream);
            }

            getContext().autoClose(reader);
            opened++;

            return reader;
        } catch (Exception ex) {
            exceptionFromOpen = ex;

            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        StageInfo stage = getContext().getStageTracker().stage(probeStage);
        if (exceptionFromOpen == null) {
            stage.ok(String.format("Created readers for %d files", opened));
        } else {
            stage.fail(exceptionFromOpen)
                .summary("Failed while trying to convert stream to a reader");
        }
    }
}