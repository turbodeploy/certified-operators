package com.vmturbo.mediation.azure.pricing.stages;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;

/**
 * This stage inserts a broken InputStream, in order to introduce an error for testing.
 */
public class BrokenInputStreamStage extends
        PricingPipeline.Stage<Stream<InputStream>, Stream<InputStream>, MockPricingProbeStage> {
    /**
     * Construct a new BrokenInputStreamStage.
     */
    BrokenInputStreamStage() {
        super(MockPricingProbeStage.BROKEN_INPUT_STAGE);
    }

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
