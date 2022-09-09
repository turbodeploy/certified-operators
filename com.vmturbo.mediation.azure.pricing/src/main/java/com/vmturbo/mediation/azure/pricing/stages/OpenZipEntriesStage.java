package com.vmturbo.mediation.azure.pricing.stages;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker.StageInfo;

/**
 * Stage for opening entries from a zip file as input streams.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class OpenZipEntriesStage<E extends ProbeStageEnum>
        extends Stage<List<ZipEntry>, Stream<InputStream>, PricingPipelineContext<E>>
        implements AutoCloseable {
    private final E probeStage;
    private Exception exceptionFromOpen = null;
    private String failedFilename = null;
    private int opened = 0;

    private FromContext<ZipFile> zipFile = requiresFromContext(PricingPipelineContextMembers.ZIP_FILE);

    /**
     * Create a zip entry opening stage.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public OpenZipEntriesStage(@Nonnull E probeStage) {
        this.probeStage = probeStage;
    }

    @NotNull
    @Override
    protected StageResult<Stream<InputStream>> executeStage(@NotNull List<ZipEntry> entries)
            throws PipelineStageException {
        // As the pipeline stage returns a stream immediately where exceptions from opening
        // may happen later, the pipeline stage returns immediate success, but the probe
        // stage will report later on any failures.

        getContext().autoClose(this);

        return StageResult.withResult(entries.stream()
                .map(this::openZipEntry))
                .andStatus(Status.success("Opening " + entries.size() + " Input Streams"));
    }

    @Nonnull
    private InputStream openZipEntry(@Nonnull ZipEntry entry) {
        try {
            InputStream stream = zipFile.get().getInputStream(entry);
            if (stream == null) {
                throw new RuntimeException(String.format(
                        "Open of %s returned null stream", entry.getName()));
            }

            opened++;

            return stream;
        } catch (Exception ex) {
            exceptionFromOpen = ex;
            failedFilename = entry.getName();

            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        StageInfo stage = getContext().getStageTracker().stage(probeStage);
        if (exceptionFromOpen == null) {
            stage.ok(String.format("Opened %s files", opened));
        } else {
            stage.fail(exceptionFromOpen)
                .summary("Failed while trying to open " + failedFilename);
        }
    }
}