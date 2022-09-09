package com.vmturbo.mediation.azure.pricing.stages;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.Nonnull;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
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
 * Stage for enumerating and selecting entries within a zip file.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class SelectZipEntriesStage<E extends ProbeStageEnum>
        extends Stage<Path, List<ZipEntry>, PricingPipelineContext<E>> {
    private final String wildcard;
    private final E probeStage;
    private ZipFile zipFile;

    /**
     * Create a zip entries stage.
     *
     * @param wildcard a glob-style wildcard. Only entries in the zip whose name match
     *   will be included in the result.
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public SelectZipEntriesStage(@Nonnull final String wildcard, @Nonnull E probeStage) {
        this.wildcard = wildcard;
        this.probeStage = probeStage;

        providesToContext(PricingPipelineContextMembers.ZIP_FILE, (Supplier<ZipFile>)this::getZipFile);
    }

    @NotNull
    @Override
    protected StageResult<List<ZipEntry>> executeStage(@NotNull Path path)
            throws PipelineStageException {
        StageInfo stage = getContext().getStageTracker().stage(probeStage);
        try {
            zipFile = new ZipFile(path.toString());
            getContext().autoClose(zipFile);

            int totalEntries = zipFile.size();
            List<ZipEntry> entries = zipFile.stream()
                .filter(e -> FilenameUtils.wildcardMatch(e.getName(), wildcard, IOCase.INSENSITIVE))
                .sorted((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.getName(), b.getName()))
                .collect(Collectors.toList());

            if (entries.isEmpty()) {
                final String statusMessage = String.format("Found no files (out of %d) matching %s",
                        zipFile.size(), wildcard);

                stage.fail(statusMessage);
                throw new PipelineStageException(statusMessage);
            } else {
                final String statusMessage = String.format(
                        "Selected files matching %s (%d of %d files)", wildcard, entries.size(),
                        zipFile.size());

                final StringBuilder sb = new StringBuilder("Files selected for processing:");
                entries.stream().map(e -> String.format("\n%s (%d bytes)", e.getName(), e.getSize()))
                        .forEach(sb::append);

                stage.ok(statusMessage).longExplanation(sb.toString());
                return StageResult.withResult(entries).andStatus(Status.success(statusMessage));
            }
        } catch (IOException ex) {
            stage.fail(ex);
            throw new PipelineStageException(ex);
        }
    }

    /**
     * Get the zip file opened by this stage.
     *
     * @return the opened zip file
     */
    public ZipFile getZipFile() {
        return zipFile;
    }
}