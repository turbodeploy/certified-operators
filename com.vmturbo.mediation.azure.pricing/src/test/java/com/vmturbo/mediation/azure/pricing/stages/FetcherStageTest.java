package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.fetcher.MockPricingFileFetcher;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests for FetcherStage.
 */
public class FetcherStageTest {
    private final MockAccount account = new MockAccount("1", "0001");
    private final MockPricingFileFetcher fetcher = new MockPricingFileFetcher();
    private final IPropertyProvider propertyProvider = IProbePropertySpec::getDefaultValue;

    /**
     * Allocates and cleans up a directory for temporary files for this test.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Verify the happy path: fetcher succeeds, the stage returns its result, and the
     * probe stage is reported as successful with the right status message.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testDownloadSuccess() throws Exception {
        final Path successPath = tmpFolder.newFolder("pricesheet").toPath().resolve("success.zip");
        final String successMessage = "Wow, it worked!";

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(account.toString(),
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<MockAccount, Path> pipeline = makePipeline(context, propertyProvider);
            fetcher.setResult(() -> new Pair<>(successPath, successMessage));

            assertEquals(successPath, pipeline.run(account));
            Files.delete(successPath);

            ProbeStageDetails status = context.getStageTracker()
                .getStageDetails(MockPricingProbeStage.DOWNLOAD_PRICE_SHEET);

            assertEquals(StageStatus.SUCCESS, status.getStatus());
            assertEquals(successMessage, status.getStatusShortExplanation());
        }
    }

    /**
     * Verify that if the fetcher fails, the stage fails, and the probe stage reflects that.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testDownloadFailure() throws Exception {
        final String failMessage = "Oops, that didn't work!";

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(account.toString(),
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            PricingPipeline<MockAccount, Path> pipeline = makePipeline(context, propertyProvider);
            fetcher.setResult(() -> {
                throw new RuntimeException(failMessage);
            });

            PipelineException ex = assertThrows(PipelineException.class, () -> {
                pipeline.run(account);
            });

            ProbeStageDetails status = context.getStageTracker()
                    .getStageDetails(MockPricingProbeStage.DOWNLOAD_PRICE_SHEET);

            assertEquals(StageStatus.FAILURE, status.getStatus());
            assertEquals(failMessage, status.getStatusShortExplanation());
        }
    }

    /**
     * Verify that if the override probe property is set, the fetcher stage doesn't
     * call the fetcher, and just returns the override path.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testDownloadOverride() throws Exception {
        final String overridePath = "/path/to/some/download/override/file.zip";
        final IPropertyProvider propertyProvider = new IPropertyProvider() {
            @SuppressWarnings("unchecked")
            @Override
            @Nonnull
            public <T> T getProperty(@Nonnull IProbePropertySpec<T> property) {
                if (FetcherStage.OVERRIDE_PATH.equals(property)) {
                    return (T)overridePath;
                } else {
                    return property.getDefaultValue();
                }
            }
        };

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(account.toString(),
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES))) {

            fetcher.setResult(() -> {
                throw new RuntimeException("This fetcher should not have been called!");
            });

            PricingPipeline<MockAccount, Path> pipeline = makePipeline(context, propertyProvider);

            // If the stage were to call the fetcher, it should throw and cause this to fail
            Path result = pipeline.run(account);

            // instead it should return the overridden path configured.
            assertEquals(Paths.get(overridePath), result);

            ProbeStageDetails status = context.getStageTracker()
                    .getStageDetails(MockPricingProbeStage.DOWNLOAD_PRICE_SHEET);

            assertEquals(StageStatus.SUCCESS, status.getStatus());
        }
    }

    @Nonnull
    private PricingPipeline<MockAccount, Path> makePipeline(PricingPipelineContext context,
            IPropertyProvider propertyProvider) {
        PricingPipeline<MockAccount, Path> pipeline = new PricingPipeline<>(
                PipelineDefinition.<MockAccount, Path,
                                PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                        .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER,
                                () -> propertyProvider)
                        .finalStage(new FetcherStage<MockAccount, MockPricingProbeStage>(
                                fetcher, MockPricingProbeStage.DOWNLOAD_PRICE_SHEET
                        )));
        return pipeline;
    }
}
