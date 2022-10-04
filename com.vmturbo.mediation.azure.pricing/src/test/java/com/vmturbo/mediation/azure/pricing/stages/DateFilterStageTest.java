package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;

/**
 * Test class for DateFilterStage.
 */
public class DateFilterStageTest {
    DateFilterStage stage;

    /**
     * setup.
     *
     */
    @Before
    public void setup() {
        stage = spy(new DateFilterStage(MockPricingProbeStage.EFFECTIVE_DATE_FILTER));
        when(stage.getCurrentInstant()).thenReturn(
                Instant.now(Clock.fixed(Instant.parse("2022-07-08T00:00:00Z"), ZoneOffset.UTC)));
    }

    /**
     * tests whether the effective date filter is working as intended.
     *
     * @throws Exception throws Exception
     */
    @Test
    public void testDateFilterStage() throws Exception {

        Path metersTestFile = Paths.get(DateFilterStage.class.getClassLoader()
                .getResource("datefilter.zip").getPath());
        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<>(MockPricingProbeStage.DISCOVERY_STAGES);
        List<String> meterStrings;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test",
                tracker)) {
            PricingPipeline<Path, Stream<AzureMeter>> pipeline = makePipeline(context);

            meterStrings = pipeline.run(metersTestFile)
                    .map(Objects::toString).collect(Collectors.toList());
            assertEquals(4, meterStrings.size());
        }
    }

    @Nonnull
    private PricingPipeline<Path, Stream<AzureMeter>> makePipeline(@Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
                .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV))
                .finalStage(stage));
    }
}
