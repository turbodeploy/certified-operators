package com.vmturbo.mediation.azure.pricing.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.mediation.azure.pricing.AzurePricingAccount;
import com.vmturbo.mediation.azure.pricing.controller.MCAPricingDiscoveryController;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingKey;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.IPMeterProcessingStage;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests for AssignPricingIdentifiersStage.
 */
public class AssignPricingIdentifiersStageTest {
    private static final String MCA_BILLING_ACCOUNT_ID = "MCACCOUNTID";
    private static final String MCA_BILLING_PROFILE_ID = "MCAPROFILEID";
    private static final String PLAN_ID = "0001";
    private static final String OTHER_PLAN_ID = "0002";

    /**
     * Test success.
     *
     * @throws PipelineStageException when there is an exception
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("ipAddresses.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        final IPropertyProvider propertyProvider = IProbePropertySpec::getDefaultValue;
        final MCAPricingDiscoveryController controller = new MCAPricingDiscoveryController(propertyProvider);

        final AzurePricingAccount account = new AzurePricingAccount(MCA_BILLING_ACCOUNT_ID,
                MCA_BILLING_PROFILE_ID, PLAN_ID, "", "", "", "", "", "", 0, "", "", false);

        final PricingKey requestedKey = controller.getKey(account);

        DiscoveredPricing result;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, DiscoveredPricing> pipeline =
                makePipeline(context, propertyProvider, requestedKey);

            result = pipeline.run(metersTestFile);
        }

        PricingKey otherPlanKey = requestedKey.withPlanId(OTHER_PLAN_ID);

        assertTrue(result.containsKey(requestedKey));
        assertTrue(result.containsKey(otherPlanKey));

        result.get(requestedKey).getPriceTableKeysList().equals(requestedKey.getPricingIdentifiers());
        result.get(otherPlanKey).getPriceTableKeysList().equals(otherPlanKey.getPricingIdentifiers());

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.ASSIGN_IDENTIFIERS);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Pricing for 2 plans discovered", status.getStatusShortExplanation());
        assertEquals("Assignments:\n"
                + "\nAzure Plan -> MCA Account MCACCOUNTID Profile MCAPROFILEID Plan 0001"
                + "\nAzure Plan For Devtest -> MCA Account MCACCOUNTID Profile MCAPROFILEID Plan 0002",
                status.getStatusLongExplanation());
    }

    @Nonnull
    private PricingPipeline<Path, DiscoveredPricing> makePipeline(
            @Nonnull PricingPipelineContext context,
            @Nonnull IPropertyProvider propertyProvider,
            @Nonnull PricingKey requestedKey) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER, () -> propertyProvider)
                .initialContextMember(PricingPipelineContextMembers.PRICING_KEY, () -> requestedKey)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
                .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV))
                .addStage(MeterResolverStage.newBuilder()
                        .addResolver(new MeterDescriptorsFileResolver())
                        .build(MockPricingProbeStage.RESOLVE_METERS))
                .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
                .addStage(new IPMeterProcessingStage(MockPricingProbeStage.IP_PRICE_PROCESSOR))
                .finalStage(new AssignPricingIdentifiersStage(MockPricingProbeStage.ASSIGN_IDENTIFIERS,
                        MCAPricingDiscoveryController.MCA_PLANID_MAP)));
    }
}
