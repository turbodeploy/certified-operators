package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContextMembers;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.BOMAwareReadersStage;
import com.vmturbo.mediation.azure.pricing.stages.ChainedCSVParserStage;
import com.vmturbo.mediation.azure.pricing.stages.MCAMeterDeserializerStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStage;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.util.PriceConverter;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.ComputePriceTableByTierEntry;

/**
 * Tests for InstanceTypeProcessingStage.
 */
public class InstanceTypeProcessingStageTest {
    private static final String ID_PREFIX = "azure::VMPROFILE::";

    /**
     * Test processing of pricing for VM instance types.
     *
     * @throws Exception if thhe test fails
     */
    @Test
    public void testComputeTierPricing() throws Exception {
        Path metersTestFile = Paths.get(InstanceTypeProcessingStageTest.class.getClassLoader()
                .getResource("vmsizes.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        OnDemandPriceTableByRegionEntry.Builder regionPricesBuilder =
            workspace.getOnDemandBuilder("azure plan", "eastus2");
        assertNotNull(regionPricesBuilder);

        OnDemandPriceTableByRegionEntry regionPrices = regionPricesBuilder.build();

        // The sample data has only Linux pricing for Standard_D2s_v4
        checkSingleOsPrice(regionPrices, "Standard_D2s_v4", OSType.LINUX, 0.096);

        // The sample data has only Windows pricing for Standard_D4s_v4
        checkSingleOsPrice(regionPrices, "Standard_D4s_v4", OSType.WINDOWS, 0.376);

        // The sample data has cheaper Linux and more expensive Windows pricing for Standard_D8s_v4
        checkTwoOsPrices(regionPrices, "Standard_D8s_v4", OSType.LINUX, OSType.WINDOWS, 0.384, 0.752);

        // The sample data has cheaper Windows and more expensive Linux pricing for Standard_D16s_v4
        // This should never happen, but does anyway sometimes in Azure's data.
        checkTwoOsPrices(regionPrices, "Standard_D16s_v4", OSType.WINDOWS, OSType.LINUX, 0.768, 1.504);

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.INSTANCE_TYPE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("VM Instance Type Price Processing", status.getDescription());
        assertEquals("9 VM Instance Type prices processed, 0 errors",
            status.getStatusShortExplanation());
    }

    private void checkSingleOsPrice(
            @Nonnull OnDemandPriceTableByRegionEntry regionPrices,
            @Nonnull String typeName,
            @Nonnull OSType osType,
            double expectedPrice) {
        ComputeTierPriceList computeTierPriceList = getPricesForInstanceType(regionPrices, typeName);

        ComputeTierConfigPrice basePrice = computeTierPriceList.getBasePrice();
        assertEquals(osType, basePrice.getGuestOsType());
        assertEquals(1, basePrice.getPricesCount());
        assertEquals(expectedPrice, basePrice.getPrices(0).getPriceAmount().getAmount(), 0.001);

        // There should be a WINDOWS_BYOL price with a price adjustment of 0
        if (osType == OSType.WINDOWS) {
            assertEquals(1, computeTierPriceList.getPerConfigurationPriceAdjustmentsCount());
            ComputeTierConfigPrice byol = computeTierPriceList.getPerConfigurationPriceAdjustments(0);
            assertEquals(OSType.WINDOWS_BYOL, byol.getGuestOsType());
            assertEquals(1, byol.getPricesCount());
            assertEquals(0, byol.getPrices(0).getPriceAmount().getAmount(), 0.001);
        } else {
            // If there is only Linux pricing, there shouldn't be WINDOWS_BYOL pricing
            assertEquals(0, computeTierPriceList.getPerConfigurationPriceAdjustmentsCount());
        }
    }

    private void checkTwoOsPrices(
            @Nonnull OnDemandPriceTableByRegionEntry regionPrices,
            @Nonnull String typeName,
            @Nonnull OSType baseOsType,
            @Nonnull OSType otherOsType,
            double expectedBasePrice,
            double expectedOtherOsPrice) {
        ComputeTierPriceList computeTierPriceList = getPricesForInstanceType(regionPrices, typeName);

        ComputeTierConfigPrice basePrice = computeTierPriceList.getBasePrice();
        assertEquals(baseOsType, basePrice.getGuestOsType());
        assertEquals(1, basePrice.getPricesCount());
        assertEquals(expectedBasePrice, basePrice.getPrices(0).getPriceAmount().getAmount(), 0.001);

        assertEquals(2, computeTierPriceList.getPerConfigurationPriceAdjustmentsCount());

        ComputeTierConfigPrice otherOsPrice = computeTierPriceList.getPerConfigurationPriceAdjustments(0);
        assertEquals(otherOsType, otherOsPrice.getGuestOsType());
        assertEquals(1, otherOsPrice.getPricesCount());
        assertEquals(expectedOtherOsPrice - expectedBasePrice,
            otherOsPrice.getPrices(0).getPriceAmount().getAmount(), 0.001);

        ComputeTierConfigPrice byol = computeTierPriceList.getPerConfigurationPriceAdjustments(1);
        assertEquals(OSType.WINDOWS_BYOL, byol.getGuestOsType());
        assertEquals(1, byol.getPricesCount());
        assertEquals(0, byol.getPrices(0).getPriceAmount().getAmount(), 0.001);
    }

    @Nonnull
    private PricingPipeline<Path, PricingWorkspace> makePipeline(
        @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
            .initialContextMember(PricingPipelineContextMembers.PRICE_CONVERTER,
                () -> new PriceConverter())
            .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
            .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
            .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
            .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
            .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV))
            .addStage(MeterResolverStage.newBuilder()
                .addResolver(new MeterDescriptorsFileResolver())
                .build(MockPricingProbeStage.RESOLVE_METERS))
            .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
            .finalStage(new InstanceTypeProcessingStage(MockPricingProbeStage.INSTANCE_TYPE_PROCESSOR)));
    }

    @Nonnull
    private ComputeTierPriceList getPricesForInstanceType(
        @Nonnull OnDemandPriceTableByRegionEntry regionPrices, @Nonnull String instanceType) {
        return regionPrices.getComputePriceTableList().stream()
            .filter(tierEntry -> tierEntry.getRelatedComputeTier().getEntityType() == EntityType.COMPUTE_TIER)
            .filter(tierEntry -> tierEntry.getRelatedComputeTier().getId().equals(ID_PREFIX + instanceType))
            .map(ComputePriceTableByTierEntry::getComputeTierPriceList)
            .findAny().orElseThrow(() -> new RuntimeException("Couldn't find " + instanceType));
    }
}
