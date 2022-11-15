package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.StoragePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * Tests for LinearSizeStorageTierProcessor.
 */
public class LinearSizeStorageTierProcessingStageTest {
    private static final String ID_PREFIX = "azure::ST::";

    /**
     * Test processing of pricing for storage that is priced using a multiplier of the size.
     *
     * @throws Exception if the test fails
     */
    @Test
    public void testLinearStoragePricing() throws Exception {
        Path metersTestFile = Paths.get(LinearSizeStorageTierProcessingStageTest.class.getClassLoader()
                .getResource("unmanagedstd.zip").getPath());

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

        StorageTierPriceList unmanagedStandard = regionPrices.getStoragePriceTableList().stream()
            .filter(tierEntry -> tierEntry.getRelatedStorageTier().getEntityType() == EntityType.STORAGE_TIER)
            .filter(tierEntry -> tierEntry.getRelatedStorageTier().getId().equals(ID_PREFIX + "UNMANAGED_STANDARD"))
            .map(StoragePriceTableByTierEntry::getStorageTierPriceList)
            .findAny().orElseThrow(() -> new RuntimeException("Couldn't find UNMANAGED_STANDARD"));

        List<Price> lrs = getRedundancyPrices(unmanagedStandard, RedundancyType.LRS);
        assertEquals(1, lrs.size());
        assertFalse(lrs.get(0).hasEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, lrs.get(0).getUnit());
        assertEquals(0.045, lrs.get(0).getPriceAmount().getAmount(), 0.00001);

        List<Price> grs = getRedundancyPrices(unmanagedStandard, RedundancyType.GRS);
        assertEquals(2, grs.size());
        assertEquals(99, grs.get(0).getEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, grs.get(0).getUnit());
        assertEquals(0.06, grs.get(0).getPriceAmount().getAmount(), 0.00001);
        assertFalse(grs.get(1).hasEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, grs.get(1).getUnit());
        assertEquals(0.055, grs.get(1).getPriceAmount().getAmount(), 0.00001);

        List<Price> ragrs = getRedundancyPrices(unmanagedStandard, RedundancyType.RAGRS);
        assertEquals(3, ragrs.size());
        assertEquals(99, ragrs.get(0).getEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, ragrs.get(0).getUnit());
        assertEquals(0.075, ragrs.get(0).getPriceAmount().getAmount(), 0.00001);
        assertEquals(499, ragrs.get(1).getEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, ragrs.get(1).getUnit());
        assertEquals(0.065, ragrs.get(1).getPriceAmount().getAmount(), 0.00001);
        assertFalse(ragrs.get(2).hasEndRangeInUnits());
        assertEquals(Unit.GB_MONTH, ragrs.get(2).getUnit());
        assertEquals(0.055, ragrs.get(2).getPriceAmount().getAmount(), 0.00001);

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Linear Size Storage Tier Price Processing", status.getDescription());
    }

    @Nonnull
    private List<Price> getRedundancyPrices(@Nonnull StorageTierPriceList unmanagedStandard,
        @Nonnull RedundancyType redundancyType) {

        return unmanagedStandard.getCloudStoragePriceList().stream()
            .filter(tierPrice -> tierPrice.getRedundancyType() == redundancyType)
            .map(tierPrice -> tierPrice.getPricesList())
            .findAny().orElseThrow(() ->
                new RuntimeException("Couldn't find redundancy " + redundancyType.toString()));
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
            .finalStage(new LinearSizeStorageTierProcessingStage(
                MockPricingProbeStage.LINEAR_SIZE_STORAGE_TIER_PRICE_PROCESSOR)));
    }
}
