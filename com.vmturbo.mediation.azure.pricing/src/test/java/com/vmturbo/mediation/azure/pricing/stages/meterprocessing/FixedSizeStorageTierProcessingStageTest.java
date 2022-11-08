package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.BOMAwareReadersStage;
import com.vmturbo.mediation.azure.pricing.stages.ChainedCSVParserStage;
import com.vmturbo.mediation.azure.pricing.stages.MCAMeterDeserializerStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStage;
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStageTest;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntryOrBuilder;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

/**
 * Test {@link FixedSizeStorageTierProcessingStage}.
 */
public class FixedSizeStorageTierProcessingStageTest {
    /**
     * Value used to verify double values.
     */
    private static final double DELTA = 0.00001d;

    /**
     * Test {@link FixedSizeStorageTierProcessingStage#processSelectedMeters}.
     *
     * @throws Exception when there is an error.
     */
    @Test
    public void testProcessSelectedMeters() throws Exception {
        /*
        The test file covers 2 regions: eastus and eastus2 with managed standard ssd
        E1, E2, E3, E4, E6, E10, E15.  Right now we only should include LRS not ZRS,
        though the zip contains ZRS entries as well.
         */
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader().getResource("standardDisks.zip").getPath());
        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // need to explicitly create the builders for the result;
        workspace.getBuilders();

        // verify "Azure plan"
        OnDemandPriceTableByRegionEntryOrBuilder westusInAzurePlan = workspace.getOnDemandBuilder("azure plan", "westus");
        assertEquals(0, westusInAzurePlan.getStoragePriceTableCount());

        Map<Long, Double> eastusPriceMap = ImmutableMap.of(
                4L, 0.3d, // E1
                8L, 0.6d, // E2
                16L, 1.2d,  //E3
                32L, 2.4d,  //E4
                64L, 4.8d, // E6
                128L, 9.6d, // E10
                256L, 19.2d // E15
        );

        OnDemandPriceTableByRegionEntryOrBuilder eastusInAzurePlan = workspace.getOnDemandBuilder("azure plan", "eastus");
        assertEquals(1, eastusInAzurePlan.getStoragePriceTableCount());
        assertEquals("azure::ST::MANAGED_STANDARD_SSD", eastusInAzurePlan.getStoragePriceTable(0).getRelatedStorageTier().getId());
        final StorageTierPrice storageTierPriceInAzurePlanEastus = eastusInAzurePlan.getStoragePriceTable(0).getStorageTierPriceList().getCloudStoragePrice(0);
        assertEquals(7, storageTierPriceInAzurePlanEastus.getPricesCount());
        assertEquals(RedundancyType.LRS, storageTierPriceInAzurePlanEastus.getRedundancyType());
        int verifiedCount = 0;
        for (final Price price : storageTierPriceInAzurePlanEastus.getPricesList()) {
            if (eastusPriceMap.keySet().contains(price.getEndRangeInUnits())) {
                assertEquals(eastusPriceMap.get(price.getEndRangeInUnits()), price.getPriceAmount().getAmount(), DELTA);
                verifiedCount++;
            }
        }
        assertEquals(7, verifiedCount);

        OnDemandPriceTableByRegionEntryOrBuilder eastus2InAzurePlan = workspace.getOnDemandBuilder("azure plan", "eastus2");
        assertEquals(1, eastus2InAzurePlan.getStoragePriceTableCount());
        assertEquals("azure::ST::MANAGED_STANDARD_SSD", eastus2InAzurePlan.getStoragePriceTable(0).getRelatedStorageTier().getId());
        final StorageTierPrice storageTierPriceInAzurePlanEastus2 = eastus2InAzurePlan.getStoragePriceTable(0).getStorageTierPriceList().getCloudStoragePrice(0);
        assertEquals(7, storageTierPriceInAzurePlanEastus2.getPricesCount());
        assertEquals(RedundancyType.LRS, storageTierPriceInAzurePlanEastus2.getRedundancyType());
        int verifiedCount2 = 0;
        for (final Price price : storageTierPriceInAzurePlanEastus2.getPricesList()) {
            if (eastusPriceMap.keySet().contains(price.getEndRangeInUnits())) {
                assertEquals(eastusPriceMap.get(price.getEndRangeInUnits()), price.getPriceAmount().getAmount(), DELTA);
                verifiedCount2++;
            }
        }
        assertEquals(7, verifiedCount2);

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Fixed Size Storage Tier Price Processing", status.getDescription());
    }

    /**
     * Test {@link FixedSizeStorageTierProcessingStage#processSelectedMeters}.
     *
     * @throws Exception when there is an error.
     */
    @Test
    public void testProcessSelectedMetersWithNoStorageTierMeters() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader().getResource("licenseprice.zip").getPath());
        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext("test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // need to explicitly create the builders for the result;
        workspace.getBuilders();

        // verify "Azure plan"
        OnDemandPriceTableByRegionEntryOrBuilder eastusInAzurePlan = workspace.getOnDemandBuilder("azure plan", "eastus");
        assertEquals(0, eastusInAzurePlan.getStoragePriceTableCount());

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Fixed Size Storage Tier Price Processing", status.getDescription());
    }


    @Nonnull
    private PricingPipeline<Path, PricingWorkspace> makePipeline(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .addStage(new SelectZipEntriesStage("*.csv", MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                .addStage(new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                .addStage(new ChainedCSVParserStage<>(MockPricingProbeStage.CHAINED_CSV_PARSERS))
                .addStage(new MCAMeterDeserializerStage(MockPricingProbeStage.DESERIALIZE_CSV))
                .addStage(MeterResolverStage.newBuilder()
                        .addResolver(new MeterDescriptorsFileResolver())
                        .build(MockPricingProbeStage.RESOLVE_METERS))
                .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
                .finalStage(new FixedSizeStorageTierProcessingStage(MockPricingProbeStage.FIXED_SIZE_STORAGE_TIER_PRICE_PROCESSOR)));
    }
}
