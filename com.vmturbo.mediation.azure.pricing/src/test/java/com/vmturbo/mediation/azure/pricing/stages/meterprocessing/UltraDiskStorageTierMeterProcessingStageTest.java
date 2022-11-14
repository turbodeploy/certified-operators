package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static com.vmturbo.mediation.cost.dto.StorageTier.MANAGED_ULTRA_SSD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
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
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.StoragePriceTableByTierEntry;

/**
 * Tests UltraDiskStorageTierMeterProcessingStage.
 */
public class UltraDiskStorageTierMeterProcessingStageTest {

    /**
     * Test success.
     *
     * @throws PipelineStageException when there is an exception.
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(
                UltraDiskStorageTierMeterProcessingStage.class.getClassLoader()
                        .getResource("ultradiskstorage.zip")
                        .getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<>(
                MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // need to explicitly create the builders for the result;
        workspace.getBuilders();

        verifyWorkspaceForPlan(workspace, "Azure plan", 2);
        verifyWorkspaceForPlan(workspace, "Azure plan for DevTest", 1);
        assertEquals(1, workspace.getAndRemoveResolvedMeterByMeterType(MeterType.Storage).size());
        assertEquals(1, workspace.getAndRemoveResolvedMeterByMeterType(MeterType.OS).size());
        ProbeStageDetails status = tracker.getStageDetails(
                MockPricingProbeStage.ULTRA_DISK_STORAGE_TIER);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Ultra Disk Storage Tier Processing", status.getDescription());
        assertEquals(
                "Managed Ultra SSD prices added for [PlanId, Region]: {azure plan for devtest=1, azure plan=2}",
                status.getStatusShortExplanation());
    }

    /**
     * Test when there is no Storage Meter.
     *
     * @throws PipelineStageException when there is an exception
     */
    @Test
    public void testNoStorageMeter() throws Exception {
        Path metersTestFile = Paths.get(
                UltraDiskStorageTierMeterProcessingStage.class.getClassLoader()
                        .getResource("licenseprice.zip")
                        .getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(
                        MockPricingProbeStage.DISCOVERY_STAGES);

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // need to explicitly create the builders for the result;
        workspace.getBuilders();

        final PriceTable.Builder azurePlanOnDemandPriceTable =
                workspace.getPriceTableBuilderForPlan("Azure Plan");
        assertNotNull(azurePlanOnDemandPriceTable);
        assertEquals("OnDemandPriceTable for Azure Plan should have no records", 0,
                azurePlanOnDemandPriceTable.getOnDemandPriceTableList().size());

        final PriceTable.Builder azurePlanForDevTestOnDemandPriceTable =
                workspace.getPriceTableBuilderForPlan("Azure Plan for DevTest");
        assertNotNull(azurePlanForDevTestOnDemandPriceTable);
        assertEquals("OnDemandPriceTable for Azure Plan For DevTest should have no records", 0,
                azurePlanForDevTestOnDemandPriceTable.getOnDemandPriceTableList().size());

        ProbeStageDetails status = tracker.getStageDetails(
                MockPricingProbeStage.ULTRA_DISK_STORAGE_TIER);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Ultra Disk Storage Tier Processing", status.getDescription());
        assertEquals("No meters found for Storage.", status.getStatusShortExplanation());
    }

    @Nonnull
    private PricingPipeline<Path, PricingWorkspace> makePipeline(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(
                PipelineDefinition.<MockAccount, Path, PricingPipelineContext<MockPricingProbeStage>>newBuilder(
                                context)
                        .addStage(new SelectZipEntriesStage("*.csv",
                                MockPricingProbeStage.SELECT_ZIP_ENTRIES))
                        .addStage(new OpenZipEntriesStage(MockPricingProbeStage.OPEN_ZIP_ENTRIES))
                        .addStage(
                                new BOMAwareReadersStage<>(MockPricingProbeStage.BOM_AWARE_READERS))
                        .addStage(new ChainedCSVParserStage<>(
                                MockPricingProbeStage.CHAINED_CSV_PARSERS))
                        .addStage(new MCAMeterDeserializerStage(
                                MockPricingProbeStage.DESERIALIZE_CSV))
                        .addStage(MeterResolverStage.newBuilder()
                                .addResolver(new MeterDescriptorsFileResolver())
                                .build(MockPricingProbeStage.RESOLVE_METERS))
                        .addStage(new RegroupByTypeStage(MockPricingProbeStage.REGROUP_METERS))
                        .finalStage(new UltraDiskStorageTierMeterProcessingStage(
                                MockPricingProbeStage.ULTRA_DISK_STORAGE_TIER)));
    }

    private void verifyWorkspaceForPlan(final PricingWorkspace workspace, final String planId,
            final int numberOfRegions) {
        final PriceTable.Builder onDemandPriceTable = workspace.getPriceTableBuilderForPlan(planId);
        assertNotNull(onDemandPriceTable);
        final List<OnDemandPriceTableByRegionEntry> onDemandPriceTableByRegionEntries =
                onDemandPriceTable.getOnDemandPriceTableList();
        assertNotNull(onDemandPriceTableByRegionEntries);
        assertEquals("There should be " + numberOfRegions + " regions in OnDemandPriceTable for "
                + planId, numberOfRegions, onDemandPriceTableByRegionEntries.size());
        int numberOfRegionsWithUltraDiskStorage = 0;
        for (OnDemandPriceTableByRegionEntry onDemandPriceTableByRegionEntry : onDemandPriceTableByRegionEntries) {
            final List<StoragePriceTableByTierEntry> storagePrice =
                    onDemandPriceTableByRegionEntry.getStoragePriceTableList();
            assertNotNull(storagePrice);
            assertEquals(1, storagePrice.size());
            assertEquals(EntityType.STORAGE_TIER, storagePrice.get(0)
                    .getRelatedStorageTier()
                    .getEntityType());
            assertEquals("azure::ST::" + MANAGED_ULTRA_SSD.getSdkName().toUpperCase(),
                    storagePrice.get(0)
                    .getRelatedStorageTier()
                    .getId());
            assertEquals(RedundancyType.valueOf(MANAGED_ULTRA_SSD.getRedundancy()),
                    storagePrice.get(0)
                            .getStorageTierPriceList()
                            .getCloudStoragePrice(0)
                            .getRedundancyType());
            List<Price> priceList = storagePrice.get(0)
                    .getStorageTierPriceList()
                    .getCloudStoragePrice(0)
                    .getPricesList();
            assertEquals(74, priceList.size());
            List<Price> filteredList = priceList.stream()
                    .filter(price -> Unit.MONTH.equals(price.getUnit())
                            && price.hasEndRangeInUnits())
                    .collect(Collectors.toList());
            assertEquals(72, filteredList.size());
            Optional<Price> rangePrice = (priceList.stream().filter(
                    price -> Unit.MONTH.equals(price.getUnit()) && price.hasEndRangeInUnits()
                            && price.getEndRangeInUnits() == 3072).findAny());
            assertTrue(rangePrice.isPresent());
            assertEquals(rangePrice.get().getPriceAmount().getAmount(), 1222.1952, .1);
            filteredList = priceList.stream().filter(
                    price -> !price.hasEndRangeInUnits() && Unit.MILLION_IOPS.equals(
                            price.getUnit())).collect(Collectors.toList());
            assertEquals(1, filteredList.size());
            assertEquals(filteredList.get(0).getPriceAmount().getAmount(), 0.3978, .001);
            filteredList = priceList.stream()
                    .filter(price -> !price.hasEndRangeInUnits() && Unit.MBPS_MONTH.equals(
                            price.getUnit()))
                    .collect(Collectors.toList());
            assertEquals(1, filteredList.size());
            assertEquals(filteredList.get(0).getPriceAmount().getAmount(), 0.3978, .001);
            numberOfRegionsWithUltraDiskStorage++;
        }
        assertEquals(numberOfRegions, numberOfRegionsWithUltraDiskStorage);
    }
}
