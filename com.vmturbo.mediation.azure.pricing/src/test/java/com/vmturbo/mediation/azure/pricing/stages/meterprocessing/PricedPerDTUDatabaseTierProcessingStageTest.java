package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
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
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests PricedPerDTUDatabaseTierProcessingStage.
 */
public class PricedPerDTUDatabaseTierProcessingStageTest {
    private final IPropertyProvider propertyProvider = IProbePropertySpec::getDefaultValue;

    /**
     * Test success.
     *
     * @throws PipelineStageException when there is an exception.
     */
    @Test
    public void testPricingWithOverrides() throws Exception {
        Path metersTestFile = Paths.get(LicensePriceProcessingStage.class.getClassLoader()
                .getResource("pricedperdtu.zip").getPath());

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

        for (Entry<String, Double> entry
                : ImmutableMap.of("azure plan", 1.0, "azure plan for devtest", 0.75).entrySet()) {
            // The pricing applies to several regions, check two of them
            for (String region : ImmutableList.of("eastus", "eastus2")) {
                OnDemandPriceTableByRegionEntry.Builder regionPricesBuilder
                    = workspace.getOnDemandBuilder(entry.getKey(), region);

                double mult = entry.getValue();

                // These three should have been discovered by the Individually Priced processor
                // and so should NOT follow the expected pricing pattern of the rest

                checkDbPrice(regionPricesBuilder, "Standard_S1", 2.0 * mult, false);
                checkDbPrice(regionPricesBuilder, "Standard_S2", 5.0 * mult, false);
                checkDbPrice(regionPricesBuilder, "Standard_S3", 10.0 * mult, false);

                // In the sample data the pricing is supplied as $1/10 DTU for regular,
                // $0.75 / 10 DTU for DevTest, so DTU * 0.10 (* 0.75 if DevTest)

                checkDbPrice(regionPricesBuilder, "Standard_S4", 20.0 * mult, true);
                checkDbPrice(regionPricesBuilder, "Standard_S6", 40.0 * mult, true);
                checkDbPrice(regionPricesBuilder, "Standard_S7", 80.0 * mult, true);
                checkDbPrice(regionPricesBuilder, "Standard_S9", 160.0 * mult, true);
                checkDbPrice(regionPricesBuilder, "Standard_S12", 300.0 * mult, true);
            }
        }

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.PER_DTU_DATABASE_TIER_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("Price-per-DTU Database Tier Processing", status.getDescription());
    }

    private void
    checkDbPrice(@Nonnull OnDemandPriceTableByRegionEntry.Builder builder, @Nonnull String size,
            double price, boolean shouldMatch) {
        List<Price> basePrices = builder.getDatabasePriceTableList().stream()
            .filter(x -> x.getRelatedDatabaseTier().getId().equals(
                AbstractDbTierMeterProcessingStage.AZURE_DBPROFILE_ID_PREFIX + size))
            .flatMap(y -> y.getDatabaseTierPriceListList().stream())
            // Tests for AbstractDbTierMeterProcessingStage will verify the details of the pricing,
            // here we just want to confirm that the storage pricing is being added.
            .filter(priceList -> priceList.getDependentPricesCount() != 0)
            .map(DatabaseTierPriceList::getBasePrice)
            .filter(basePrice -> basePrice.getDbEngine() == DatabaseEngine.SQLSERVER)
            .filter(basePrice -> basePrice.getDbEdition() == DatabaseEdition.NONE)
            .filter(basePrice -> !basePrice.hasDbDeploymentType())
            .filter(basePrice -> !basePrice.hasDbLicenseModel())
            .flatMap(basePrice -> basePrice.getPricesList().stream())
            .collect(Collectors.toList());

        assertEquals(1, basePrices.size());
        Price basePrice = basePrices.get(0);

        assertEquals(Unit.DAYS, basePrice.getUnit());
        assertTrue(basePrice.hasPriceAmount());
        assertTrue(basePrice.getPriceAmount().hasAmount());

        if (shouldMatch) {
            assertEquals(price, basePrice.getPriceAmount().getAmount(), 0.0001);
        } else {
            assertNotEquals(price, basePrice.getPriceAmount().getAmount(), 0.0001);
        }
    }

    @Nonnull
    private PricingPipeline<Path, PricingWorkspace> makePipeline(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path,
                        PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
                .initialContextMember(PricingPipelineContextMembers.PROPERTY_PROVIDER,
                    () -> propertyProvider)
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
                .addStage(new DBStorageMeterProcessingStage(MockPricingProbeStage.DB_STORAGE_METER_PROCESSOR))
                .addStage(new IndividuallyPricedDbTierProcessingStage(
                    MockPricingProbeStage.INDIVIDUALLY_PRICED_DB_TIER_PROCESSOR))
                .finalStage(new PricedPerDTUDatabaseTierProcessingStage(
                    MockPricingProbeStage.PER_DTU_DATABASE_TIER_PROCESSOR)));
    }
}
