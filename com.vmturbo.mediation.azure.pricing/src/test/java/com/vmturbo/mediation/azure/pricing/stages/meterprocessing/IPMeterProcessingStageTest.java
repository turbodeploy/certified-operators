package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Currency;
import java.util.List;

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
import com.vmturbo.mediation.azure.pricing.stages.MeterResolverStageTest;
import com.vmturbo.mediation.azure.pricing.stages.OpenZipEntriesStage;
import com.vmturbo.mediation.azure.pricing.stages.RegroupByTypeStage;
import com.vmturbo.mediation.azure.pricing.stages.SelectZipEntriesStage;
import com.vmturbo.mediation.cost.common.CostContributersUtils;
import com.vmturbo.mediation.cost.common.CostUtils;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;

/**
 * Tests for IPMeterProcessingStage.
 */
public class IPMeterProcessingStageTest {
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

        PricingWorkspace workspace;

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);

            workspace = pipeline.run(metersTestFile);
        }

        // need to explicitly create the builders for the result;
        workspace.getBuilders();

        verifyWorkspaceForPlan(workspace, "Azure plan", 0.004d, 51);
        verifyWorkspaceForPlan(workspace, "Azure plan for DevTest", 0.002d, 51);

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.IP_PRICE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("IP Price Processing", status.getDescription());
        assertEquals("IP prices added: Azure Plan=51, Azure Plan For Devtest=51",
                status.getStatusShortExplanation());
    }

    /**
     * Test when there is no IP Meter.
     *
     * @throws PipelineStageException when there is an exception
     */
    @Test
    public void testNoIpMeter() throws Exception {
        Path metersTestFile = Paths.get(MeterResolverStageTest.class.getClassLoader()
                .getResource("resolvertest.zip").getPath());

        ProbeStageTracker<MockPricingProbeStage> tracker =
                new ProbeStageTracker<MockPricingProbeStage>(MockPricingProbeStage.DISCOVERY_STAGES);

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
        assertEquals("OnDemandPriceTable for Azure Plan should have no records",
                0, azurePlanOnDemandPriceTable.getOnDemandPriceTableList().size());

        final PriceTable.Builder azurePlanForDevTestOnDemandPriceTable =
                workspace.getPriceTableBuilderForPlan("Azure Plan for DevTest");
        assertNotNull(azurePlanForDevTestOnDemandPriceTable);
        assertEquals("OnDemandPriceTable for Azure Plan For DevTest should have no records",
                0, azurePlanForDevTestOnDemandPriceTable.getOnDemandPriceTableList().size());

        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.IP_PRICE_PROCESSOR);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("IP Price Processing", status.getDescription());
        assertEquals("No meters found for IP.", status.getStatusShortExplanation());
    }

    private void verifyWorkspaceForPlan(final PricingWorkspace workspace, final String planId, final double expectedIpPrice, final int numberOfRegions) {
        final PriceTable.Builder onDemandPriceTable = workspace.getPriceTableBuilderForPlan(planId);
        assertNotNull(onDemandPriceTable);
        final List<OnDemandPriceTableByRegionEntry> onDemandPriceTableByRegionEntries = onDemandPriceTable.getOnDemandPriceTableList();
        assertNotNull(onDemandPriceTableByRegionEntries);
        assertEquals("There should be " + numberOfRegions + " regions in OnDemandPriceTable for " + planId,
                numberOfRegions, onDemandPriceTableByRegionEntries.size());
        int numberOfRegionsWithIpPrice = 0;
        for (OnDemandPriceTableByRegionEntry onDemandPriceTableByRegionEntry : onDemandPriceTableByRegionEntries) {
            final IpPriceList ipPriceList = onDemandPriceTableByRegionEntry.getIpPrices();
            assertNotNull(ipPriceList);
            assertEquals(1, ipPriceList.getIpPriceList().size());

            final IpConfigPrice ipConfigPrice = ipPriceList.getIpPrice(0);
            assertNotNull(ipConfigPrice);
            assertEquals(CostContributersUtils.TYPE_PUBLIC_IPADRESS, ipConfigPrice.getType());
            assertEquals(0, ipConfigPrice.getFreeIpCount());
            assertEquals(1, ipConfigPrice.getPricesList().size());
            final PricingDTO.Price price = ipConfigPrice.getPrices(0);
            assertEquals(Unit.HOURS, price.getUnit());
            assertEquals(expectedIpPrice, price.getPriceAmount().getAmount(), 0.0000001d);
            assertEquals(Currency.getInstance(CostUtils.COST_CURRENCY_USD).getNumericCode(), price.getPriceAmount().getCurrency());
            numberOfRegionsWithIpPrice++;
        }
        assertEquals(numberOfRegions, numberOfRegionsWithIpPrice);
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
                .finalStage(new IPMeterProcessingStage(MockPricingProbeStage.IP_PRICE_PROCESSOR)));
    }
}
