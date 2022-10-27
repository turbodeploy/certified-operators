package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

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
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;

/**
 * Tests LicensePriceProcessingStage.
 */
public class LicensePriceProcessingStageTest {

    /**
     * Test success.
     *
     * @throws PipelineStageException when there is an exception.
     */
    @Test
    public void testSuccess() throws Exception {
        Path metersTestFile = Paths.get(LicensePriceProcessingStage.class.getClassLoader()
                .getResource("licenseprice.zip").getPath());

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

        Set<LicensePriceEntry> onDemandLicenseSet = workspace.getLicensePriceEntryList(
                "Azure plan");
        assertEquals(2, onDemandLicenseSet.size());
        onDemandLicenseSet.forEach(entry -> {
            if (entry.getOsType().equals(OSType.LINUX_WITH_SQL_ENTERPRISE)) {
                assertEquals(1, entry.getLicensePricesList().size());
                assertEquals(132.0, entry.getLicensePricesList()
                        .get(0)
                        .getPrice()
                        .getPriceAmount()
                        .getAmount(), 0);
            } else if (entry.getOsType().equals(OSType.LINUX_WITH_SQL_WEB)) {
                assertEquals(2, entry.getLicensePricesList().size());
            }
        });

        onDemandLicenseSet = workspace.getLicensePriceEntryList("Azure plan for DevTest");
        assertEquals(4, onDemandLicenseSet.size());
        onDemandLicenseSet.forEach(entry -> {
            if (entry.getOsType().equals(OSType.LINUX_WITH_SQL_ENTERPRISE)) {
                assertEquals(1, entry.getLicensePricesList().size());
                assertEquals(132.0, entry.getLicensePricesList()
                        .get(0)
                        .getPrice()
                        .getPriceAmount()
                        .getAmount(), 0);
            } else if (entry.getOsType().equals(OSType.LINUX_WITH_SQL_WEB)) {
                assertEquals(2, entry.getLicensePricesList().size());
            }
        });

        assertTrue(onDemandLicenseSet.stream().anyMatch(LicensePriceEntry::getBurstableCPU));
        assertTrue(onDemandLicenseSet.stream()
                .anyMatch(entry -> !entry.getBurstableCPU() && entry.getOsType()
                        .equals(OSType.WINDOWS)));


        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.LICENSE_PRICE_PROCESSING);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("License Price Processing", status.getDescription());
        assertEquals("License prices added for [PlanId, Number of OS Types]: {Azure plan=2, Azure plan for DevTest=4}".toLowerCase(),
               status.getStatusShortExplanation().toLowerCase());
    }


    /**
     * Tests no OS records.
     *
     * @throws PipelineStageException when there is an exception.
     */
    @Test
    public void testNoRecords() throws Exception {
        Path metersTestFile = Paths.get(LicensePriceProcessingStage.class.getClassLoader()
                .getResource("ipAddresses.zip").getPath());

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


        ProbeStageDetails status = tracker.getStageDetails(MockPricingProbeStage.LICENSE_PRICE_PROCESSING);
        assertEquals(StageStatus.SUCCESS, status.getStatus());
        assertEquals("License Price Processing", status.getDescription());
        assertEquals("No meters found for OS.",
                status.getStatusShortExplanation());
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
                .finalStage(new LicensePriceProcessingStage(MockPricingProbeStage.LICENSE_PRICE_PROCESSING)));
    }

}
