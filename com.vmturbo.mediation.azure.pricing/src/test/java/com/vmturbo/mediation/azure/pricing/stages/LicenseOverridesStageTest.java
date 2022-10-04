package com.vmturbo.mediation.azure.pricing.stages;

import static com.vmturbo.mediation.azure.pricing.stages.LicenseOverridesStage.BASE_OS_SET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.fetcher.MockAccount;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.azure.pricing.resolver.MeterDescriptorsFileResolver;
import com.vmturbo.mediation.azure.pricing.stages.meterprocessing.IPMeterProcessingStage;
import com.vmturbo.mediation.azure.pricing.util.VmSizeParser;
import com.vmturbo.mediation.azure.pricing.util.VmSizeParserImpl;
import com.vmturbo.mediation.cost.parser.azure.VMSizes.VMSize;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests for LicenseOverridesStageTest.
 */
public class LicenseOverridesStageTest {

    VmSizeParser parser = mock(VmSizeParserImpl.class);
    IPropertyProvider propertyProvider = mock(IPropertyProvider.class);

    /**
     * Tests Licence override stage.
     *
     * @throws Exception throws Exception.
     */
    @Test
    public void testLicenseOverride() throws Exception {
        //test file does not matter for this stage. hence using an existing one.
        Path metersTestFile = Paths.get(LicenseOverridesStageTest.class.getClassLoader()
                .getResource("resolvertest.zip")
                .getPath());
        ProbeStageTracker<MockPricingProbeStage> tracker = new ProbeStageTracker<>(
                MockPricingProbeStage.DISCOVERY_STAGES);
        PricingWorkspace workspace;
        VMSize vmSize = getVMSize("Standard_DS12-1_v2", 2);
        VMSize vmSizeOne = getVMSize("Standard_DS12-2_v2", 2);
        VMSize vmSizeTwo = getVMSize("Standard_DS13-2_v2", null);

        try (PricingPipelineContext<MockPricingProbeStage> context = new PricingPipelineContext(
                "test", tracker)) {
            PricingPipeline<Path, PricingWorkspace> pipeline = makePipeline(context);
            when(parser.getAllSizes()).thenReturn(
                    Stream.of(vmSize, vmSizeOne, vmSizeTwo).collect(Collectors.toSet()));
            when(propertyProvider.getProperty(BASE_OS_SET)).thenReturn(
                    Sets.immutableEnumSet(OSType.LINUX, OSType.SUSE, OSType.RHEL, OSType.WINDOWS));
            workspace = pipeline.run(metersTestFile);
            workspace.getOnDemandBuilder("Azure Plan", "eastus");
            workspace.getBuilders();
            Map<String, LicenseOverrides> licenceOverrideMap =
                    workspace.getPriceTableBuilderForPlan("Azure Plan")
                            .getOnDemandLicenseOverridesMap();
            assertEquals(2, licenceOverrideMap.size());
            LicenseOverrides override = licenceOverrideMap.get(
                    "azure::VMPROFILE::Standard_DS12-1_v2");
            assertNotNull(override);
            assertEquals(4, override.getLicenseOverrideCount());
            assertEquals(2, override.getLicenseOverride(0).getOverrideValue().getNumOfCores(), 0);
        }
    }

    private PricingPipeline<Path, PricingWorkspace> makePipeline(
            @Nonnull PricingPipelineContext context) {
        return new PricingPipeline<>(PipelineDefinition.<MockAccount, Path, PricingPipelineContext<MockPricingProbeStage>>newBuilder(context)
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
                .finalStage(new LicenseOverridesStage(MockPricingProbeStage.LICENSE_OVERRIDES, parser,
                        propertyProvider)));
    }

    private VMSize getVMSize(String name, Integer override) {
        return new VMSize(name, null, null, null, null, null, null, null, null, null,
                new HashSet<>(), null, null, null, override);
    }
}
