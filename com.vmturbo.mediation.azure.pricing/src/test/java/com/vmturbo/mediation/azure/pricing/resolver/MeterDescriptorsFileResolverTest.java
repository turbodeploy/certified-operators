package com.vmturbo.mediation.azure.pricing.resolver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.VMSizes;
import com.vmturbo.mediation.cost.parser.azure.VMSizes.VMSize;

/**
 * Tests for MeterDescriptorsFileResolver.
 */
public class MeterDescriptorsFileResolverTest {
    private static final String METER_ID_E64S_V3 = "ea6c338e-4057-46c4-8b26-a208a5db35bd";
    private static final String METER_ID_DS12_V2 = "e3f81bab-5147-49fc-a7e5-f61f069bfc33";
    private static final Set<String> VM_SIZES_E64 = ImmutableSet.of("Standard_E64_v3", "Standard_E64s_v3");
    private static final Set<String> VM_SIZES_E64_WITH_PRICED_AS = ImmutableSet.of("Standard_E64_v3",
        "Standard_E64s_v3", "Standard_E64-32s_v3", "Standard_E64-16s_v3");
    private static final Set<String> VM_SIZES_D12 = ImmutableSet.of("Standard_D12_v2", "Standard_DS12_v2");
    private static final Set<String> VM_SIZES_D12_WITH_PRICED_AS = ImmutableSet.of("Standard_D12_v2",
        "Standard_DS12_v2", "Standard_DS12-1_v2");

    /**
     * Test that for instance types, the file-based resolver adds in other instance
     * types that share the same pricing as indicated by the azure-vm-sizes.yml file.
     */
    @Test
    public void testAddedPricedAsSkus() {
        Optional<AzureMeterDescriptor> optDescriptor = AzureMeterDescriptors.describeMeter(METER_ID_E64S_V3);
        assertTrue(optDescriptor.isPresent());
        AzureMeterDescriptor descriptor = optDescriptor.get();

        assertEquals(VM_SIZES_E64, new HashSet<>(descriptor.getSkus()));

        MeterResolver resolver = new MeterDescriptorsFileResolver();

        AzureMeter vmMeter = mock(AzureMeter.class);
        when(vmMeter.getMeterId()).thenReturn(METER_ID_E64S_V3);

        Optional<AzureMeterDescriptor> resolvedMeter = resolver.resolveMeter(vmMeter);
        assertTrue(resolvedMeter.isPresent());
        AzureMeterDescriptor resolvedDescriptor = resolvedMeter.get();

        // Sizes that share pricing should have been added to the descriptor's SKUs

        assertEquals(VM_SIZES_E64_WITH_PRICED_AS, new HashSet<>(resolvedDescriptor.getSkus()));

        // Other fields should be identical

        assertEquals(descriptor.getType(), resolvedDescriptor.getType());
        assertEquals(descriptor.getSubType(), resolvedDescriptor.getSubType());
        assertEquals(descriptor.getRegions(), resolvedDescriptor.getRegions());
        assertEquals(descriptor.getRedundancy(), resolvedDescriptor.getRedundancy());
        assertEquals(descriptor.getVCoreCount(), resolvedDescriptor.getVCoreCount());
    }

    /**
     * Test that for instance types, the file-based resolver adds in other instance
     * types that share the same pricing as indicated by the azure-vm-sizes.yml file,
     * EXCEPT when there is also explicit pricing. Note that this test may be a bit fragile
     * because it's testing how the code deals with a very quirky situation, and if that
     * situation is resolved it will probably break. For that reason it's ignored by default.
     */
    @Ignore
    @Test
    public void testPricedAsDoesNotOverride() {
        Optional<AzureMeterDescriptor> optDescriptor = AzureMeterDescriptors.describeMeter(METER_ID_DS12_V2);
        assertTrue(optDescriptor.isPresent());
        AzureMeterDescriptor descriptor = optDescriptor.get();

        assertEquals(VM_SIZES_D12, new HashSet<>(descriptor.getSkus()));

        // The azure-vm-sizes.yml says that Standard_DS12-1_v2 and Standard_DS12-2_v2
        // share pricing with Standard_DS12_v2. However below, we won't add in
        // pricing for Standard_DS12-2_v2 because it also has explicit pricing for
        // Windows only.

        VMSize ds12v2 = VMSizes.fromName("Standard_DS12_v2")
                .orElseThrow(() -> new RuntimeException("No such instance type Standard_DS12_v2?"));

        assertTrue(ds12v2.getSizesSharingPricing().contains("Standard_DS12-1_v2"));
        assertTrue(ds12v2.getSizesSharingPricing().contains("Standard_DS12-2_v2"));

        MeterResolver resolver = new MeterDescriptorsFileResolver();

        AzureMeter vmMeter = mock(AzureMeter.class);
        when(vmMeter.getMeterId()).thenReturn(METER_ID_DS12_V2);

        Optional<AzureMeterDescriptor> resolvedMeter = resolver.resolveMeter(vmMeter);
        assertTrue(resolvedMeter.isPresent());
        AzureMeterDescriptor resolvedDescriptor = resolvedMeter.get();

        // Sizes that share pricing should have been added to the descriptor's SKUs, unless
        // there is explicit pricing. For some reason there is explicit pricing for
        // Standard_DS12-2_v2, for Windows only (but not Standard_DS12-1_v2?) so it's not included.

        assertEquals(VM_SIZES_D12_WITH_PRICED_AS, new HashSet<>(resolvedDescriptor.getSkus()));

        // Other fields should be identical

        assertEquals(descriptor.getType(), resolvedDescriptor.getType());
        assertEquals(descriptor.getSubType(), resolvedDescriptor.getSubType());
        assertEquals(descriptor.getRegions(), resolvedDescriptor.getRegions());
        assertEquals(descriptor.getRedundancy(), resolvedDescriptor.getRedundancy());
        assertEquals(descriptor.getVCoreCount(), resolvedDescriptor.getVCoreCount());
    }
}
