package com.vmturbo.mediation.azure.pricing.stages.meterprocessing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.mediation.azure.common.storage.StorageTier;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.MockPricingProbeStage;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;

/**
 * Test {@link AbstractStorageTierMeterProcessingStage}.
 */
public class AbstractStorageTierMeterProcessingStageTest {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Test {@link AbstractStorageTierMeterProcessingStage#addPricingForResolvedMeters(PricingWorkspace, Collection)}.
     */
    @Test
    public void testAddPricingForResolvedMeters() {

        AzureMeterDescriptor azureMeterDescriptor1 = mock(AzureMeterDescriptor.class);
        when(azureMeterDescriptor1.getSkus()).thenReturn(Arrays.asList(StorageTier.MANAGED_STANDARD.name()));
        AzureMeterDescriptor azureMeterDescriptor2 = mock(AzureMeterDescriptor.class);
        when(azureMeterDescriptor2.getSkus()).thenReturn(Arrays.asList(StorageTier.MANAGED_PREMIUM.name()));
        AzureMeterDescriptor azureMeterDescriptor3 = mock(AzureMeterDescriptor.class);
        when(azureMeterDescriptor3.getSkus()).thenReturn(Arrays.asList(StorageTier.UNMANAGED_PREMIUM.name()));
        AzureMeterDescriptor azureMeterDescriptor4 = mock(AzureMeterDescriptor.class);
        when(azureMeterDescriptor4.getSkus()).thenReturn(Arrays.asList(StorageTier.UNMANAGED_STANDARD.name()));

        ResolvedMeter managedStandardResolvedMeter1 = mock(ResolvedMeter.class);
        when(managedStandardResolvedMeter1.getDescriptor()).thenReturn(azureMeterDescriptor1);
        ResolvedMeter managedPremiumResolvedMeter1 = mock(ResolvedMeter.class);
        when(managedPremiumResolvedMeter1.getDescriptor()).thenReturn(azureMeterDescriptor2);
        ResolvedMeter unmanagedPremiumResolvedMeter1 = mock(ResolvedMeter.class);
        when(unmanagedPremiumResolvedMeter1.getDescriptor()).thenReturn(azureMeterDescriptor3);
        ResolvedMeter unmanagedStandardResolvedMeter1 = mock(ResolvedMeter.class);
        when(unmanagedStandardResolvedMeter1.getDescriptor()).thenReturn(azureMeterDescriptor4);

        Map<MeterType, List<ResolvedMeter>> resolvedMeterByMeterType = new HashMap<>();
        resolvedMeterByMeterType.put(MeterType.Storage, Arrays.asList(managedStandardResolvedMeter1, managedPremiumResolvedMeter1,
                unmanagedPremiumResolvedMeter1, unmanagedStandardResolvedMeter1));


        FakeStorageTierMeterProcessingStage fakeStProcessingStage = new FakeStorageTierMeterProcessingStage(
                MockPricingProbeStage.FAKE_STORAGE_TIER_PROCESSING);



        PricingWorkspace pricingWorkspace = new PricingWorkspace(new HashMap<>());

        String message = fakeStProcessingStage.addPricingForResolvedMeters(pricingWorkspace, Arrays.asList(managedStandardResolvedMeter1, managedPremiumResolvedMeter1,
                unmanagedPremiumResolvedMeter1, unmanagedStandardResolvedMeter1));

        // verify the unmanaged storage tiers are put back to pricingWorkspace
        assertEquals(3, pricingWorkspace.getResolvedMeterByMeterType().get(MeterType.Storage).size());
    }

}

