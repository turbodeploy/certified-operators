package com.vmturbo.mediation.azure.pricing;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;

/**
 * AzurePricingProbe Test.
 */
public class AzurePricingProbeTest {
    private AzurePricingAccount account;
    private AzurePricingProbe probe;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        account = Mockito.mock(AzurePricingAccount.class);
        probe = new AzurePricingProbe();
        final IProbeContext context = Mockito.mock(IProbeContext.class);
        Mockito.when(context.getPropertyProvider()).thenReturn(IProbePropertySpec::getDefaultValue);
        probe.initialize(context, null);
    }

    /**
     * Test discoverTarget.
     * TODO: update tests when the method is fully implemented.
     *
     * @throws InterruptedException exception from method.
     */
    @Test
    public void testDiscoverTarget() throws InterruptedException {
        DiscoveryResponse response = probe.discoverTarget(account);
        Assert.assertNotNull(response);
    }

    /**
     * Test getAccountDefinitionClass.
     */
    @Test
    public void testGetAccountDefinitionClass() {
        Assert.assertEquals(AzurePricingAccount.class, probe.getAccountDefinitionClass());
    }

    /**
     * Test validateTarget.
     * TODO: update once discoveryController is implememnted.
     *
     * @throws InterruptedException exception from method.
     */
    @Test
    public void testValidateTarget() throws InterruptedException {
        Assert.assertNotNull(probe.validateTarget(account));
    }
}
