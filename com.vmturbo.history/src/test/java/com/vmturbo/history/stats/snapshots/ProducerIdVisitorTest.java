package com.vmturbo.history.stats.snapshots;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProviderInformation;

/**
 * Test {@link ProducerIdVisitor}.
 */
public class ProducerIdVisitorTest {

    /**
     * Test {@link ProviderInformation#isMultiple()} when multiple new providerId are added.
     */
    @Test
    public void testProviderInformationGetMultiple() {
        ProviderInformation providerInfo = new ProviderInformation();
        providerInfo.newProviderId(1111L);
        providerInfo.newProviderId(2222L);
        providerInfo.newProviderId(2222L);
        assertTrue(providerInfo.isMultiple());
    }
}