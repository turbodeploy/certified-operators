package com.vmturbo.topology.processor.targets;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test class of {@link CustomScopingOperationLibrary}.
 */
public class CustomScopingOperationLibraryTest {
    /**
     * Test that we get the correct operation for AZURE probes and that VCENTER gets back no
     * operation.
     */
    @Test
    public void testCustomScopingOperationLibraryTest() {
        CustomScopingOperationLibrary library = new CustomScopingOperationLibrary();
        Assert.assertTrue(library.getCustomScopingOperation(SDKProbeType.AZURE).get() instanceof
            BusinessAccountBySubscriptionIdCustomScopingOperation);
        Assert.assertEquals(Optional.empty(),
            library.getCustomScopingOperation(SDKProbeType.VCENTER));
    }
}
