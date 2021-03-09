package com.vmturbo.integrations.intersight.targetsync;

import com.cisco.intersight.client.model.AssetTarget;

import org.mockito.Mockito;

/**
 * A factory class to mock {@link AssetTarget} objects for testing.
 */
public class MockAssetTarget {
    private MockAssetTarget() {}

    /**
     * Mock a {@link AssetTarget} instance with the input target MOID.
     *
     * @param targetMoid the target MOID
     * @return the created {@link AssetTarget} instance
     */
    public static AssetTarget withTargetMoid(final String targetMoid) {
        final AssetTarget assetTarget = Mockito.mock(AssetTarget.class);
        Mockito.when(assetTarget.getMoid()).thenReturn(targetMoid);
        return assetTarget;
    }
}
