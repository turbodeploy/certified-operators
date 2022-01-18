package com.vmturbo.integrations.intersight.targetsync;

import java.util.Collections;

import com.cisco.intersight.client.model.AssetScopedTargetConnection;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetTarget.TargetTypeEnum;

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

    /**
     * Mock a {@link AssetTarget} instance with the input target MOID and the input target type.
     * @param targetMoid the target MOID
     * @param targetType the target type
     * @return the created {@link AssetTarget} instance
     */
    public static AssetTarget withTargetMoidAndType(final String targetMoid, final AssetTarget.TargetTypeEnum targetType) {
        final AssetTarget assetTarget = Mockito.mock(AssetTarget.class);
        Mockito.when(assetTarget.getMoid()).thenReturn(targetMoid);
        Mockito.when(assetTarget.getTargetType()).thenReturn(targetType);
        return assetTarget;
    }

    /**
     * Mock a {@link AssetTarget} of MSSQL server type with scope.
     *
     * @param targetMoid the target MOID
     * @param scopeId the id of the scope
     * @return the created {@link AssetTarget} instance
     */
    public static AssetTarget mssql(final String targetMoid, final String scopeId) {
        final AssetScopedTargetConnection conn = Mockito.mock(AssetScopedTargetConnection.class);
        Mockito.when(conn.getScope()).thenReturn(scopeId);

        final AssetTarget assetTarget = Mockito.mock(AssetTarget.class);
        Mockito.when(assetTarget.getMoid()).thenReturn(targetMoid);
        Mockito.when(assetTarget.getTargetType()).thenReturn(TargetTypeEnum.MICROSOFTSQLSERVER);
        Mockito.when(assetTarget.getConnections()).thenReturn(Collections.singletonList(conn));

        return assetTarget;
    }
}
