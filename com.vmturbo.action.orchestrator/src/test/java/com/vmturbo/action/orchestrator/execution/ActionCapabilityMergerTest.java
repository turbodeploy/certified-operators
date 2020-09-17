package com.vmturbo.action.orchestrator.execution;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement.ProviderScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionCapabilityMerger}.
 */
public class ActionCapabilityMergerTest {

    /**
     * Test merging of capabilities that have no scope fields populated.
     */
    @Test
    public void testCapabilitiesWithoutScope() {
        // ARRANGE
        final ActionCapabilityElement capabilitySupported = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.SUPPORTED)
                .setDisruptive(true)
                .build();
        final ActionCapabilityElement capabilityNotSupported = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.NOT_SUPPORTED)
                .setReversible(false)
                .build();

        // ACT
        final MergedActionCapability result = merge(capabilitySupported, capabilityNotSupported);

        // ASSERT
        // "Not Supported" capability should have priority.
        Assert.assertEquals(SupportLevel.UNSUPPORTED, result.getSupportLevel());
        Assert.assertNull(result.getDisruptive());
        Assert.assertEquals(Boolean.FALSE, result.getReversible());
    }

    /**
     * Test merging of capabilities that have no scope fields populated.
     */
    @Test
    public void testCapabilitiesWithScope() {
        // ARRANGE
        final ActionCapabilityElement capabilitySupported = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.SUPPORTED)
                .setProviderScope(ProviderScope.newBuilder()
                        .setProviderType(EntityType.COMPUTE_TIER)
                        .build())
                .setDisruptive(false)
                .build();
        final ActionCapabilityElement capabilityNotExecutable = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                .setProviderScope(ProviderScope.newBuilder()
                        .setProviderType(EntityType.COMPUTE_TIER)
                        .build())
                .setReversible(false)
                .build();
        final ActionCapabilityElement capabilityNotSupported = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.NOT_SUPPORTED)
                .setProviderScope(ProviderScope.newBuilder()
                        .setProviderType(EntityType.COMPUTE_TIER)
                        .build())
                .setDisruptive(true)
                .build();

        // ACT
        final MergedActionCapability result = merge(capabilitySupported, capabilityNotExecutable,
                capabilityNotSupported);

        // ASSERT
        // Merged capability should pick "Not Supported" level.
        Assert.assertEquals(SupportLevel.UNSUPPORTED, result.getSupportLevel());
        Assert.assertEquals(Boolean.TRUE, result.getDisruptive());
        Assert.assertEquals(Boolean.FALSE, result.getReversible());
    }

    /**
     * Test merging of capabilities when one of them has a scope, another one doesn't.
     */
    @Test
    public void testCapabilitiesWithScopeAndWithoutScope() {
        // ARRANGE
        final ActionCapabilityElement capabilityWithScope = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.SUPPORTED)
                .setProviderScope(ProviderScope.newBuilder()
                        .setProviderType(EntityType.COMPUTE_TIER)
                        .build())
                .setDisruptive(true)
                .build();
        final ActionCapabilityElement capabilityWithoutScope = ActionCapabilityElement
                .newBuilder()
                .setActionType(ActionType.SCALE)
                .setActionCapability(ActionCapability.NOT_SUPPORTED)
                .setDisruptive(false)
                .build();

        // ACT
        final MergedActionCapability result = merge(capabilityWithScope, capabilityWithoutScope);

        // ASSERT
        // Merged capability should pick capability with scope.
        Assert.assertEquals(SupportLevel.SUPPORTED, result.getSupportLevel());
        Assert.assertEquals(Boolean.TRUE, result.getDisruptive());
        Assert.assertNull(result.getReversible());
    }

    /**
     * Test merging of capabilities when capability list is empty.
     */
    @Test
    public void testEmptyCapabilities() {
        // ACT
        final MergedActionCapability result = merge();

        // ASSERT
        // Merged capability should pick capability with scope.
        Assert.assertEquals(SupportLevel.SHOW_ONLY, result.getSupportLevel());
        Assert.assertNull(result.getDisruptive());
        Assert.assertNull(result.getReversible());
    }

    private static MergedActionCapability merge(
            @Nonnull final ActionCapabilityElement... capabilities) {
        return new ActionCapabilityMerger().merge(Arrays.asList(capabilities));
    }
}
