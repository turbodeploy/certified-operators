package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;


public class ProbeInfoCompatibilityCheckerTest {

    private final ProbeInfoCompatibilityChecker checker = new ProbeInfoCompatibilityChecker();

    private final AccountDefEntry firstEntry = AccountDefEntry.newBuilder()
        .setMandatory(true)
        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
            .setName("name")
            .setDisplayName("displayName")
            .setDescription("description")
            .setIsSecret(true))
        .build();
    private final AccountDefEntry secondEntry = AccountDefEntry.newBuilder()
        .setMandatory(true)
        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
            .setName("other-name")
            .setDisplayName("other-displayName")
            .setDescription("other-description")
            .setIsSecret(true))
        .build();

    @Test
    public void testAreCompatibleProbeType() {
        final ProbeInfo foo = ProbeInfo.newBuilder()
            .setProbeType("foo")
            .setProbeCategory("category")
            .build();
        final ProbeInfo bar = ProbeInfo.newBuilder()
            .setProbeType("bar")
            .setProbeCategory("category")
            .build();

        assertFalse(checker.areCompatible(foo, bar));
        assertTrue(checker.areCompatible(foo, foo));
    }

    @Test
    public void testAreCompatibleProbeCategory() {
        final ProbeInfo hypervisor = ProbeInfo.newBuilder()
            .setProbeType("foo")
            .setProbeCategory("hypervisor")
            .build();
        final ProbeInfo storage = ProbeInfo.newBuilder()
            .setProbeType("foo")
            .setProbeCategory("storage")
            .build();

        assertFalse(checker.areCompatible(hypervisor, storage));
        assertTrue(checker.areCompatible(storage, storage));
    }

    @Test
    public void testAreCompatibleAccountDefinitions() {
        final ProbeInfo a = probeInfoBuilder()
            .addAccountDefinition(firstEntry)
            .addAccountDefinition(secondEntry)
            .build();
        final ProbeInfo b = probeInfoBuilder()
            .addAccountDefinition(secondEntry) // Add account definitions in reverse order
            .addAccountDefinition(firstEntry)
            .build();

        // Order of account definitions should not be considered
        assertTrue(checker.areCompatible(a, b));
        assertTrue(checker.areCompatible(a, a));
    }

    @Test
    public void testTargetIdentifiers() {
        final ProbeInfo a = probeInfoBuilder()
            .addTargetIdentifierField("id1")
            .addTargetIdentifierField("id2")
            .build();
        final ProbeInfo b = probeInfoBuilder()
            .addTargetIdentifierField("id2")
            .addTargetIdentifierField("id1")
            .build();
        final ProbeInfo c = probeInfoBuilder()
            .addTargetIdentifierField("id3")
            .build();

        assertTrue(checker.areCompatible(a, b));
        assertTrue(checker.areCompatible(b, a));
        assertFalse(checker.areCompatible(a, c));
        assertFalse(checker.areCompatible(c, b));
    }

    @Test
    public void testIdentityMetadataChangeToEntity() {
        final ProbeInfo existing = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .build();
        final ProbeInfo newInfo = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id"))
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("foo")))
            .build();
        assertFalse(checker.areCompatible(existing, newInfo));
    }

    @Test
    public void testIdentityMetadataAddNewEntityType() {
        final ProbeInfo existing = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .build();
        final ProbeInfo newInfo = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .build();
        assertTrue(checker.areCompatible(existing, newInfo));
    }

    @Test
    public void testIdentityMetadatRemoveEntityType() {
        final ProbeInfo existingInfo = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .build();
        final ProbeInfo newInfo = probeInfoBuilder()
            .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addNonVolatileProperties(PropertyMetadata.newBuilder()
                    .setName("id")))
            .build();
        assertFalse(checker.areCompatible(existingInfo, newInfo));
    }

    private static ProbeInfo.Builder probeInfoBuilder() {
        return ProbeInfo.newBuilder()
            .setProbeType("foo")
            .setProbeCategory("hypervisor");
    }
}