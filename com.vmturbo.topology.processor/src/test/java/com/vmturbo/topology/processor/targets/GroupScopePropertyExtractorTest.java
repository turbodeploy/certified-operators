package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import io.jsonwebtoken.lang.Strings;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.topology.processor.targets.GroupScopeResolver.GroupScopedEntity;

public class GroupScopePropertyExtractorTest {

    private static Long oid = 1L;

    private static Long guestLoadOid = 2333L;

    private static String displayName = "foobar";

    private static double vcpuCapacity = 200.0;

    private static double vmemCapacity = 300.0;

    private static double ballooningCapacity = 400.0;

    private static String storagePrefix = "test_storage_";

    private static String storageSuffix1 = "storage1";

    private static String storageSuffix2 = "storage2";

    private static String ipAddress1 = "10.10.150.160";

    private static String ipAddress2 = "10.10.150.170";


    @Test
    public void testAllPropertiesPresent() throws Exception {
        TopologyEntityDTO vmDTO = createTopologyEntity(oid, displayName, EntityState.MAINTENANCE,
                vcpuCapacity, vmemCapacity, ballooningCapacity,
                ImmutableList.of(ipAddress1, ipAddress2),
                ImmutableList.of(storagePrefix + storageSuffix1, storagePrefix + storageSuffix2));
        GroupScopedEntity vm = new GroupScopedEntity(vmDTO, Optional.of(String.valueOf(guestLoadOid)));
        Optional<String> testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.UUID, vm);
        assertTrue(testValue.isPresent());
        assertEquals(String.valueOf(oid), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.GUEST_LOAD_UUID, vm);
        assertTrue(testValue.isPresent());
        assertEquals(String.valueOf(guestLoadOid), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.DISPLAY_NAME, vm);
        assertTrue(testValue.isPresent());
        assertEquals(displayName, testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.STATE, vm);
        assertTrue(testValue.isPresent());
        assertEquals(EntityState.MAINTENANCE, EntityState.valueOf(testValue.get()));
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VCPU_CAPACITY, vm);
        assertTrue(testValue.isPresent());
        assertEquals(Double.toString(vcpuCapacity), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VMEM_CAPACITY, vm);
        assertTrue(testValue.isPresent());
        assertEquals(Double.toString(vmemCapacity), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.MEM_BALLOONING, vm);
        assertTrue(testValue.isPresent());
        assertEquals(Double.toString(ballooningCapacity), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VSTORAGE_KEY_PREFIX, vm);
        assertTrue(testValue.isPresent());
        assertEquals(storagePrefix, testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.IP_ADDRESS, vm);
        assertTrue(testValue.isPresent());
        Set<String> ipAddresses = Strings.commaDelimitedListToSet(testValue.get());
        assertEquals(2, ipAddresses.size());
        assertTrue(ipAddresses.contains(ipAddress1));
        assertTrue(ipAddresses.contains(ipAddress2));
    }

    @Test
    public void testAllPropertiesMissing() throws Exception {
        TopologyEntityDTO vmDTO = createTopologyEntity(oid, null, null,
                0, 0, 0,
                Collections.EMPTY_LIST,
                Collections.EMPTY_LIST);
        GroupScopedEntity vm = new GroupScopedEntity(vmDTO, Optional.empty());
        // OID is always present
        Optional<String> testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.UUID, vm);
        assertTrue(testValue.isPresent());
        assertEquals(String.valueOf(oid), testValue.get());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.GUEST_LOAD_UUID, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.DISPLAY_NAME, vm);
        assertFalse(testValue.isPresent());
        // EntityState is always present
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.STATE, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VCPU_CAPACITY, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VMEM_CAPACITY, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.MEM_BALLOONING, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VSTORAGE_KEY_PREFIX, vm);
        assertFalse(testValue.isPresent());
        testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.IP_ADDRESS, vm);
        assertFalse(testValue.isPresent());
    }

    @Test
    public void testMismatchedStoragePrefixes() throws Exception {
        TopologyEntityDTO vmDTO = createTopologyEntity(oid, null, null,
                0, 0, 0,
                Collections.EMPTY_LIST,
                ImmutableList.of(storagePrefix + storageSuffix1, "bogusPrefix" + storageSuffix2));
        GroupScopedEntity vm = new GroupScopedEntity(vmDTO, Optional.empty());
        Optional<String> testValue = GroupScopePropertyExtractor
                .extractEntityProperty(EntityPropertyName.VSTORAGE_KEY_PREFIX, vm);
        assertFalse(testValue.isPresent());
    }

    private TopologyEntityDTO createTopologyEntity(@Nonnull Long oid, String displayName,
                                                   EntityState entityState,
                                                   double vcpuCap, double vmemCap,
                                                   double ballooningCap,
                                                   @Nonnull List<String> ipAddresses,
                                                   @Nonnull List<String> vstorageKeys) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        if (displayName != null) {
            builder.setDisplayName(displayName);
        }
        if (entityState != null) {
            builder.setEntityState(entityState);
        }
        if (vcpuCap > 0) {
            addCommoditySoldWithCapacity(builder, CommodityType.VCPU_VALUE, vcpuCap);
        }
        if (vmemCap > 0) {
            addCommoditySoldWithCapacity(builder, CommodityType.VMEM_VALUE, vmemCap);

        }
        if (ballooningCap > 0) {
            addCommoditySoldWithCapacity(builder, CommodityType.BALLOONING_VALUE, ballooningCap);

        }
        vstorageKeys.forEach(keyVal -> builder
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.VSTORAGE_VALUE)
                                .setKey(keyVal))
                        .build()));

        addIpAddresses(builder, ipAddresses);
        return builder.build();
    }

    private void addIpAddresses(@Nonnull TopologyEntityDTO.Builder topoEntityBuilder,
                                @Nonnull List<String> ipAddresses) {
        if (ipAddresses.isEmpty()) {
            return;
        }
        final VirtualMachineInfo.Builder vmInfoBuilder = VirtualMachineInfo.newBuilder();
        ipAddresses.forEach(ipAddress -> vmInfoBuilder.addIpAddresses(
                IpAddress.newBuilder().setIpAddress(ipAddress).build()));
        topoEntityBuilder.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(vmInfoBuilder));
    }

    private void addCommoditySoldWithCapacity(@Nonnull TopologyEntityDTO.Builder topoEntityBuilder,
                                              int commType,
                                              double capacity) {
        topoEntityBuilder.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(commType))
                .setCapacity(capacity)
                .build());
    }
}