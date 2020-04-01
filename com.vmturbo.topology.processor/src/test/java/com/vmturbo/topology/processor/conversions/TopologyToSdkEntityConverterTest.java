package com.vmturbo.topology.processor.conversions;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class TopologyToSdkEntityConverterTest {

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private EntityStore entityStore = Mockito.spy(new EntityStore(targetStore, identityProvider,
        Clock.systemUTC()));

    /**
     * The class under test
     */
    private TopologyToSdkEntityConverter topologyToSdkEntityConverter =
        new TopologyToSdkEntityConverter(entityStore, targetStore, Mockito.mock(GroupScopeResolver.class));

    /**
     * A simple test verifying that basic data is carried over after converting a TopologyEntityDTO
     * to an EntityDTO.
     */
    @Test
    public void testConvertToEntityDTO() {
        // Create the TopologyEntityDTO to be converted
        final long oid = 93995728L;
        final String displayName = "testVM";
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .setDisplayName(displayName)
                .setOid(oid)
                .build();

        // Set the mocks
        Entity matchingEntity = new Entity(oid, entityType);
        final String uuid = "8333-AF322-6DAA3";
        EntityDTO rawDiscoveredEntityDTO = EntityDTO.newBuilder()
                .setId(uuid)
                .setEntityType(entityType)
                .setDisplayName(displayName)
                .setOrigin(EntityOrigin.DISCOVERED)
                .build();
        final int targetId = 8832213;
        matchingEntity.addTargetInfo(targetId, rawDiscoveredEntityDTO);
        Mockito.doReturn(Optional.of(matchingEntity)).when(entityStore).getEntity(oid);

        mockTarget(targetId, SDKProbeType.VMM, "aaa");

        // Perform the conversion (this is the method under test)
        EntityDTO entityDTO = topologyToSdkEntityConverter.convertToEntityDTO(topologyEntityDTO);

        // Check the output data is correct
        Assert.assertEquals(displayName, entityDTO.getDisplayName());
        Assert.assertEquals(entityType, entityDTO.getEntityType());
        // ID gets set to a probe-meaningful UUID during conversion
        Assert.assertEquals(uuid, entityDTO.getId());
    }


    /**
     * A simple test verifying that commodities in sold list are carried over after converting a TopologyEntityDTO
     * to an EntityDTO.
     */
    @Test
    public void testConvertToEntityDTOWithCommoditySoldList() {
        // Create the TopologyEntityDTO to be converted
        final long oid = 93995728L;
        final String displayName = "testVM";
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final String key1 = "key1";
        final boolean isResizeable = false;
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .setDisplayName(displayName)
                .setOid(oid)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setKey(key1)
                                .setType(1)
                                .build())
                        .setIsResizeable(isResizeable)
                        .build())
                .build();

        // Set the mocks
        Entity matchingEntity = new Entity(oid, entityType);
        final String uuid = "8333-AF322-6DAA3";
        EntityDTO rawDiscoveredEntityDTO = EntityDTO.newBuilder()
                .setId(uuid)
                .setEntityType(entityType)
                .setDisplayName(displayName)
                .setOrigin(EntityOrigin.DISCOVERED)
                .build();
        final int targetId = 8832213;
        matchingEntity.addTargetInfo(targetId, rawDiscoveredEntityDTO);
        Mockito.doReturn(Optional.of(matchingEntity)).when(entityStore).getEntity(oid);

        mockTarget(targetId, SDKProbeType.VMM, "aaa");

        // Perform the conversion (this is the method under test)
        EntityDTO entityDTO = topologyToSdkEntityConverter.convertToEntityDTO(topologyEntityDTO);

        // Check the output data is correct
        Assert.assertEquals(displayName, entityDTO.getDisplayName());
        Assert.assertEquals(entityType, entityDTO.getEntityType());
        Assert.assertEquals(key1, entityDTO.getCommoditiesSold(0).getKey());
        Assert.assertEquals(isResizeable, entityDTO.getCommoditiesSold(0).getResizable());
        // ID gets set to a probe-meaningful UUID during conversion
        Assert.assertEquals(uuid, entityDTO.getId());
    }

    /**
     * Test that entity properties are set for entity with multiple discovery origins.
     */
    @Test
    public void testMultipleTargets() {
        long targetId1 = 8832213L;
        long targetId2 = 23827L;
        String localName1 = "qqq";
        String localName2 = "www";
        EntityType entityType = EntityType.VIRTUAL_MACHINE;
        long oid = 2837187L;
        PerTargetEntityInformation info1 = PerTargetEntityInformation.newBuilder()
                        .setVendorId(localName1).build();
        PerTargetEntityInformation info2 = PerTargetEntityInformation.newBuilder()
                        .setVendorId(localName2).build();
        final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType.getNumber())
                .setDisplayName("fkjd")
                .setOid(oid)
                .setOrigin(Origin.newBuilder()
                       .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                           .putDiscoveredTargetData(targetId1, info1)
                                           .putDiscoveredTargetData(targetId2, info2)))
                .build();

        Entity matchingEntity = new Entity(oid, entityType);
        EntityDTO rawDiscoveredEntityDTO = EntityDTO.newBuilder()
                .setId("qqq")
                .setEntityType(entityType)
                .setDisplayName("fdgds")
                .setOrigin(EntityOrigin.DISCOVERED)
                .build();
        matchingEntity.addTargetInfo(targetId1, rawDiscoveredEntityDTO);
        matchingEntity.addTargetInfo(targetId2, rawDiscoveredEntityDTO);
        Mockito.doReturn(Optional.of(matchingEntity)).when(entityStore).getEntity(oid);

        String address1 = "kdjfhs7";
        String address2 = "42cg4";
        mockTarget(targetId1, SDKProbeType.VCENTER, address1);
        mockTarget(targetId2, SDKProbeType.VCD, address2);

        EntityDTO entityDTO = topologyToSdkEntityConverter.convertToEntityDTO(topologyEntityDTO);

        Map<String, Map<String, List<EntityProperty>>> namespace2name2props = entityDTO.getEntityPropertiesList()
                        .stream().collect(Collectors.groupingBy(EntityProperty::getNamespace,
                                                                HashMap::new,
                                                                Collectors.groupingBy(EntityProperty::getName)));
        Assert.assertEquals(2, namespace2name2props.size());
        checkProps(namespace2name2props.get(address1), localName1);
        checkProps(namespace2name2props.get(address2), localName2);
    }

    private static void checkProps(Map<String, List<EntityProperty>> name2props, String localName) {
        Assert.assertNotNull(name2props);
        Assert.assertTrue(name2props.containsKey(SupplyChainConstants.TARGET_TYPE));
        List<EntityProperty> localNames = name2props.get(SupplyChainConstants.LOCAL_NAME);
        Assert.assertNotNull(localNames);
        Assert.assertEquals(1, localNames.size());
        Assert.assertEquals(localName, localNames.get(0).getValue());
    }

    private Target mockTarget(long id, SDKProbeType type, String address) {
        Target target = Mockito.mock(Target.class);
        String addressField = PredefinedAccountDefinition.Address.name();
        ProbeInfo probe = ProbeInfo.newBuilder().setProbeType(type.getProbeType())
                        .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
                        .setUiProbeCategory(ProbeCategory.HYPERVISOR.toString())
                        .addAccountDefinition(AccountDefEntry.newBuilder()
                                        .setPredefinedDefinition(addressField))
                        .addTargetIdentifierField(addressField)
                        .build();
        Mockito.doReturn(probe).when(target).getProbeInfo();
        Mockito.doReturn(ImmutableList
                        .of(AccountValue.newBuilder().setKey(addressField).setStringValue(address).build()))
                        .when(target).getMediationAccountVals(Mockito.any());

        Mockito.doReturn(Optional.of(target)).when(targetStore).getTarget(id);
        Mockito.doReturn(Optional.of(type)).when(targetStore).getProbeTypeForTarget(id);
        return target;
    }

}
