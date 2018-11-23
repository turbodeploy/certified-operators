package com.vmturbo.topology.processor.conversions;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

public class TopologyToSdkEntityConverterTest {

    private EntityStore entityStoreMock = Mockito.mock(EntityStore.class);

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    /**
     * The class under test
     */
    private TopologyToSdkEntityConverter topologyToSdkEntityConverter =
            new TopologyToSdkEntityConverter(entityStoreMock, targetStore);

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
        Mockito.doReturn(Optional.of(matchingEntity)).when(entityStoreMock).getEntity(oid);

        Mockito.doReturn(Optional.of("vmm-01")).when(targetStore).getTargetAddress(targetId);
        Mockito.doReturn(Optional.of(SDKProbeType.VMM))
                .when(targetStore).getProbeTypeForTarget(targetId);

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
        Mockito.doReturn(Optional.of(matchingEntity)).when(entityStoreMock).getEntity(oid);

        Mockito.doReturn(Optional.of("vmm-01")).when(targetStore).getTargetAddress(targetId);
        Mockito.doReturn(Optional.of(SDKProbeType.VMM))
                .when(targetStore).getProbeTypeForTarget(targetId);

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
}
