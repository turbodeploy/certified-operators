package com.vmturbo.mediation.udt;

import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.VENDOR;
import static com.vmturbo.platform.sdk.common.util.SDKUtil.VENDOR_ID;
import static java.util.Collections.emptySet;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.builders.GenericEntityBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The class for {@link UdtProbeConverter}.
 */
public class UdtProbeConverterTest {

    /**
     * The method tests that UDT entities are correctly converted to a discovery response.
     * BUSINESS_TRANSACTION(UDT)
     *                         |_ SERVICE(UDT)
     *                                       |_ VIRTUAL_MACHINE(topology entity)
     */
    @Test
    public void testConvertToResponse() {
        long serviceOid = 1000L;
        UdtEntity btEntity = new UdtEntity(EntityType.BUSINESS_TRANSACTION, "10", "bt-0",
                Collections.singleton(new UdtChildEntity(serviceOid, EntityType.SERVICE)));
        UdtEntity serviceEntity = new UdtEntity(EntityType.SERVICE, "20", "svc-0",
                Collections.singleton(new UdtChildEntity(2000L, EntityType.VIRTUAL_MACHINE)));
        // btEntity's child is the serviceEntity
        serviceEntity.setOid(serviceOid);
        UdtProbeConverter converter = new UdtProbeConverter(Sets.newHashSet(btEntity, serviceEntity));
        DiscoveryResponse response = converter.createDiscoveryResponse();
        List<EntityDTO> dtos = response.getEntityDTOList();

        Assert.assertEquals(3, dtos.size());

        EntityDTO vmDto = dtos.stream().filter(dto -> dto.getEntityType() == EntityType.VIRTUAL_MACHINE).findFirst().orElse(null);
        EntityDTO svcDto = dtos.stream().filter(dto -> dto.getEntityType() == EntityType.SERVICE).findFirst().orElse(null);
        EntityDTO btDto = dtos.stream().filter(dto -> dto.getEntityType() == EntityType.BUSINESS_TRANSACTION).findFirst().orElse(null);

        Assert.assertNotNull(vmDto);
        Assert.assertNotNull(svcDto);
        Assert.assertNotNull(btDto);

        Assert.assertEquals(serviceOid, Long.parseLong(svcDto.getId()));    // Service DTO has ID == Oid (not UDT ID).

        Assert.assertEquals(EntityDTO.EntityOrigin.DISCOVERED, btDto.getOrigin());
        Assert.assertEquals(EntityDTO.EntityOrigin.DISCOVERED, svcDto.getOrigin());
        Assert.assertEquals(EntityDTO.EntityOrigin.PROXY, vmDto.getOrigin());

        String expectedKeySvc2Bt = ConverterUtils.getApplicationCommodityKey(svcDto.getId(), btDto.getId());
        verifyIsConnectedByApplicationCommodity(btDto, svcDto, expectedKeySvc2Bt);

        String expectedKeyVm2Svc = ConverterUtils.getApplicationCommodityKey(vmDto.getId(), svcDto.getId());
        verifyIsConnectedByApplicationCommodity(svcDto, vmDto, expectedKeyVm2Svc);

    }

    /**
     * The method tests that UdtProbeConverter correctly creates UDT entity. It should have
     * VENDOR and VENDOR_ID.
     */
    @Test
    public void testCreateUdtDto() {
        String id = "102030";
        String name = "svc-1";
        EntityType type = EntityType.SERVICE;
        UdtProbeConverter converter = new UdtProbeConverter(Collections.emptySet());
        UdtEntity udtEntity = new UdtEntity(type, id, name, emptySet());
        EntityDTO dto = converter.createUdtDto(udtEntity).build();

        Assert.assertEquals(id, dto.getId());
        Assert.assertEquals(name, dto.getDisplayName());
        Assert.assertEquals(type, dto.getEntityType());

        List<EntityProperty> props = dto.getEntityPropertiesList();
        EntityProperty vendorProp = props.stream()
                .filter(p -> p.getName().equals(VENDOR)).findFirst().orElse(null);
        EntityProperty vendorIdProp = props.stream()
                .filter(p -> p.getName().equals(VENDOR_ID)).findFirst().orElse(null);

        Assert.assertNotNull(vendorProp);
        Assert.assertNotNull(vendorIdProp);
        Assert.assertEquals(id, vendorIdProp.getValue());
    }

    /**
     * The method tests that UdtProbeConverter correctly creates UDT child entity.
     */
    @Test
    public void testCreateUdtChildDto() {
        UdtProbeConverter converter = new UdtProbeConverter(Collections.emptySet());
        String id = "405060";
        EntityType type = EntityType.VIRTUAL_MACHINE;
        EntityDTO dto = converter.createUdtChildDto(id, type).build();
        Assert.assertEquals(id, dto.getId());
        Assert.assertEquals(type, dto.getEntityType());
    }

    /**
     * The method tests that UdtProbeConverter correctly links two entities using APPLICATION
     * commodity.
     */
    @Test
    public void testLinkEntities() {
        String expectedKey = ConverterUtils.getApplicationCommodityKey("B", "A");
        UdtProbeConverter converter = new UdtProbeConverter(Collections.emptySet());
        GenericEntityBuilder udtEntity = EntityBuilders.entity("A").entityType(EntityType.SERVICE);
        GenericEntityBuilder udtChild = EntityBuilders.entity("B").entityType(EntityType.VIRTUAL_MACHINE);
        converter.linkEntities(udtEntity, udtChild);

        EntityDTO udtEntityDto = udtEntity.build();
        EntityDTO udtChildDto = udtChild.build();

        verifyIsConnectedByApplicationCommodity(udtEntityDto, udtChildDto, expectedKey);
    }

    /**
     * The method checks if entities are linked using APPLICATION commodity.
     *
     * @param udtEntityDto - DTO from UDT entity.
     * @param udtChildDto  - DTO from UDT`s child entity.
     * @param expectedKey  - expected commodity key.
     */
    private void verifyIsConnectedByApplicationCommodity(EntityDTO udtEntityDto, EntityDTO udtChildDto, String expectedKey) {
        List<CommodityBought> commoditiesBoughts = udtEntityDto.getCommoditiesBoughtList();
        Assert.assertFalse(commoditiesBoughts.isEmpty());

        CommodityBought commodityBought = commoditiesBoughts.get(0);
        List<CommodityDTO> commonDTOS = commodityBought.getBoughtList();
        Assert.assertFalse(commonDTOS.isEmpty());
        CommodityDTO commodityBoughtApp = commonDTOS.get(0);
        Assert.assertEquals(CommodityType.APPLICATION, commodityBoughtApp.getCommodityType());
        Assert.assertEquals(expectedKey, commodityBoughtApp.getKey());

        List<CommodityDTO> soldList = udtChildDto.getCommoditiesSoldList();
        Assert.assertFalse(soldList.isEmpty());
        CommodityDTO sold = soldList.get(0);
        Assert.assertEquals(CommodityType.APPLICATION, sold.getCommodityType());
        Assert.assertEquals(expectedKey, sold.getKey());
    }

}
