package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.repository.dto.BusinessUserInfoRepoDTO;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;
import com.vmturbo.repository.dto.DesktopPoolInfoRepoDTO;
import com.vmturbo.repository.dto.GuestOSRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit tests for {@link TopologyEntityDtoConverterTest}.
 */
public class TopologyEntityDtoConverterTest {

    private static double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private TopologyEntityDTO vmTopoDTO;
    private TopologyEntityDTO pmTopoDTO;
    private TopologyEntityDTO dsTopoDTO;
    private TopologyEntityDTO vdcTopoDTO;
    private TopologyEntityDTO networkTopoDTO;
    private TopologyEntityDTO desktopPoolTopoDTO;
    private TopologyEntityDTO businessUserTopoDTO;
    private ServiceEntityRepoDTO vmServiceEntity = new ServiceEntityRepoDTO();

    @Before
    public void setup() throws IOException {
        vmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        vdcTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        networkTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/network-1.dto.json");
        desktopPoolTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/desktopPool-1.dto.json");
        businessUserTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/businessUser-1.dto.json");
        buildVMServiceEntityRepoDTO(vmServiceEntity);
    }

    private void buildVMServiceEntityRepoDTO(@Nonnull final ServiceEntityRepoDTO vmServiceEntity) {
        vmServiceEntity.setDisplayName("test-vm");
        vmServiceEntity.setOid("111");
        vmServiceEntity.setUuid("111");
        vmServiceEntity.setEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        vmServiceEntity.setState(UIEntityState.ACTIVE.apiStr());
        final Map<String, List<String>> tagsMap = new HashMap<>();
        tagsMap.put("key1", Arrays.asList("value1", "value2"));
        tagsMap.put("key2", Collections.singletonList("value1"));
        vmServiceEntity.setTags(tagsMap);
        VirtualMachineInfoRepoDTO virtualMachineInfoRepoDTO = new VirtualMachineInfoRepoDTO();
        IpAddressRepoDTO ipAddressRepoDTO = new IpAddressRepoDTO();
        ipAddressRepoDTO.setElastic(false);
        ipAddressRepoDTO.setIpAddress("10.0.1.15");
        IpAddressRepoDTO ipAddressRepoDTO2 = new IpAddressRepoDTO();
        ipAddressRepoDTO2.setElastic(true);
        ipAddressRepoDTO2.setIpAddress("10.0.1.25");
        IpAddressRepoDTO ipAddressRepoDTO3 = new IpAddressRepoDTO();
        ipAddressRepoDTO3.setElastic(true);
        virtualMachineInfoRepoDTO.setIpAddressInfoList(ImmutableList.of(ipAddressRepoDTO,
                ipAddressRepoDTO2, ipAddressRepoDTO3));
        virtualMachineInfoRepoDTO.setTenancy("DEFAULT");
        virtualMachineInfoRepoDTO.setGuestOsInfo(new GuestOSRepoDTO(OSType.WINDOWS, OSType.WINDOWS.name()));
        vmServiceEntity.setVirtualMachineInfoRepoDTO(virtualMachineInfoRepoDTO);
        final CommoditySoldRepoDTO commoditySoldRepoDTO = new CommoditySoldRepoDTO();
        commoditySoldRepoDTO.setCapacity(123);
        commoditySoldRepoDTO.setKey("test-sold-key");
        commoditySoldRepoDTO.setType(UICommodityType.VMEM.apiStr());
        commoditySoldRepoDTO.setUsed(100);
        commoditySoldRepoDTO.setProviderOid("111");
        commoditySoldRepoDTO.setOwnerOid("111");
        final CommoditySoldRepoDTO commoditySoldRepoDTOTwo = new CommoditySoldRepoDTO();
        commoditySoldRepoDTOTwo.setCapacity(345);
        commoditySoldRepoDTOTwo.setKey("test-sold-key-two");
        commoditySoldRepoDTOTwo.setType(UICommodityType.APPLICATION.apiStr());
        commoditySoldRepoDTOTwo.setUsed(100);
        commoditySoldRepoDTOTwo.setProviderOid("111");
        commoditySoldRepoDTOTwo.setOwnerOid("111");
        final CommoditySoldRepoDTO commoditySoldRepoDTOThree = new CommoditySoldRepoDTO();
        commoditySoldRepoDTOThree.setCapacity(345);
        commoditySoldRepoDTOThree.setKey("test-sold-key-three");
        commoditySoldRepoDTOThree.setType(UICommodityType.VCPU.apiStr());
        commoditySoldRepoDTOThree.setUsed(100);
        commoditySoldRepoDTOThree.setProviderOid("111");
        commoditySoldRepoDTOThree.setOwnerOid("111");
        commoditySoldRepoDTOThree.setHotReplaceSupported(true);
        commoditySoldRepoDTOThree.setHotAddSupported(true);
        commoditySoldRepoDTOThree.setHotRemoveSupported(true);
        vmServiceEntity.setCommoditySoldList(Lists.newArrayList(commoditySoldRepoDTO, commoditySoldRepoDTOTwo, commoditySoldRepoDTOThree));
        final CommodityBoughtRepoDTO commodityBoughtRepoDTO = new CommodityBoughtRepoDTO();
        commodityBoughtRepoDTO.setKey("test-key");
        commodityBoughtRepoDTO.setType(UICommodityType.MEM.apiStr());
        commodityBoughtRepoDTO.setUsed(123);
        commodityBoughtRepoDTO.setProviderOid("222");
        commodityBoughtRepoDTO.setOwnerOid("111");
        final CommodityBoughtRepoDTO commodityBoughtRepoDTOTwo = new CommodityBoughtRepoDTO();
        commodityBoughtRepoDTOTwo.setKey("test-key-two");
        commodityBoughtRepoDTOTwo.setType(UICommodityType.CLUSTER.apiStr());
        commodityBoughtRepoDTOTwo.setUsed(123);
        commodityBoughtRepoDTOTwo.setProviderOid("222");
        commodityBoughtRepoDTOTwo.setOwnerOid("111");
        final CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO =
                new CommoditiesBoughtRepoFromProviderDTO();
        commoditiesBoughtRepoFromProviderDTO.setProviderId(222L);
        commoditiesBoughtRepoFromProviderDTO.setProviderEntityType(14);
        commoditiesBoughtRepoFromProviderDTO.setCommodityBoughtRepoDTOs(
                Lists.newArrayList(commodityBoughtRepoDTO, commodityBoughtRepoDTOTwo));
        vmServiceEntity.setCommoditiesBoughtRepoFromProviderDTOList(
                Lists.newArrayList(commoditiesBoughtRepoFromProviderDTO));
        // set connected entity list
        ConnectedEntityRepoDTO connectedEntityRepoDTO = new ConnectedEntityRepoDTO();
        connectedEntityRepoDTO.setConnectionType(ConnectionType.NORMAL_CONNECTION.getNumber());
        connectedEntityRepoDTO.setConnectedEntityType(EntityType.PHYSICAL_MACHINE.getNumber());
        connectedEntityRepoDTO.setConnectedEntityId(pmTopoDTO.getOid());
        vmServiceEntity.setConnectedEntityList(Lists.newArrayList(connectedEntityRepoDTO));
        vmServiceEntity.setTargetVendorIds(ImmutableMap.of("12512", "qqq"));
    }

    @Test
    public void testConvertDTOs() {
        assertEquals(0,
                TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.emptyList())
                        .size());
        assertEquals(7, TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(
                Arrays.asList(vmTopoDTO, pmTopoDTO, dsTopoDTO, vdcTopoDTO, networkTopoDTO,
                        desktopPoolTopoDTO, businessUserTopoDTO)).size());
    }

    @Test
    public void testConvertDTOSE() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vdcTopoDTO))
                .iterator().next();
        verifySE(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vmTopoDTO))
                .iterator().next();
        verifySE(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(pmTopoDTO))
                .iterator().next();
        verifySE(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(dsTopoDTO))
                .iterator().next();
        verifySE(dsTopoDTO, dsRepoDTO);

        ServiceEntityRepoDTO networkRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(networkTopoDTO))
                .iterator().next();
        verifySE(networkTopoDTO, networkRepoDTO);

        ServiceEntityRepoDTO desktopPoolRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(desktopPoolTopoDTO))
                .iterator().next();
        verifySE(desktopPoolTopoDTO, desktopPoolRepoDTO);

        ServiceEntityRepoDTO businessUserRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(businessUserTopoDTO))
                .iterator().next();
        verifySE(businessUserTopoDTO, businessUserRepoDTO);
    }

    @Test
    public void testConvertDTOCommoditiesBought() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vdcTopoDTO))
                .iterator().next();
        verifyCommodityBought(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vmTopoDTO))
                .iterator().next();
        verifyCommodityBought(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(pmTopoDTO))
                .iterator().next();
        verifyCommodityBought(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(dsTopoDTO))
                .iterator().next();
        verifyCommodityBought(dsTopoDTO, dsRepoDTO);

        ServiceEntityRepoDTO networkRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(networkTopoDTO))
                .iterator().next();
        verifyCommodityBought(networkTopoDTO, networkRepoDTO);

        ServiceEntityRepoDTO desktopPoolRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(desktopPoolTopoDTO))
                .iterator().next();
        verifyCommodityBought(desktopPoolTopoDTO, desktopPoolRepoDTO);

        ServiceEntityRepoDTO businessUserRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(businessUserTopoDTO))
                .iterator().next();
        verifyCommodityBought(businessUserTopoDTO, businessUserRepoDTO);
    }

    @Test
    public void testConvertDTOCommoditiesSold() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vdcTopoDTO))
                .iterator().next();
        verifyCommoditySold(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(vmTopoDTO))
                .iterator().next();
        verifyCommoditySold(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(pmTopoDTO))
                .iterator().next();
        verifyCommoditySold(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(dsTopoDTO))
                .iterator().next();
        verifyCommoditySold(dsTopoDTO, dsRepoDTO);

        ServiceEntityRepoDTO desktopPoolRepoDTO = TopologyEntityDTOConverter.convertToServiceEntityRepoDTOs(Collections.singletonList(desktopPoolTopoDTO))
                .iterator().next();
        verifyCommoditySold(desktopPoolTopoDTO, desktopPoolRepoDTO);
    }

    @Test
    public void testConvertRepoToDTO() {
        TopologyEntityDTO topologyEntityDTO =
                ServiceEntityRepoDTOConverter.convertToTopologyEntityDTOs(Collections.singletonList(vmServiceEntity))
                        .iterator().next();
        verifySE(topologyEntityDTO, vmServiceEntity);
        verifyCommodityBought(topologyEntityDTO, vmServiceEntity);
        verifyCommoditySold(topologyEntityDTO, vmServiceEntity);
    }

    private static void verifySE(@Nonnull final TopologyEntityDTO seTopoDTO,
                                 @Nonnull final ServiceEntityRepoDTO seRepoDTO) {
        final String expectedState = UIEntityState.fromEntityState(seTopoDTO.getEntityState())
                .apiStr();
        final String expectedType = ApiEntityType.fromEntity(seTopoDTO).apiStr();

        assertEquals(Long.toString(seTopoDTO.getOid()), seRepoDTO.getOid());
        assertEquals(seTopoDTO.getDisplayName(), seRepoDTO.getDisplayName());
        assertEquals(expectedType, seRepoDTO.getEntityType());
        assertEquals(String.valueOf(seTopoDTO.getOid()), seRepoDTO.getUuid());
        assertEquals(expectedState, seRepoDTO.getState());

        // compare tags
        assertEquals(seRepoDTO.getTags().size(), seTopoDTO.getTags().getTagsMap().size());
        seRepoDTO.getTags().forEach((key, value) ->
                assertEquals(value, seTopoDTO.getTags().getTagsMap().get(key).getValuesList()));

        // compare virtual machine info
        if (seRepoDTO.getVirtualMachineInfoRepoDTO() != null) {
            assertTrue(seTopoDTO.hasTypeSpecificInfo());
            assertTrue(seTopoDTO.getTypeSpecificInfo().hasVirtualMachine());
            VirtualMachineInfo vmInfo = seTopoDTO.getTypeSpecificInfo().getVirtualMachine();
            assertEquals(seRepoDTO.getVirtualMachineInfoRepoDTO().getTenancy(),
                    vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null);
            assertEquals(seRepoDTO.getVirtualMachineInfoRepoDTO().getGuestOsInfo(),
                    vmInfo.hasGuestOsInfo()
                        ? new GuestOSRepoDTO(vmInfo.getGuestOsInfo().getGuestOsType(),
                            vmInfo.getGuestOsInfo().getGuestOsName()) : null);
            if (seRepoDTO.getVirtualMachineInfoRepoDTO().getIpAddressInfoList() != null) {
                // remove null Ip Address entries Repo DTO since those would have been skipped
                // when converting to TopologyEntityDTO
                List<IpAddressRepoDTO> ipAddressRepoList =
                        seRepoDTO.getVirtualMachineInfoRepoDTO().getIpAddressInfoList().stream()
                        .filter(ipAddressRepoDTO -> ipAddressRepoDTO.getIpAddress() != null)
                        .collect(Collectors.toList());
                assertEquals(ipAddressRepoList.size(), vmInfo.getIpAddressesCount());
                ipAddressRepoList.forEach(ipAddressRepoDTO ->
                        assertTrue(vmInfo.getIpAddressesList().stream()
                                .anyMatch(ipAddressInfo ->
                                        ipAddressRepoDTO.getIpAddress()
                                                .equals(ipAddressInfo.getIpAddress())
                                                &&
                                                ipAddressInfo.getIsElastic() ==
                                                        ipAddressRepoDTO.getElastic())));
            }
        }

        // compare desktop pool info
        final DesktopPoolInfoRepoDTO desktopPoolInfoRepoDTO = seRepoDTO.getDesktopPoolInfoRepoDTO();
        if (desktopPoolInfoRepoDTO != null) {
            Assert.assertTrue(seTopoDTO.hasTypeSpecificInfo());
            Assert.assertTrue(seTopoDTO.getTypeSpecificInfo().hasDesktopPool());
            final DesktopPoolInfo desktopPool = seTopoDTO.getTypeSpecificInfo().getDesktopPool();
            Assert.assertEquals(desktopPoolInfoRepoDTO.getAssignmentType(), desktopPool.getAssignmentType());
            Assert.assertEquals(desktopPoolInfoRepoDTO.getCloneType(), desktopPool.getCloneType());
            Assert.assertEquals(desktopPoolInfoRepoDTO.getProvisionType(), desktopPool.getProvisionType());
            Assert.assertEquals(desktopPoolInfoRepoDTO.getTemplateReferenceId(), (Long)desktopPool.getTemplateReferenceId());
        }

        // compare business user info
        final BusinessUserInfoRepoDTO businessUserInfoRepoDTO = seRepoDTO.getBusinessUserInfoRepoDTO();
        if (businessUserInfoRepoDTO != null) {
            Assert.assertTrue(seTopoDTO.hasTypeSpecificInfo());
            Assert.assertTrue(seTopoDTO.getTypeSpecificInfo().hasBusinessUser());
            final BusinessUserInfo businessUser = seTopoDTO.getTypeSpecificInfo().getBusinessUser();
            Assert.assertEquals(businessUserInfoRepoDTO.getVmOidToSessionDuration(), businessUser.getVmOidToSessionDurationMap());
        }

        // check connected entity list
        assertEquals(seRepoDTO.getConnectedEntityList().size(), seTopoDTO.getConnectedEntityListCount());
        seRepoDTO.getConnectedEntityList().forEach(connectedEntityRepoDTO ->
                assertTrue(seTopoDTO.getConnectedEntityListList().stream()
                        .anyMatch(connectedEntity ->
                                connectedEntity.getConnectionType().getNumber() ==
                                        connectedEntityRepoDTO.getConnectionType() &&
                                connectedEntity.getConnectedEntityType() ==
                                        connectedEntityRepoDTO.getConnectedEntityType() &&
                                connectedEntity.getConnectedEntityId() ==
                                        connectedEntityRepoDTO.getConnectedEntityId())));

        // check target ids
        if (seRepoDTO.getTargetVendorIds() != null) {
            assertTrue(seRepoDTO.getTargetVendorIds().keySet().stream()
                .map(Long::valueOf)
                .collect(Collectors.toList()).containsAll(
                    seTopoDTO.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap().keySet()));
        }
    }

    private static void verifyCommodityBought(
            final TopologyEntityDTO seTopoDTO, final ServiceEntityRepoDTO seRepoDTO) {
        List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList =
                seRepoDTO.getCommoditiesBoughtRepoFromProviderDTOList();
        List<CommoditiesBoughtFromProvider> topoCommoditiesBoughtList =
                seTopoDTO.getCommoditiesBoughtFromProvidersList();

        assertEquals(topoCommoditiesBoughtList.size(), commoditiesBoughtRepoFromProviderDTOList.size());

        for (CommoditiesBoughtRepoFromProviderDTO boughtRepoGrouping : commoditiesBoughtRepoFromProviderDTOList) {
            final Long provider = boughtRepoGrouping.getProviderId();
            final List<CommodityBoughtRepoDTO> repoCommoditiesBought =
                    boughtRepoGrouping.getCommodityBoughtRepoDTOs();
            Optional<CommoditiesBoughtFromProvider> commodityBoughtList =
                    topoCommoditiesBoughtList.stream().
                    filter(commoditiesBoughtFromProvider ->
                            commoditiesBoughtFromProvider.getProviderId() == provider)
                    .findFirst();
            CommoditiesBoughtFromProvider grouping = commodityBoughtList.orElse(null);
            assertNotNull("commodity bought grouping is null", grouping);
            List<CommodityBoughtDTO> topoCommoditiesBought = grouping.getCommodityBoughtList();

            assertEquals(topoCommoditiesBought.size(), repoCommoditiesBought.size());

            for (int i = 0; i < topoCommoditiesBought.size(); i++) {
                verifyCommodityBought(seRepoDTO.getOid(),
                                      String.valueOf(provider),
                                      topoCommoditiesBought.get(i),
                                      repoCommoditiesBought.get(i));
            }
        }
    }

    private static void verifyCommodityBought(final String ownerOid,
                                              final String providerOid,
                                              final CommodityBoughtDTO commTopoDTO,
                                              final CommodityBoughtRepoDTO commRepoDTO) {
        final String expectedType = UICommodityType.fromType(commTopoDTO.getCommodityType().getType()).apiStr();
        assertEquals(commTopoDTO.getCommodityType().getKey(), commRepoDTO.getKey());
        assertEquals(commTopoDTO.getPeak(), commRepoDTO.getPeak(), epsilon);
        assertEquals(providerOid, commRepoDTO.getProviderOid());
        assertEquals(ownerOid, commRepoDTO.getOwnerOid());
        assertEquals(expectedType, commRepoDTO.getType());
        assertEquals(commTopoDTO.getUsed(), commRepoDTO.getUsed(), epsilon);
    }

    private static void verifyCommoditySold(final TopologyEntityDTO seTopoDTO,
                                            final ServiceEntityRepoDTO seRepoDTO) {
        List<CommoditySoldRepoDTO> repoCommoditiesSold = seRepoDTO.getCommoditySoldList();
        List<CommoditySoldDTO> topoCommoditiesSold = seTopoDTO.getCommoditySoldListList();

        assertEquals(topoCommoditiesSold.size(), repoCommoditiesSold.size());

        for (int i = 0; i < topoCommoditiesSold.size(); i++) {
            verifyCommoditySold(seRepoDTO.getOid(),
                                seRepoDTO.getOid(),
                                topoCommoditiesSold.get(i),
                                repoCommoditiesSold.get(i));
        }
    }

    private static void verifyCommoditySold(final String ownerOid,
                                            final String providerOid,
                                            final CommoditySoldDTO commTopoDTO,
                                            final CommoditySoldRepoDTO commRepoDTO) {
        final String expectedType = UICommodityType.fromType(commTopoDTO.getCommodityType().getType()).apiStr();
        assertEquals(commTopoDTO.getCapacity(), commRepoDTO.getCapacity(), epsilon);
        assertEquals(commTopoDTO.getEffectiveCapacityPercentage(), commRepoDTO.getEffectiveCapacityPercentage(), epsilon);
        assertEquals(commTopoDTO.getCommodityType().getKey(), commRepoDTO.getKey());
        assertEquals(commTopoDTO.getPeak(), commRepoDTO.getPeak(), epsilon);
        assertEquals(providerOid, commRepoDTO.getProviderOid());
        assertEquals(ownerOid, commRepoDTO.getOwnerOid());
        assertEquals(commTopoDTO.getReservedCapacity(), commRepoDTO.getReservedCapacity(), epsilon);
        assertEquals(expectedType, commRepoDTO.getType());
        assertEquals(commTopoDTO.getUsed(), commRepoDTO.getUsed(), epsilon);
        assertEquals(commTopoDTO.getHotResizeInfo().getHotAddSupported(), commRepoDTO.isHotAddSupported());
        assertEquals(commTopoDTO.getHotResizeInfo().getHotRemoveSupported(), commRepoDTO.isHotRemoveSupported());
        assertEquals(commTopoDTO.getHotResizeInfo().getHotReplaceSupported(), commRepoDTO.isHotReplaceSupported());
    }
}
