package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;
import com.vmturbo.repository.constant.RepoObjectState;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterTest {

    private static double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private TopologyEntityDTO vmTopoDTO;
    private TopologyEntityDTO pmTopoDTO;
    private TopologyEntityDTO dsTopoDTO;
    private TopologyEntityDTO vdcTopoDTO;
    private TopologyEntityDTO networkTopoDTO;
    private ServiceEntityRepoDTO vmServiceEntity = new ServiceEntityRepoDTO();

    @Before
    public void setup() throws IOException {
        vmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        vdcTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        networkTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/network-1.dto.json");
        buildVMServiceEntityRepoDTO(vmServiceEntity);
    }

    private void buildVMServiceEntityRepoDTO(@Nonnull final ServiceEntityRepoDTO vmServiceEntity) {
        vmServiceEntity.setDisplayName("test-vm");
        vmServiceEntity.setOid("111");
        vmServiceEntity.setUuid("111");
        vmServiceEntity.setEntityType(RepoObjectType.mapEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
        vmServiceEntity.setState(RepoObjectState.toRepoEntityState(TopologyDTO.EntityState.POWERED_ON));
        final Map<String, List<String>> tagsMap = new HashMap<>();
        tagsMap.put("key1", Arrays.asList("value1", "value2"));
        tagsMap.put("key2", Arrays.asList("value1"));
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
        virtualMachineInfoRepoDTO.setGuestOsType("WINDOWS");
        vmServiceEntity.setVirtualMachineInfo(virtualMachineInfoRepoDTO);
        final CommoditySoldRepoDTO commoditySoldRepoDTO = new CommoditySoldRepoDTO();
        commoditySoldRepoDTO.setCapacity(123);
        commoditySoldRepoDTO.setKey("test-sold-key");
        commoditySoldRepoDTO.setType(RepoObjectType.mapCommodityType(CommodityType.VMEM.getValue()));
        commoditySoldRepoDTO.setUsed(100);
        commoditySoldRepoDTO.setProviderOid("111");
        commoditySoldRepoDTO.setOwnerOid("111");
        final CommoditySoldRepoDTO commoditySoldRepoDTOTwo = new CommoditySoldRepoDTO();
        commoditySoldRepoDTOTwo.setCapacity(345);
        commoditySoldRepoDTOTwo.setKey("test-sold-key-two");
        commoditySoldRepoDTOTwo.setType(RepoObjectType.mapCommodityType(CommodityType.APPLICATION.getValue()));
        commoditySoldRepoDTOTwo.setUsed(100);
        commoditySoldRepoDTOTwo.setProviderOid("111");
        commoditySoldRepoDTOTwo.setOwnerOid("111");
        vmServiceEntity.setCommoditySoldList(Lists.newArrayList(commoditySoldRepoDTO, commoditySoldRepoDTOTwo));
        final CommodityBoughtRepoDTO commodityBoughtRepoDTO = new CommodityBoughtRepoDTO();
        commodityBoughtRepoDTO.setKey("test-key");
        commodityBoughtRepoDTO.setType(RepoObjectType.mapCommodityType(CommodityType.MEM.getValue()));
        commodityBoughtRepoDTO.setUsed(123);
        commodityBoughtRepoDTO.setProviderOid("222");
        commodityBoughtRepoDTO.setOwnerOid("111");
        final CommodityBoughtRepoDTO commodityBoughtRepoDTOTwo = new CommodityBoughtRepoDTO();
        commodityBoughtRepoDTOTwo.setKey("test-key-two");
        commodityBoughtRepoDTOTwo.setType(RepoObjectType.mapCommodityType(CommodityType.CLUSTER.getValue()));
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
        vmServiceEntity.setTargetIds(Lists.newArrayList(1111L));
    }

    @Test
    public void testConvertDTOs() {
        assertEquals(0, TopologyConverter.convert(Arrays.asList()).size());

        assertEquals(1, TopologyConverter.convert(Arrays.asList(vmTopoDTO)).size());
        assertEquals(5, TopologyConverter.convert(
                Arrays.asList(vmTopoDTO, pmTopoDTO, dsTopoDTO, vdcTopoDTO, networkTopoDTO)).size());
    }

    @Test
    public void testConvertDTOSE() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyConverter.convert(Arrays.asList(vdcTopoDTO))
                .iterator().next();
        verifySE(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyConverter.convert(Arrays.asList(vmTopoDTO))
                .iterator().next();
        verifySE(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyConverter.convert(Arrays.asList(pmTopoDTO))
                .iterator().next();
        verifySE(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyConverter.convert(Arrays.asList(dsTopoDTO))
                .iterator().next();
        verifySE(dsTopoDTO, dsRepoDTO);

        ServiceEntityRepoDTO networkRepoDTO = TopologyConverter.convert(Arrays.asList(networkTopoDTO))
                .iterator().next();
        verifySE(networkTopoDTO, networkRepoDTO);
    }

    @Test
    public void testConvertDTOCommoditiesBought() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyConverter.convert(Arrays.asList(vdcTopoDTO))
                .iterator().next();
        verifyCommodityBought(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyConverter.convert(Arrays.asList(vmTopoDTO))
                .iterator().next();
        verifyCommodityBought(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyConverter.convert(Arrays.asList(pmTopoDTO))
                .iterator().next();
        verifyCommodityBought(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyConverter.convert(Arrays.asList(dsTopoDTO))
                .iterator().next();
        verifyCommodityBought(dsTopoDTO, dsRepoDTO);

        ServiceEntityRepoDTO networkRepoDTO = TopologyConverter.convert(Arrays.asList(networkTopoDTO))
                .iterator().next();
        verifyCommodityBought(networkTopoDTO, networkRepoDTO);
    }

    @Test
    public void testConvertDTOCommoditiesSold() {
        ServiceEntityRepoDTO vdcRepoDTO = TopologyConverter.convert(Arrays.asList(vdcTopoDTO))
                .iterator().next();
        verifyCommoditySold(vdcTopoDTO, vdcRepoDTO);

        ServiceEntityRepoDTO vmRepoDTO = TopologyConverter.convert(Arrays.asList(vmTopoDTO))
                .iterator().next();
        verifyCommoditySold(vmTopoDTO, vmRepoDTO);

        ServiceEntityRepoDTO pmRepoDTO = TopologyConverter.convert(Arrays.asList(pmTopoDTO))
                .iterator().next();
        verifyCommoditySold(pmTopoDTO, pmRepoDTO);

        ServiceEntityRepoDTO dsRepoDTO = TopologyConverter.convert(Arrays.asList(dsTopoDTO))
                .iterator().next();
        verifyCommoditySold(dsTopoDTO, dsRepoDTO);
    }

    @Test
    public void testConvertRepoToDTO() {
        TopologyEntityDTO topologyEntityDTO =
                TopologyConverter.convertToTopologyEntity(Arrays.asList(vmServiceEntity))
                        .iterator().next();
        verifySE(topologyEntityDTO, vmServiceEntity);
        verifyCommodityBought(topologyEntityDTO, vmServiceEntity);
        verifyCommoditySold(topologyEntityDTO, vmServiceEntity);
    }

    private static void verifySE(
            final TopologyEntityDTO seTopoDTO, final ServiceEntityRepoDTO seRepoDTO) {
        final String expectedState = TopologyConverter.ServiceEntityMapper.mapEntityState(
                seTopoDTO.getEntityState());
        final String expectedType = TopologyConverter.ServiceEntityMapper.mapEntityType(
                seTopoDTO.getEntityType());

        assertEquals(Long.toString(seTopoDTO.getOid()), seRepoDTO.getOid());
        assertEquals(seTopoDTO.getDisplayName(), seRepoDTO.getDisplayName());
        assertEquals(expectedType, seRepoDTO.getEntityType());
        assertEquals(String.valueOf(seTopoDTO.getOid()), seRepoDTO.getUuid());
        assertEquals(expectedState, seRepoDTO.getState());

        // compare tags
        assertEquals(seRepoDTO.getTags().size(), seTopoDTO.getTagsMap().size());
        seRepoDTO.getTags().entrySet().forEach(t ->
                assertEquals(t.getValue(), seTopoDTO.getTagsMap().get(t.getKey()).getValuesList()));

        // compare virtual machine info
        if (seRepoDTO.getVirtualMachineInfo() != null) {
            assertTrue(seTopoDTO.hasTypeSpecificInfo());
            assertTrue(seTopoDTO.getTypeSpecificInfo().hasVirtualMachine());
            VirtualMachineInfo vmInfo = seTopoDTO.getTypeSpecificInfo().getVirtualMachine();
            assertEquals(seRepoDTO.getVirtualMachineInfo().getTenancy(),
                    vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null);
            assertEquals(seRepoDTO.getVirtualMachineInfo().getGuestOsType(),
                    vmInfo.hasGuestOsType() ? vmInfo.getGuestOsType().toString() : null);
            if (seRepoDTO.getVirtualMachineInfo().getIpAddressInfoList() != null) {
                // remove null Ip Address entries Repo DTO since those would have been skipped
                // when converting to TopologyEntityDTO
                List<IpAddressRepoDTO> ipAddressRepoList =
                        seRepoDTO.getVirtualMachineInfo().getIpAddressInfoList().stream()
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
        if (seRepoDTO.getTargetIds() != null) {
            assertTrue(seRepoDTO.getTargetIds().containsAll(
                    seTopoDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList()));
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
        final String expectedType = RepoObjectType.mapCommodityType(
                commTopoDTO.getCommodityType().getType());
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
        final String expectedType = RepoObjectType.mapCommodityType(
                commTopoDTO.getCommodityType().getType());
        assertEquals(commTopoDTO.getCapacity(), commRepoDTO.getCapacity(), epsilon);
        assertEquals(commTopoDTO.getEffectiveCapacityPercentage(), commRepoDTO.getEffectiveCapacityPercentage(), epsilon);
        assertEquals(commTopoDTO.getCommodityType().getKey(), commRepoDTO.getKey());
        assertEquals(commTopoDTO.getPeak(), commRepoDTO.getPeak(), epsilon);
        assertEquals(providerOid, commRepoDTO.getProviderOid());
        assertEquals(ownerOid, commRepoDTO.getOwnerOid());
        assertEquals(commTopoDTO.getReservedCapacity(), commRepoDTO.getReservedCapacity(), epsilon);
        assertEquals(expectedType, commRepoDTO.getType());
        assertEquals(commTopoDTO.getUsed(), commRepoDTO.getUsed(), epsilon);
    }
}
