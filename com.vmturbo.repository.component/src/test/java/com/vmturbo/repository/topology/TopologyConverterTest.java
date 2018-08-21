package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;
import com.vmturbo.repository.constant.RepoObjectState;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
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
        seRepoDTO.getTags().entrySet().forEach(
                t ->
                    assertEquals(
                            t.getValue(),
                            seTopoDTO.getTagsMap().get(t.getKey()).getValuesList()));
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
