package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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

    @Before
    public void setup() throws IOException {
        vmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        vdcTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        networkTopoDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/network-1.dto.json");
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
