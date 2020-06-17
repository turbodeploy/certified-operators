package com.vmturbo.components.test.utilities.topology.conversion;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class TopologyToDiscoveryResponseTest {

    DiscoveryResponse discoveryResponse;

    @Before
    public void setup() throws Exception {
        final String filename = "topologies/classic/test.customer.markets.topology";
        URL url = TopologyToDiscoveryResponseTest.class.getClassLoader().getResource(filename);
        if (url == null) {
            fail("Unable to load resource file " + filename);
        }

        final TopologyToDiscoveryResponse converter = new TopologyToDiscoveryResponse(url.getFile());
        discoveryResponse = converter.convert();
    }

    @Test
    public void testEntitiesParsed() throws Exception {
        assertEquals(106, discoveryResponse.getEntityDTOList().size());
    }

    @Test
    public void testDatacenterCommoditiesParsed() throws Exception {
        final EntityDTO datacenter = discoveryResponse.getEntityDTOList().stream()
            .filter(entity -> entity.getId().equals("e02da2203c8eac2d66f98a3ec1123bf5340b5465"))
            .findFirst()
            .get();
        assertEquals(EntityType.DATACENTER, datacenter.getEntityType());

        // Verify datacenter sells space, power, cooling and buys nothing.
        assertEquals(3, datacenter.getCommoditiesSoldCount());
        final List<CommodityType> sold = datacenter.getCommoditiesSoldList().stream()
            .map(CommodityDTO::getCommodityType)
            .collect(Collectors.toList());
        assertThat(sold, containsInAnyOrder(CommodityType.COOLING, CommodityType.POWER, CommodityType.SPACE));
        assertEquals(0, datacenter.getCommoditiesBoughtCount());
    }

    @Test
    public void testHostCommoditiesParsed() throws Exception {
        final EntityDTO host = discoveryResponse.getEntityDTOList().stream()
            .filter(entity -> entity.getId().equals("2cb26fb93af31cfc0587cecb408f209e43062047"))
            .findFirst()
            .get();
        assertEquals(EntityType.PHYSICAL_MACHINE, host.getEntityType());

        // Verify host sells a number of commodities.
        final List<CommodityType> sold = host.getCommoditiesSoldList().stream()
            .map(CommodityDTO::getCommodityType)
            .collect(Collectors.toList());
        assertTrue(sold.contains(CommodityType.MEM_ALLOCATION));
        assertTrue(sold.contains(CommodityType.CPU_ALLOCATION));
        assertTrue(sold.contains(CommodityType.CPU));
        assertTrue(sold.contains(CommodityType.MEM));
        assertTrue(sold.contains(CommodityType.NET_THROUGHPUT));
        assertTrue(sold.contains(CommodityType.IO_THROUGHPUT));
        assertTrue(sold.contains(CommodityType.CPU_PROVISIONED));
        assertTrue(sold.contains(CommodityType.MEM_PROVISIONED));
        assertTrue(sold.contains(CommodityType.DATASTORE));
        assertTrue(sold.contains(CommodityType.NETWORK));

        // Verify host is buying a number of commodities from correct suppliers.
        final Map<String, List<CommodityType>> bought = host.getCommoditiesBoughtList().stream()
            .collect(Collectors.toMap(EntityDTO.CommodityBought::getProviderId,
                provider -> provider.getBoughtList().stream()
                    .map(CommodityDTO::getCommodityType)
                    .collect(Collectors.toList())));

        assertTrue(bought.containsKey("e02da2203c8eac2d66f98a3ec1123bf5340b5465")); // The datacenter
        assertThat(bought.get("e02da2203c8eac2d66f98a3ec1123bf5340b5465"),
            containsInAnyOrder(CommodityType.COOLING, CommodityType.POWER, CommodityType.SPACE));
    }
}