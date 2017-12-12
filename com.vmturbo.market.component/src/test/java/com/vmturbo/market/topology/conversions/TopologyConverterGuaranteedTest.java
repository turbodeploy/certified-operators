package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for proper handling of {@link TopologyConverter) inclodeVDC argument
 * and proper handling of guaranteed buyers.
 */
public class TopologyConverterGuaranteedTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final long VDC1_OID = 50001;
    private static final long VDC2_OID = 50002;
    private static final long DPOD_OID = 50003;
    private static final long HOST_OID = 50004;
    private static final long VM1_OID = 70001;
    private static final long VM2_OID = 70002;
    private static CommodityType MEM_ALLOC = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE)
            .build();
    private static List<TopologyEntityDTO> entities;

    /**
     * Create a topology with two VDCs, one that qualifies as guaranteed buyer and one that doesn't,
     * a DPod and a PM.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // Not a guaranteed buyer
        TopologyEntityDTO vdc1 = TopologyEntityDTO.newBuilder()
                        .setOid(VDC1_OID)
                        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                        .build();
        // Guaranteed buyer because it consumes from a host
        TopologyEntityDTO vdc2 = TopologyEntityDTO.newBuilder()
                        .setOid(VDC2_OID)
                        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setProviderId(HOST_OID)
                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(MEM_ALLOC)))
                        .build();
        // Guaranteed buyer because it is a DPod
        TopologyEntityDTO dpod = TopologyEntityDTO.newBuilder()
                        .setOid(DPOD_OID)
                        .setEntityType(EntityType.DPOD_VALUE)
                        .build();
        // Not a guaranteed buyer (sells to one)
        TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                        .setOid(HOST_OID)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(MEM_ALLOC)
                            .build())
                        .build();
        TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
                        .setOid(VM1_OID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEntityState(EntityState.UNKNOWN)
                        .build();
        TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
                        .setOid(VM2_OID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEntityState(EntityState.MAINTENANCE)
                        .build();
        entities = ImmutableList.of(vdc1, vdc2, dpod, pm, vm1, vm2);
    }

    /**
     * Test the converter when includeGuaranteedBuyers is false.
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testExcludeVDCs() throws InvalidTopologyException {
        // includeVDC is false
        TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO);
        Set<TraderTO> traders = converter.convertToMarket(entities);
        // VDCs are skipped, VMs in maintenance and unknown state are skipped
        assertEquals(1, traders.size());
        List<Long> traderOids = traders.stream().map(TraderTO::getOid).collect(Collectors.toList());
        assertFalse(traderOids.contains(DPOD_OID));
        assertTrue(traderOids.contains(HOST_OID));
        List<Long> guaranteedBuyers = traders.stream()
                        .filter(trader ->  trader.getSettings().getGuaranteedBuyer())
                        .map(TraderTO::getOid).collect(Collectors.toList());
        assertEquals(0, guaranteedBuyers.size());
        assertFalse(guaranteedBuyers.contains(DPOD_OID));
    }

    /**
     * Test the converter when includeGuaranteedBuyers is true.
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testIncludeVDCs() throws InvalidTopologyException {
        TopologyConverter converter = new TopologyConverter(true, REALTIME_TOPOLOGY_INFO);
        Set<TraderTO> traders = converter.convertToMarket(entities);
        assertEquals(4, traders.size());
        List<Long> guaranteedBuyers = traders.stream()
            .filter(trader ->  trader.getSettings().getGuaranteedBuyer())
            .map(TraderTO::getOid).collect(Collectors.toList());
        assertEquals(2, guaranteedBuyers.size());
        assertTrue(guaranteedBuyers.contains(DPOD_OID));
        assertTrue(guaranteedBuyers.contains(VDC2_OID));
    }
}
