package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CostDTOCreator.OSTypeMapping;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;


/**
 * Test class to unit test the CostDTOCreator methods.
 */
public class CostDTOCreatorTest {

    private static final long TIER_ID = 111;
    private static final long REGION_ID = 222;
    private static final long BA_ID = 333;
    private static final int IOSPEC_BASE_TYPE = 1;
    private static final int IOSPEC_TYPE = 11;
    private static final int NETSPEC_BASE_TYPE = 2;
    private static final int NETSPEC_TYPE = 22;

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE)
            .setOid(REGION_ID)
            .build();

    private static final TopologyEntityDTO BA = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(BA_ID)
            .build();

    private MarketPriceTable marketPriceTable;
    private CommodityConverter converter;

    /**
     * Initialization for the mocked fields.
     */
    @Before
    public void setup() {
        marketPriceTable = mock(MarketPriceTable.class);
        converter = mock(CommodityConverter.class);
    }

    /**
     * This test ensures that all the values in Enum OSType have a mapping in
     * CostDTOCreator::OSTypeMapping, except "Windows Server" and "Windows server Burst".
     */
    @Test
    public void testOSTypeMappings() {
        List<OSType> osWithoutMapping = new ArrayList<>();
        Map<OSType, String> inversedOSTypeMapping = OSTypeMapping.entrySet().stream().collect(
                Collectors.toMap(Entry::getValue, Entry::getKey));
        for (OSType os : OSType.values()) {
            if (os != OSType.WINDOWS_SERVER && os != OSType.WINDOWS_SERVER_BURST &&
                    !inversedOSTypeMapping.containsKey(os)) {
                osWithoutMapping.add(os);
            }
        }
        String error = "Operating systems " + osWithoutMapping.stream().map(os -> os.toString())
                .collect(Collectors.joining(", ")) + " do not have mapping in " +
                "CostDTOCreator::OSTypeMapping.";
        assertTrue(error, osWithoutMapping.isEmpty());
    }


    /**
     * Creates and returns a test TopologyEntityDTO.
     * @return TopologyEntityDTO
     */
    private TopologyEntityDTO getTestComputeTier() {

        CommodityType ioTpCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();
        CommodityType netTpCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE).build();
        CommoditySpecificationTO ioCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(IOSPEC_BASE_TYPE)
                .setType(IOSPEC_TYPE)
                .build();
        CommoditySpecificationTO netCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(NETSPEC_BASE_TYPE)
                .setType(NETSPEC_TYPE)
                .build();
        Mockito.doReturn(ioCommSpecTO).when(converter)
                .commoditySpecification(ioTpCommType);
        Mockito.doReturn(netCommSpecTO).when(converter)
                .commoditySpecification(netTpCommType);

        ComputePriceBundle computeBundle = ComputePriceBundle.newBuilder()
                .addPrice(BA_ID, OSType.LINUX, 0.5, true)
                .build();
        when(marketPriceTable.getComputePriceBundle(TIER_ID, REGION_ID))
                .thenReturn(computeBundle);

        final TopologyDTO.CommoditySoldDTO topologyIoTpSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE)
                                .build())
                        .build();
        final TopologyDTO.CommoditySoldDTO topologyNetTpSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE)
                                .build())
                        .build();
        TopologyEntityDTO tier = TopologyEntityDTO.newBuilder()
                .setOid(111)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(topologyIoTpSold)
                .addCommoditySoldList(topologyNetTpSold)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setDedicatedStorageNetworkState(
                                        DedicatedStorageNetworkState.NOT_SUPPORTED)
                                .build())
                        .build())
                .build();
        return tier;
    }

    /**
     * Tests the ComputeResourceDependency aspect of the CreateComputeTierCostDTO.
     */
    @Test
    public void testComputeResourceDependency() {
        final TopologyEntityDTO tier = getTestComputeTier();
        Set<TopologyEntityDTO> bas = new HashSet<>();
        bas.add(BA);
        CostDTOCreator costDTOCreator = new CostDTOCreator(converter, marketPriceTable);
        CostDTO costDTO = costDTOCreator.createComputeTierCostDTO(tier, REGION, bas);
        Assert.assertEquals(1, costDTO.getComputeTierCost().getComputeResourceDepedencyCount());
        ComputeResourceDependency dependency = costDTO.getComputeTierCost().getComputeResourceDepedency(0);
        Assert.assertNotNull(dependency.getBaseResourceType());
        Assert.assertEquals(NETSPEC_BASE_TYPE, dependency.getBaseResourceType().getBaseType());
        Assert.assertEquals(NETSPEC_TYPE, dependency.getBaseResourceType().getType());
        Assert.assertNotNull(dependency.getDependentResourceType());
        Assert.assertEquals(IOSPEC_BASE_TYPE, dependency.getDependentResourceType().getBaseType());
        Assert.assertEquals(IOSPEC_TYPE, dependency.getDependentResourceType().getType());
    }

}
