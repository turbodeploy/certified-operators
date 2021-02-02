package com.vmturbo.market.topology.conversions;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityResults;
import com.vmturbo.market.runner.wastedfiles.WastedFilesResults;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class MarketExceptionsTest {

    private static final long PM_OID = 10000L;
    private static final long DS_OID = 20000L;
    private static final long DA_OID = 30000L;
    private static final long SC_OID = 40000L;
    private static final Float RAW_PM_USED = 0.5F;
    private static final Float RAW_PM_CAPACITY = 2.0F;


    private final CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
    private final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
    private final CloudCostData mockCCD = mock(CloudCostData.class);
    private TopologyConverter converter;

    private final ReservedCapacityResults reservedCapacityResults = ReservedCapacityResults.EMPTY;

    CommodityType ioType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();

    CommodityType vMemType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

    CommodityType vStorageType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE).build();

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =
            TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();

    private static final NumericIDAllocator ID_ALLOCATOR = new NumericIDAllocator();
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private ReversibilitySettingFetcher reversibilitySettingFetcher =
            mock(ReversibilitySettingFetcher.class);

    private static final int ST_AMT_TYPE_ID = ID_ALLOCATOR.allocate("StorageAmount");
    private CloudTopology<TopologyEntityDTO> cloudTopology =
            mock(TopologyEntityCloudTopology.class);

    /**
     * Teset setup.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
        when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(consistentScalingHelper);
        constructTopologyConverter();
    }

    /**
     * Construct default TopologyConverter.
     */
    private void constructTopologyConverter() {
        constructTopologyConverter(CommodityIndex.newFactory());
    }

    /**
     * Construct TopologyConverter wit provided {@link CommodityIndexFactory}.
     *
     * @param commodityIndexFactory Commodity index factory
     */
    private void constructTopologyConverter(final CommodityIndexFactory commodityIndexFactory) {
        constructTopologyConverter(commodityIndexFactory, REALTIME_TOPOLOGY_INFO);
    }

    /**
     * Creates converter with specified topologyInfo.
     *
     * @param commodityIndexFactory The factory.
     * @param topologyInfo Real-time or plan topologyInfo.
     */
    private void constructTopologyConverter(final CommodityIndexFactory commodityIndexFactory,
            final TopologyInfo topologyInfo) {
        converter = Mockito.spy(new TopologyConverter(
                topologyInfo,
                false,
                MarketAnalysisUtils.QUOTE_FACTOR,
                MarketMode.M2Only,
                MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketCloudRateExtractor,
                mockCommodityConverter,
                mockCCD,
                commodityIndexFactory,
                tierExcluderFactory,
                consistentScalingHelperFactory, cloudTopology, reversibilitySettingFetcher));
    }

    /**
     * Recover when topologyDTOtoTraderTO hits an exception for a trader.
     */
    @Test
    public void testTopologyDTOtoTraderTORecovery() {
        // Arrange
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .setUsed(RAW_PM_USED).setCapacity(RAW_PM_CAPACITY)
                .build();
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).setOid(PM_OID)
                .addCommoditySoldList(topologyCPUSold)
                .putEntityPropertyMap("dummy", "dummy").build();
        converter.topologyDTOtoTraderTO(expectedEntity);
    }

    /**
     * Recover when interpretAction hits an exception for an action.
     */
    @Test
    public void testInterpretActionRecovery() {
        double stAmtCapacity = 1024000d;
        double stAmtBought = 102400d;
        double originalStorageAmtUsed = 4096d;
        final TopologyDTO.CommoditySoldDTO topologyStAmtSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                        .build())
                .setCapacity(stAmtCapacity)
                .setUsed(originalStorageAmtUsed)
                .build();
        final List<CommodityBoughtDTO> topologyStAmtBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                .setUsed(stAmtBought).build());
        TopologyDTO.TopologyEntityDTO daTopo = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DISK_ARRAY_VALUE).setOid(DA_OID)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditySoldList(topologyStAmtSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(SC_OID)
                        .addAllCommodityBought(topologyStAmtBought)
                        .setProviderEntityType(EntityType.STORAGE_CONTROLLER_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();
        TopologyDTO.TopologyEntityDTO dsTopo = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE).setOid(DS_OID)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditySoldList(topologyStAmtSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(DA_OID)
                        .addAllCommodityBought(topologyStAmtBought)
                        .setProviderEntityType(EntityType.DISK_ARRAY_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();
        Map<Long, TopologyEntityDTO> origTopoMap =
                ImmutableMap.of(dsTopo.getOid(), dsTopo, daTopo.getOid(), daTopo);
        WastedFilesResults wastedFilesAnalysisMock = mock(WastedFilesResults.class);
        List<TraderTO> traderTOs =
                Lists.newArrayList();
        ActionTO provByDemandTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
            .setProvisionByDemand(ProvisionByDemandTO.newBuilder()
                    .setModelBuyer(-DS_OID).setModelSeller(DA_OID)
                    .setProvisionedSeller(DA_OID + 100L).build())
            .build();
        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopo = converter.convertFromMarket(
            traderTOs, origTopoMap, PriceIndexMessage.getDefaultInstance(), reservedCapacityResults, wastedFilesAnalysisMock);
        converter.interpretAction(provByDemandTO, projectedTopo, null, null, null);
    }

    /**
     * Recover when traderTOtoTopologyDTO hits an exception for a trader.
     */
    @Test
    public void testTraderTOtoTopologyDTOORecovery() {
        double originalStorageAmtUsed = 4096d;
        CommoditySpecificationTO stAmtCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(ST_AMT_TYPE_ID).setType(ST_AMT_TYPE_ID).build();
        final CommodityDTOs.CommoditySoldTO economyStAmtSold = CommodityDTOs.CommoditySoldTO
                .newBuilder()
                .setSpecification(stAmtCommSpecTO)
                .setQuantity((float)originalStorageAmtUsed)
                .build();
        final CommodityDTOs.CommodityBoughtTO economyStAmtBought =
                CommodityBoughtTO.newBuilder().setSpecification(stAmtCommSpecTO).build();
        EconomyDTOs.TraderTO traderDS = EconomyDTOs.TraderTO.newBuilder().setOid(DS_OID)
                .addCommoditiesSold(economyStAmtSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.STORAGE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(DS_OID + 1L)
                        .setSupplier(DA_OID)
                        .addCommoditiesBought(economyStAmtBought).build())
                .build();
        converter.traderTOtoTopologyDTO(traderDS, new HashMap<>(), null, null, null);
    }
}