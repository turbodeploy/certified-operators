package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.jsonprocessing.JsonToSMAInputTranslator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class SMAConverterTest {


    private TopologyConverter converter = mock(TopologyConverter.class);
    private SMAConverter smaConverter;
    private TopologyEntityDTO computeTier1;
    private TopologyEntityDTO computeTier2;
    private TopologyEntityDTO vm1DTO;
    private TopologyEntityDTO vm2DTO;
    private TopologyEntityDTO region;
    private Map<Long, TopologyEntityDTO> unmodifiableEntityOidToDtoMap = new HashMap<>();
    private Map<Long, TraderTO> oidToOriginalTraderTOMap = new HashMap<>();
    CloudTopologyConverter cloudTC = mock(CloudTopologyConverter.class);

    /**
     * initial setup.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        constructSMAConverter();
        JsonToSMAInputTranslator jsonToSMAInputTranslator =
                new JsonToSMAInputTranslator();
        Pair<SMAInput, SMAOutput> inputOutputPair =
                jsonToSMAInputTranslator.parseInputWithExpectedOutput(
                        "src/test/java/com/vmturbo/market/topology/conversions/2vm1ri.json");
        smaConverter.setSmaOutput(inputOutputPair.second);
        computeTier1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(100001L).build();
        computeTier2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(100002L).build();
        vm1DTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(2000001L).build();
        vm2DTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(2000002L).build();
        region = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(31)
                .build();
        MarketTier mst1 = new OnDemandMarketTier(computeTier1);
        MarketTier mst2 = new OnDemandMarketTier(computeTier2);
        TraderTO tp1 = TraderTO.newBuilder().setOid(14100001L).addCommoditiesSold(
                CommoditySoldTO.newBuilder().setSpecification(
                        CommoditySpecificationTO.newBuilder()
                                .setBaseType(1).setType(1))).build();
        TraderTO tp2 = TraderTO.newBuilder().setOid(14100002L).addCommoditiesSold(
                CommoditySoldTO.newBuilder().setSpecification(
                        CommoditySpecificationTO.newBuilder()
                                .setBaseType(1).setType(1))).build();
        oidToOriginalTraderTOMap.put(14100001L, tp1);
        oidToOriginalTraderTOMap.put(14100002L, tp2);
        ReservedInstanceData riData = mock(ReservedInstanceData.class);
        Optional<CommodityBoughtTO> coupon =
                Optional.of(CommodityBoughtTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(1).setType(1))
                        .build());
        unmodifiableEntityOidToDtoMap.put(100001L, computeTier1);
        unmodifiableEntityOidToDtoMap.put(100002L, computeTier2);
        unmodifiableEntityOidToDtoMap.put(2000001L, vm1DTO);
        unmodifiableEntityOidToDtoMap.put(2000002L, vm2DTO);
        when(converter.getUnmodifiableEntityOidToDtoMap())
                .thenReturn(unmodifiableEntityOidToDtoMap);
        when(converter.getUnmodifiableOidToOriginalTraderTOMap())
                .thenReturn(oidToOriginalTraderTOMap);
        when(converter.getCloudTc()).thenReturn(cloudTC);
        when(cloudTC.getTraderTOOid(mst1)).thenReturn(14100001L);
        when(cloudTC.getTraderTOOid(mst2)).thenReturn(14100002L);
        when(cloudTC.getIndexOfSlSuppliedByPrimaryTier(any())).thenReturn(0);
        when(cloudTC.getRegionOfCloudConsumer(any())).thenReturn(region);
        when(cloudTC.getRiDataById(1000001L)).thenReturn(riData);
        when(cloudTC.getRIDiscountedMarketTierIDFromRIData(riData))
                .thenReturn(15100002L);
        when(converter.createCouponCommodityBoughtForCloudEntity(
                15100002L, 2000002L))
                .thenReturn(coupon);


    }

    /**
     * Construct SMAConverter.
     */
    private void constructSMAConverter() {
        smaConverter = new SMAConverter(converter);
    }

    /**
     * test updateWithSMAOutput method with 2 VMS and 1 RI. One VM moving to
     * a non RI template and another VM moving to a RI template.
     */
    @Test
    public void testUpdateWithSMAOutput() {
        List<TraderTO> projectedTraderDTOs = new ArrayList<>();

        ShoppingListTO sl1 = ShoppingListTO.newBuilder().setOid(70001L).build();
        ShoppingListTO sl2 = ShoppingListTO.newBuilder().setOid(70002L).build();
        TraderTO vm1 = TraderTO.newBuilder().setOid(2000001L)
                .addShoppingLists(sl1).build();
        TraderTO vm2 = TraderTO.newBuilder().setOid(2000002L)
                .addShoppingLists(sl2).build();
        projectedTraderDTOs.add(vm1);
        projectedTraderDTOs.add(vm2);
        smaConverter.updateWithSMAOutput(projectedTraderDTOs);

        // vm1 (2000001L) is not matched to RI so has no couponid. It also
        // does not buy the coupon commodity.
        assertFalse(smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000001L)
                .findFirst().get().getShoppingListsList().get(0).hasCouponId());
        assert (14100001L == smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000001L)
                .findFirst().get().getShoppingListsList().get(0).getSupplier());
        assert (0 == smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000001L)
                .findFirst().get().getShoppingListsList().get(0)
                .getCommoditiesBoughtList().size());
        // vm2 (2000002L) is matched to RI. Make sure it buys correct quantity
        // of coupon commodity. Also make sure the supplier and couponiD are
        // updated.
        assertTrue(smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000002L)
                .findFirst().get().getShoppingListsList().get(0).hasCouponId());
        assert (15100002L == smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000002L)
                .findFirst().get().getShoppingListsList().get(0).getCouponId());
        assert (14100002L == smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000002L)
                .findFirst().get().getShoppingListsList().get(0).getSupplier());
        assert (3 == smaConverter.getProjectedTraderDTOsWithSMA()
                .stream().filter(a -> a.getOid() == 2000002L)
                .findFirst().get().getShoppingListsList().get(0)
                .getCommoditiesBoughtList().get(0).getQuantity());

        // verify the source and destination of the actions.
        assert (smaConverter.getSmaActions().stream()
                .filter(a -> a.getMove().getShoppingListToMove() == 70001L)
                .findFirst().get().getMove().getDestination() == 14100001L);
        assert (smaConverter.getSmaActions().stream()
                .filter(a -> a.getMove().getShoppingListToMove() == 70001L)
                .findFirst().get().getMove().getSource() == 14100002L);

        assert (smaConverter.getSmaActions().stream()
                .filter(a -> a.getMove().getShoppingListToMove() == 70002L)
                .findFirst().get().getMove().getDestination() == 14100002L);
        assert (smaConverter.getSmaActions().stream()
                .filter(a -> a.getMove().getShoppingListToMove() == 70002L)
                .findFirst().get().getMove().getSource() == 14100001L);

    }

}
