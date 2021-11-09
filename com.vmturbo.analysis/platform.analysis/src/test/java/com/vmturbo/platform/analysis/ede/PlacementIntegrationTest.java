package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Constant;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.StandardWeighted;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Delta;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class PlacementIntegrationTest {

    private final TraderSettingsTO shoptogetherTrueTO =
        TraderSettingsTO.newBuilder().setIsShopTogether(true)
            .setMoveCostFactor(0.005f)
                .setCurrentContext(Context.newBuilder().setRegionId(10L).setBalanceAccount(BalanceAccountDTO.newBuilder().setId(100L).build()).build())
            .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                .setSumOfCommodity(SumOfCommodity
                    .newBuilder().build())
                .build())
            .build();
    private final TraderSettingsTO shoptogetherFalseTO =
        TraderSettingsTO.newBuilder().setIsShopTogether(false)
            .setMoveCostFactor(0.005f)
                .setCurrentContext(Context.newBuilder().setRegionId(10L).setBalanceAccount(BalanceAccountDTO.newBuilder().setId(100L).build()).build())
            .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                .setSumOfCommodity(SumOfCommodity
                    .newBuilder().build())
                .build())
            .build();
    private final CommoditySpecificationTO cpuSpecTO =
        CommoditySpecificationTO.newBuilder().setBaseType(0).setType(1)
            .setDebugInfoNeverUseInCode("CPU|").build();
    private final CommoditySpecificationTO bicliqueSpecTO =
        CommoditySpecificationTO.newBuilder().setType(2).setBaseType(3)
                .setDebugInfoNeverUseInCode("Biclique|2").build();
    private final CommoditySpecificationTO storageProvisionSpecTO =
        CommoditySpecificationTO.newBuilder().setBaseType(4).setType(5)
                .setDebugInfoNeverUseInCode("ST_PROVISIONED").build();
    private final CommoditySpecificationTO memSpecTO =
        CommoditySpecificationTO.newBuilder().setBaseType(6).setType(7)
            .setDebugInfoNeverUseInCode("MEM|").build();
    private final CommoditySpecificationTO ioSpecTO =
        CommoditySpecificationTO.newBuilder().setBaseType(8).setType(9)
            .setDebugInfoNeverUseInCode("IO_THROUGHPUT|").build();

    private final PriceFunctionTO standardPriceTO = PriceFunctionTO.newBuilder().setStandardWeighted(
        StandardWeighted.newBuilder().setWeight(
            1).build()).build();
    private final UpdatingFunctionTO ufTO = UpdatingFunctionTO.newBuilder().setDelta(Delta.newBuilder()
        .build()).build();
    private final PriceFunctionTO constantPriceTO = PriceFunctionTO.newBuilder().setConstant(
        Constant.newBuilder().setValue(0.1f).build()).build();
    private final CommoditySoldSettingsTO standardSettingTO = CommoditySoldSettingsTO.newBuilder()
        .setPriceFunction(standardPriceTO).setUpdateFunction(ufTO).build();
    private final CommoditySoldSettingsTO constantSettingTO = CommoditySoldSettingsTO.newBuilder()
        .setPriceFunction(constantPriceTO).setUpdateFunction(ufTO).build();

    private static final int LICENSE_ID = 7654;
    private static final int COUPON_ID = 7656;

    @Test
    public void testPlacementDecisions() {
        final CommodityBoughtTO cpuBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(100)
            .setPeakQuantity(100).setSpecification(cpuSpecTO).build();
        final CommodityBoughtTO bicliqueBoughtTO =
            CommodityBoughtTO.newBuilder().setSpecification(bicliqueSpecTO).build();
        final CommodityBoughtTO storageProvisionBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(50)
            .setPeakQuantity(50).setSpecification(storageProvisionSpecTO).build();

        CommoditySoldTO cpuSoldByPM1 = CommoditySoldTO.newBuilder().setSpecification(cpuSpecTO)
                        .setQuantity(1000).setPeakQuantity(1000)
                        .setMaxQuantity(1000).setCapacity(2000)
                        .setSettings(standardSettingTO.toBuilder().clearUpdateFunction().build()).build();
        CommoditySoldTO cpuSoldByPM2 = CommoditySoldTO.newBuilder().setSpecification(cpuSpecTO)
                        .setQuantity(100).setPeakQuantity(100).setMaxQuantity(100).setCapacity(2000)
                        .setSettings(standardSettingTO.toBuilder().clearUpdateFunction().build()).build();
        CommoditySoldTO storageSoldByST1 = CommoditySoldTO.newBuilder()
                        .setSpecification(storageProvisionSpecTO).setQuantity(1000)
                        .setPeakQuantity(1000).setMaxQuantity(1000).setCapacity(2000)
                        .setSettings(standardSettingTO.toBuilder().clearUpdateFunction().build()).build();
        CommoditySoldTO storageSoldByST2 =
                        CommoditySoldTO.newBuilder().setSpecification(storageProvisionSpecTO)
                        .setQuantity(100).setPeakQuantity(100).setMaxQuantity(100)
                        .setCapacity(2000).setSettings(standardSettingTO.toBuilder().clearUpdateFunction().build()).build();
        CommoditySoldTO bicliqueSoldTO = CommoditySoldTO.newBuilder()
                        .setSpecification(bicliqueSpecTO)
                        .setSettings(constantSettingTO).build();

        // shop together vm should have shoptogether falg true and not buying biclique commodity
        TraderTO shopTogetherVMTO = TraderTO.newBuilder().setOid(12345)
                        .setType(55555).setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherTrueTO)
                        .setDebugInfoNeverUseInCode("shopTogetherVM")
                        .addShoppingLists(ShoppingListTO.newBuilder()
                                        .setOid(11112).setMovable(true).setSupplier(34567)
                                        .addCommoditiesBought(cpuBoughtTO).build())
                        .addShoppingLists(ShoppingListTO.newBuilder()
                                        .setOid(11111).setMovable(true).setSupplier(56789)
                                        .addCommoditiesBought(storageProvisionBoughtTO).build())
                        .build();
        // non shop together vm should have flag false and buying bicliquc commodity
        TraderTO shopAloneVMTO = TraderTO.newBuilder().setOid(23456).setType(55555)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .setDebugInfoNeverUseInCode("shopAloneVM")
                        .addShoppingLists(ShoppingListTO.newBuilder()
                                        .setOid(11113).setMovable(true).setSupplier(34567)
                                        .addCommoditiesBought(cpuBoughtTO)
                                        .addCommoditiesBought(bicliqueBoughtTO).build())
                        .addShoppingLists(ShoppingListTO.newBuilder()
                                        .setOid(11114).setMovable(true).setSupplier(56789)
                                        .addCommoditiesBought(storageProvisionBoughtTO)
                                        .addCommoditiesBought(bicliqueBoughtTO).build())
                        .build();
        TraderTO pm1TO = TraderTO.newBuilder().setOid(34567).setType(66666)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(cpuSoldByPM1).addCommoditiesSold(bicliqueSoldTO)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCliques(0).build();
        TraderTO pm2TO = TraderTO.newBuilder().setOid(45678).setType(66666)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(cpuSoldByPM2).addCommoditiesSold(bicliqueSoldTO)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCliques(0).build();
        TraderTO st1TO = TraderTO.newBuilder().setOid(56789).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(storageSoldByST1).addCommoditiesSold(bicliqueSoldTO)
                        .setType(EntityType.STORAGE_VALUE)
                        .addCliques(0).build();
        TraderTO st2TO = TraderTO.newBuilder().setOid(67890).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(storageSoldByST2).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0)
                        .setType(EntityType.STORAGE_VALUE)
                        .build();

        Topology topology = new Topology();
        Trader shopAloneVM = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO);
        Trader shopTogetherVM = ProtobufToAnalysis.addTrader(topology, shopTogetherVMTO);
        Trader pm1Trader = ProtobufToAnalysis.addTrader(topology, pm1TO);
        pm1Trader.getSettings().setCanAcceptNewCustomers(true);
        Trader pm2Trader = ProtobufToAnalysis.addTrader(topology, pm2TO);
        pm2Trader.getSettings().setCanAcceptNewCustomers(true);
        Trader st1Trader = ProtobufToAnalysis.addTrader(topology, st1TO);
        st1Trader.getSettings().setCanAcceptNewCustomers(true);
        Trader st2Trader = ProtobufToAnalysis.addTrader(topology, st2TO);
        st2Trader.getSettings().setCanAcceptNewCustomers(true);
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();
        Economy economy = (Economy)topology.getEconomy();
        economy.composeMarketSubsetForPlacement();
        List<Action> actions = Placement.placementDecisions(economy).getActions();
        // Assert that both VMs move from PM1/ST1 to PM2/ST2
        assertEquals(4, actions.size());
        Set<ShoppingList> shopAloneSls = economy.getMarketsAsBuyer(shopAloneVM).keySet();
        ShoppingList shopAloneComputeSl = shopAloneSls.stream().filter(
            sl -> sl.getSupplier().getType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst().get();
        ShoppingList shopAloneStorageSl = shopAloneSls.stream().filter(
            sl -> sl.getSupplier().getType() == EntityType.STORAGE_VALUE).findFirst().get();
        Set<ShoppingList> shopTogetherSls = economy.getMarketsAsBuyer(shopTogetherVM).keySet();
        ShoppingList shopTogetherComputeSl = shopTogetherSls.stream().filter(
            sl -> sl.getSupplier().getType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst().get();
        ShoppingList shopTogetherStorageSl = shopTogetherSls.stream().filter(
            sl -> sl.getSupplier().getType() == EntityType.STORAGE_VALUE).findFirst().get();
        Set<Action> expectedActions = Sets.newHashSet(new Move(economy, shopAloneComputeSl, pm1Trader, pm2Trader),
            new Move(economy, shopAloneStorageSl, st1Trader, st2Trader),
            new Move(economy, shopTogetherComputeSl, pm1Trader, pm2Trader),
            new Move(economy, shopTogetherStorageSl, st1Trader, st2Trader));
        assertEquals(Sets.newHashSet(actions), expectedActions);
    }

    private static final long VM_BUYER_OID = 123L;
    private static final long PM_SELLER_A_OID = 111L;
    private static final long PM_SELLER_B_OID = 222L;
    private static final long PM_SELLER_C_OID = 333L;
    private static final long PM_SELLER_D_OID = 444L;
    private static final long PM_SELLER_E_OID = 555L;

    private static final int VM_TYPE = 99999;
    private static final int PM_TYPE = 88888;

    private final ComputeTierCostDTO.Builder costBundleBuilder = ComputeTierCostDTO.newBuilder()
        .setLicenseCommodityBaseType(LICENSE_ID).setCouponBaseType(COUPON_ID);
    private final CostDTO costDTO = CostDTO.newBuilder()
            .setComputeTierCost(costBundleBuilder.addCostTupleList(CostTuple.newBuilder()
                    .setPrice(100.0).setRegionId(10L).setBusinessAccountId(1000L).build()))
            .build();
    private final CostFunction costFunction = CostFunctionFactory.createCostFunction(costDTO);

    private static CommodityBoughtTO commodityBought(@Nonnull final CommoditySpecificationTO commSpec,
                                               final float quantity) {
        return CommodityBoughtTO.newBuilder()
            .setQuantity(quantity)
            .setPeakQuantity(quantity)
            .setSpecification(commSpec)
            .build();
    }

    /**
     * VM buying 1600 mem, 300 CPU
     * +--------+-----------------+-----------------+
     * |host    |       Mem       |       CPU       |
     * +--------+-----------------+-----------------+
     * |pm_a    |      1200       |       120       |
     * +--------+-----------------+-----------------+
     * |pm_b    |      2000       |       150       |
     * +--------+-----------------+-----------------+
     * |pm_c    |      1500       |       100       |
     * +--------+-----------------+-----------------+
     * |pm_d    |       500       |       250       |
     * +--------+-----------------+-----------------+
     * |pm_e    |       800       |       175       |
     * +--------+-----------------+-----------------+
     *
     * The unplaced trader explanation should say that vm sl get infinity quote on CPU because
     * there is 1 seller that lacks CPU amount, all others have insufficient amount in MEM and CPU
     * commodities.
     *
     */

    @Test
    public void testUnplacedTradersRiskBasedQuote() {
        final CommodityBoughtTO cpuBought = commodityBought(cpuSpecTO, 300);
        final CommodityBoughtTO memBought = commodityBought(memSpecTO, 1600);

        final TraderTO vm = virtualMachineTO(12345L, "test_vm", cpuBought, memBought);
        final TraderTO pmA = physicalMachineTO(PM_SELLER_A_OID, "pm_a", 1200, 120);
        final TraderTO pmB = physicalMachineTO(PM_SELLER_B_OID, "pm_b", 2000, 150); // Enough memory, insufficient CPU
        final TraderTO pmC = physicalMachineTO(PM_SELLER_C_OID, "pm_c", 1500, 100);
        final TraderTO pmD = physicalMachineTO(PM_SELLER_D_OID, "pm_d", 500, 250);
        final TraderTO pmE = physicalMachineTO(PM_SELLER_E_OID, "pm_e", 800, 175);

        final Topology topology = new Topology();
        final Trader vmTrader = ProtobufToAnalysis.addTrader(topology, vm);
        Stream.of(pmA, pmB, pmC, pmD, pmE).forEach(trader -> {
            final Trader t = ProtobufToAnalysis.addTrader(topology, trader);
            t.getSettings().setCanAcceptNewCustomers(true);
            t.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
            t.getSettings().setCostFunction(costFunction);
        });

        topology.populateMarketsWithSellersAndMergeConsumerCoverage();
        Economy economy = (Economy)topology.getEconomy();
        economy.composeMarketSubsetForPlacement();
        PlacementResults results = Placement.placementDecisions(economy);
        Map<Trader, List<InfiniteQuoteExplanation>> explanationByTrader = results
                .populateExplanationForInfinityQuoteTraders();

        assertEquals(1, explanationByTrader.size());
        List<InfiniteQuoteExplanation> explanations = explanationByTrader.get(vmTrader);
        assertTrue(explanations.size() == 1);

        assertTrue(explanations.get(0).commBundle.size() == 1);
        assertTrue(explanations.get(0).commBundle.iterator().next().commSpec.equals((ProtobufToAnalysis
                .commoditySpecification(cpuSpecTO))));
        assertEquals(300, explanations.get(0).commBundle.iterator().next().requestedAmount, 0);
        assertTrue(explanations.get(0).providerType.get() == pmB.getType());
        assertTrue(explanations.get(0).seller.get().getOid() == pmB.getOid());
    }

    private TraderTO virtualMachineTO(final long oid,
                                      @Nonnull final String debugInfo,
                                      @Nonnull final CommodityBoughtTO... commoditiesBought) {
        return TraderTO.newBuilder().setOid(oid)
            .setType(VM_TYPE)
            .setDebugInfoNeverUseInCode(debugInfo)
            .setState(TraderStateTO.ACTIVE)
            .setSettings(shoptogetherTrueTO)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(VM_BUYER_OID)
                .setMovable(true)
                .setSupplier(PM_SELLER_A_OID)
                .addAllCommoditiesBought(Arrays.asList(commoditiesBought))
                .build())
            .build();
    }

    private TraderTO physicalMachineTO(final long oid,
                                       @Nonnull final String debugInfo,
                                       final float memCapacity,
                                       final float cpuCapacity) {
        CommoditySoldTO cpuSold = CommoditySoldTO.newBuilder()
            .setSpecification(cpuSpecTO)
            .setQuantity(0)
            .setPeakQuantity(0)
            .setMaxQuantity(0)
            .setCapacity(cpuCapacity)
            .setSettings(standardSettingTO)
            .build();

        CommoditySoldTO memSold = CommoditySoldTO.newBuilder()
            .setSpecification(memSpecTO)
            .setQuantity(0)
            .setPeakQuantity(0)
            .setMaxQuantity(0)
            .setCapacity(memCapacity)
            .setSettings(standardSettingTO)
            .build();

        return TraderTO.newBuilder()
            .setOid(oid)
            .setDebugInfoNeverUseInCode(debugInfo)
            .setType(PM_TYPE)
            .setState(TraderStateTO.ACTIVE)
            .setSettings(shoptogetherFalseTO)
            .addCommoditiesSold(cpuSold)
            .addCommoditiesSold(memSold)
            .addCliques(0)
            .build();
    }

    private TraderTO physicalMachineTO(final long oid,
                                       @Nonnull final String debugInfo,
                                       final float memCapacity,
                                       final float cpuCapacity,
                                       final float ioCapacity) {
        CommoditySoldTO cpuSold = CommoditySoldTO.newBuilder()
            .setSpecification(cpuSpecTO)
            .setQuantity(0)
            .setPeakQuantity(0)
            .setMaxQuantity(0)
            .setCapacity(cpuCapacity)
            .setSettings(standardSettingTO)
            .build();

        CommoditySoldTO memSold = CommoditySoldTO.newBuilder()
            .setSpecification(memSpecTO)
            .setQuantity(0)
            .setPeakQuantity(0)
            .setMaxQuantity(0)
            .setCapacity(memCapacity)
            .setSettings(standardSettingTO)
            .build();

        CommoditySoldTO ioSold = CommoditySoldTO.newBuilder()
            .setSpecification(ioSpecTO)
            .setQuantity(0)
            .setPeakQuantity(0)
            .setMaxQuantity(0)
            .setCapacity(ioCapacity)
            .setSettings(standardSettingTO)
            .build();

        return TraderTO.newBuilder()
            .setOid(oid)
            .setDebugInfoNeverUseInCode(debugInfo)
            .setType(PM_TYPE)
            .setState(TraderStateTO.ACTIVE)
            .setSettings(shoptogetherFalseTO)
            .addCommoditiesSold(cpuSold)
            .addCommoditiesSold(memSold)
            .addCommoditiesSold(ioSold)
            .addCliques(0)
            .build();
    }
}
