package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
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
import com.vmturbo.platform.analysis.utilities.QuoteTracker;

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
                        .setSettings(standardSettingTO).build();
        CommoditySoldTO cpuSoldByPM2 = CommoditySoldTO.newBuilder().setSpecification(cpuSpecTO)
                        .setQuantity(100).setPeakQuantity(100).setMaxQuantity(100).setCapacity(2000)
                        .setSettings(standardSettingTO).build();
        CommoditySoldTO storageSoldByST1 = CommoditySoldTO.newBuilder()
                        .setSpecification(storageProvisionSpecTO).setQuantity(1000)
                        .setPeakQuantity(1000).setMaxQuantity(1000).setCapacity(2000)
                        .setSettings(standardSettingTO).build();
        CommoditySoldTO storageSoldByST2 =
                        CommoditySoldTO.newBuilder().setSpecification(storageProvisionSpecTO)
                        .setQuantity(100).setPeakQuantity(100).setMaxQuantity(100)
                        .setCapacity(2000).setSettings(standardSettingTO).build();
        CommoditySoldTO bicliqueSoldTO = CommoditySoldTO.newBuilder()
                        .setSpecification(bicliqueSpecTO)
                        .setSettings(constantSettingTO).build();

        // shop together vm should have shoptogether falg true and not buying biclique commodity
        TraderTO shopTogetherVMTO = TraderTO.newBuilder().setOid(12345)
                        .setType(55555).setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherTrueTO)
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
                        .addCliques(0).build();
        TraderTO pm2TO = TraderTO.newBuilder().setOid(45678).setType(66666)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(cpuSoldByPM2).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0).build();
        TraderTO st1TO = TraderTO.newBuilder().setOid(56789).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(storageSoldByST1).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0).build();
        TraderTO st2TO = TraderTO.newBuilder().setOid(67890).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(shoptogetherFalseTO)
                        .addCommoditiesSold(storageSoldByST2).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0)
                        .build();

        Topology topology = new Topology();
        Trader shopAloneVM = ProtobufToAnalysis.addTrader(topology, shopAloneVMTO);
        Trader shopTogetherVM = ProtobufToAnalysis.addTrader(topology, shopTogetherVMTO);
        ProtobufToAnalysis.addTrader(topology, pm1TO).getSettings().setCanAcceptNewCustomers(true);
        ProtobufToAnalysis.addTrader(topology, pm2TO).getSettings().setCanAcceptNewCustomers(true);
        ProtobufToAnalysis.addTrader(topology, st1TO).getSettings().setCanAcceptNewCustomers(true);
        ProtobufToAnalysis.addTrader(topology, st2TO).getSettings().setCanAcceptNewCustomers(true);
        topology.populateMarketsWithSellers();
        Economy economy = (Economy)topology.getEconomy();
        economy.composeMarketSubsetForPlacement();
        List<Action> actions = Placement.placementDecisions(economy).getActions();
        assertTrue(actions.size()==2);
        assertTrue(actions.get(0) instanceof Move);
        assertEquals(shopAloneVM, actions.get(0).getActionTarget());
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
     * VM buying 1000 mem, 200 CPU
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
     * The unplaced trader explanation should say that pm_b has insufficient CPU because it has the most CPU
     * of anything with insufficient memory.
     *
     * It should say that pm_d has insufficient Mem because it has the most memory of any host with
     * sufficient CPU.
     */
    @Test
    public void testUnplacedTradersRiskBasedQuote() {
        final CommodityBoughtTO cpuBought = commodityBought(cpuSpecTO, 200);
        final CommodityBoughtTO memBought = commodityBought(memSpecTO, 1000);

        final TraderTO vm = virtualMachineTO(12345L, "test_vm", cpuBought, memBought);
        final TraderTO pmA = physicalMachineTO(PM_SELLER_A_OID, "pm_a", 1200, 120);
        final TraderTO pmB = physicalMachineTO(PM_SELLER_B_OID, "pm_b", 2000, 150); // Enough memory, closest CPU
        final TraderTO pmC = physicalMachineTO(PM_SELLER_C_OID, "pm_c", 1500, 100);
        final TraderTO pmD = physicalMachineTO(PM_SELLER_D_OID, "pm_d", 500, 250);  // Enough CPU, not enough memory
        final TraderTO pmE = physicalMachineTO(PM_SELLER_E_OID, "pm_e", 800, 175);  // Neither enough CPU or memory

        final Topology topology = new Topology();
        ProtobufToAnalysis.addTrader(topology, vm);
        Stream.of(pmA, pmB, pmC, pmD, pmE).forEach(trader -> {
            final Trader t = ProtobufToAnalysis.addTrader(topology, trader);
            t.getSettings().setCanAcceptNewCustomers(true);
            t.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
            t.getSettings().setCostFunction(costFunction);
        });

        topology.populateMarketsWithSellers();
        Economy economy = (Economy)topology.getEconomy();
        economy.composeMarketSubsetForPlacement();
        final Map<Trader, Collection<QuoteTracker>> unplacedTraders =
            Placement.placementDecisions (economy).getUnplacedTraders();

        assertEquals(1, unplacedTraders.size());
        final String explanation = explanationsFor(unplacedTraders).get(0);

        assertTrue(explanation.contains("CPU| (200.0/150.0) on seller pm_b"));
        assertTrue(explanation.contains("MEM| (1000.0/500.0) on seller pm_d"));
    }

    /**
     * VM buying 1000 mem, 200 CPU, 50 IO
     * +--------+-----------------+-----------------+-----------------+
     * |host    |       Mem       |       CPU       |        IO       |
     * +--------+-----------------+-----------------+-----------------+
     * |pm_a    |      1200       |       120       |        30       |
     * +--------+-----------------+-----------------+-----------------+
     * |pm_b    |       700       |       150       |        90       |
     * +--------+-----------------+-----------------+-----------------+
     * |pm_c    |       800       |       250       |        20       |
     * +--------+-----------------+-----------------+-----------------+
     * |pm_d    |       900       |       180       |        40       |
     * +--------+-----------------+-----------------+-----------------+
     * |pm_e    |         1       |       250       |         1       |
     * +--------+-----------------+-----------------+-----------------+
     *
     * The unplaced trader explanation should say that pm_b has insufficient CPU because it has the most CPU
     * of anything with insufficient memory.
     *
     * It should say that pm_d has insufficient Mem because it has the most memory of any host with
     * sufficient CPU.
     */
    @Test
    public void testUnplacedTradersCpuMemIo() {
        final CommodityBoughtTO cpuBought = commodityBought(cpuSpecTO, 200);
        final CommodityBoughtTO memBought = commodityBought(memSpecTO, 1000);
        final CommodityBoughtTO ioBought = commodityBought(ioSpecTO, 50);

        final TraderTO vm = virtualMachineTO(12345L, "test_vm", cpuBought, memBought, ioBought);
        final TraderTO pmA = physicalMachineTO(PM_SELLER_A_OID, "pm_a", 1200, 120, 30); // Insufficient CPU & IO
        final TraderTO pmB = physicalMachineTO(PM_SELLER_B_OID, "pm_b", 700, 150, 90); // Insufficient CPU & Mem
        final TraderTO pmC = physicalMachineTO(PM_SELLER_C_OID, "pm_c", 800, 250, 20); // Insufficient Mem & IO
        final TraderTO pmD = physicalMachineTO(PM_SELLER_D_OID, "pm_d", 900, 180, 40);  // Insufficient of all
        final TraderTO pmE = physicalMachineTO(PM_SELLER_E_OID, "pm_e", 1, 250, 1); // Insufficient Mem & IO

        final Topology topology = new Topology();
        ProtobufToAnalysis.addTrader(topology, vm);
        Stream.of(pmA, pmB, pmC, pmD, pmE).forEach(trader -> {
            final Trader t = ProtobufToAnalysis.addTrader(topology, trader);
            t.getSettings().setCanAcceptNewCustomers(true);
            t.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
            t.getSettings().setCostFunction(costFunction);
        });

        topology.populateMarketsWithSellers();
        Economy economy = (Economy)topology.getEconomy();
        economy.composeMarketSubsetForPlacement();

        final Map<Trader, Collection<QuoteTracker>> unplacedTraders =
            Placement.placementDecisions (economy).getUnplacedTraders();

        assertEquals(1, unplacedTraders.size());
        final String explanation = explanationsFor(unplacedTraders).get(0);

        assertTrue(explanation.contains("CPU| (200.0/120.0), and IO_THROUGHPUT| (50.0/30.0) on seller pm_a"));
        assertTrue(explanation.contains("CPU| (200.0/150.0), and MEM| (1000.0/700.0) on seller pm_b"));
        assertTrue(explanation.contains("MEM| (1000.0/800.0), and IO_THROUGHPUT| (50.0/20.0) on seller pm_c"));
    }

    private List<String> explanationsFor(Map<Trader, Collection<QuoteTracker>> unplacedTraders) {
        return unplacedTraders.entrySet().stream()
            .map(entry -> {
                final Trader trader = entry.getKey();
                final Collection<QuoteTracker> quoteTrackers = entry.getValue();

                return trader.getDebugInfoNeverUseInCode() + ": " + quoteTrackers.stream()
                    .filter(QuoteTracker::hasQuotesToExplain)
                    .map(QuoteTracker::explainInterestingSellers)
                    .collect(Collectors.joining(","));
            }).collect(Collectors.toList());
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
