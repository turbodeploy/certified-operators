package com.vmturbo.platform.analysis.translators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Constant;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.StandardWeighted;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Step;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;
/**
 * A test case for the {@link ProtobufToAnalysis} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ProtobufToAnalysisTest {
    // Fields

    // Methods for converting PriceFunctionDTOs.

    @SuppressWarnings("unused")
    private final static Object[] parametersForTestPriceFunction() {
        PriceFunctionTO funcTO1 = PriceFunctionTO.newBuilder()
                        .setConstant(Constant.newBuilder().setValue(100f).build()).build();
        PriceFunctionTO funcTO2 = PriceFunctionTO.newBuilder()
                        .setStandardWeighted(StandardWeighted.newBuilder().setWeight(2.0f).build())
                        .build();
        PriceFunctionTO funcTO3 = PriceFunctionTO.newBuilder()
                        .setStep(Step.newBuilder().setStepAt(0.5f).setPriceAbove(100).setPriceBelow(10).build()).build();
        PriceFunction func1 = PriceFunction.Cache.createConstantPriceFunction(100f);
        PriceFunction func2 = PriceFunction.Cache.createStandardWeightedPriceFunction(2.0f);
        PriceFunction func3 = PriceFunction.Cache.createStepPriceFunction(0.5f, 10, 100);

        return new Object[][] {{funcTO1, func1}, {funcTO2, func2}, {funcTO3, func3}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: priceFunction({0}) == {1}")
    public final void testPriceFunction(PriceFunctionTO funcTO, PriceFunction expect) {
        PriceFunction function = ProtobufToAnalysis.priceFunction(funcTO);
        assertEquals(expect, function);

    }

    // Methods for converting UpdatingFunctionDTOs.

    @Test
    @Ignore
    public final void testUpdatingFunction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting EconomyDTOs.

    @Test
    @Parameters({"0", "1", "35"})
    @TestCaseName("Test #{index}: commoditySpecification({0})")
    public final void testCommoditySpecification(@NonNull int type) {
        CommoditySpecificationTO commSpecTO = CommoditySpecificationTO.newBuilder().setType(type)
                        .setBaseType(1000).build();
        CommoditySpecification spec = ProtobufToAnalysis.commoditySpecification(commSpecTO);
        assertEquals(type, spec.getType());
    }

    @Test
    @Parameters({"0", "1",})
    @TestCaseName("Test #{index}: Basket_ListOfCommoditySpecificationTO({0})")
    public final void testBasket_ListOfCommoditySpecificationTO(int type) {
        CommoditySpecificationTO specTO = CommoditySpecificationTO.newBuilder().setType(type)
                        .setBaseType(1000).build();
        Basket basket = ProtobufToAnalysis.basket(
                        new ArrayList<CommoditySpecificationTO>(Arrays.asList(specTO)));
        assertEquals(new CommoditySpecification(type, 1000), basket.get(0));

    }

    @Test
    @Parameters({"0, 0, 0", "1, 4, 2147483647", "120, 120, 120"})
    @TestCaseName("Test #{index}: Basket_ShoppingListTO({0}, {1}, {2})")
    public final void testBasket_ShoppingListTO(int type, int lowerBound, int upperBound) {
        CommoditySpecificationTO specTO = CommoditySpecificationTO.newBuilder().setType(type)
                        .setBaseType(1000).build();
        CommodityBoughtTO commBoughtTO = CommodityBoughtTO.newBuilder().setSpecification(specTO)
                        .setQuantity(50).setPeakQuantity(50).build();
        ShoppingListTO shopTO = ShoppingListTO.newBuilder().addCommoditiesBought(commBoughtTO)
                        .setMovable(true).setOid(111).setSupplier(222).build();
        Basket basket = ProtobufToAnalysis.basket(shopTO);
        Basket expect = new Basket(new CommoditySpecification(type, 1000));
        assertEquals(expect, basket);
    }

    @Test
    @Parameters({"0", "1", "2"})
    @TestCaseName("Test #{index}: Basket_TraderTO({0})")
    public final void testBasket_TraderTO(int type) {
        TraderTO trader = TraderTO.newBuilder().setOid(1).addCommoditiesSold(CommoditySoldTO
                        .newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder().setType(type)
                                        .setBaseType(1000).build())
                        .build()).build();
        Basket basket = ProtobufToAnalysis.basket(trader);
        Basket expect = new Basket(new CommoditySpecification(type, 1000));
        assertEquals(expect, basket);
    }

    @Test
    public final void testAddShoppingList() {
        Topology topo = new Topology();
        Economy e = new Economy();
        Trader buyer = e.addTrader(0, TraderState.ACTIVE, new Basket(), new Basket());
        CommoditySpecificationTO commSpecTO1 = CommoditySpecificationTO.newBuilder().setType(1).setBaseType(1000).build();
        CommoditySpecificationTO commSpecTO2 = CommoditySpecificationTO.newBuilder().setType(2).setBaseType(1000).build();
        CommodityBoughtTO commBoughtTO1 = CommodityBoughtTO.newBuilder().setSpecification(commSpecTO1)
                        .setQuantity(50).setPeakQuantity(50).build();
        CommodityBoughtTO commBoughtTO2 = CommodityBoughtTO.newBuilder().setSpecification(commSpecTO2)
                        .setQuantity(100).setPeakQuantity(100).build();
        ShoppingListTO shopTO = ShoppingListTO.newBuilder().setOid(0).addCommoditiesBought(commBoughtTO1).addCommoditiesBought(commBoughtTO2).build();
        ShoppingList shopList = ProtobufToAnalysis.addShoppingList(topo, "", buyer, shopTO);
        assertEquals(buyer, shopList.getBuyer());
        assertEquals(50, shopList.getPeakQuantities()[0], TestUtils.FLOATING_POINT_DELTA);
        assertEquals(100, shopList.getPeakQuantities()[1], TestUtils.FLOATING_POINT_DELTA);
        assertEquals(50, shopList.getQuantities()[0], TestUtils.FLOATING_POINT_DELTA);
        assertEquals(100, shopList.getQuantities()[1], TestUtils.FLOATING_POINT_DELTA);

    }

    @Test
    @Ignore
    public final void testPopulateCommoditySoldSettings() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testPopulateCommoditySold() {
        float commUsed = 1f;
        float commPeakUsed = 2f;
        float commMaxUsed = 3f;
        float commSoldCap = 5f;
        int numConusmer = 6;
        float historicalUsed = 4f;
        CommoditySoldSettingsTO settingTO = CommoditySoldSettingsTO.newBuilder().build();
        CommoditySoldTO.Builder commSoldTOBuilder = CommoditySoldTO.newBuilder()
                        .setSettings(settingTO)
                        .setQuantity(commUsed)
                        .setPeakQuantity(commPeakUsed)
                        .setMaxQuantity(commMaxUsed)
                        .setCapacity(commSoldCap)
                        .setNumConsumers(numConusmer)
                        .setThin(true);

        TraderSettingsTO traderSetting = TraderSettingsTO
                        .newBuilder()
                        .setQuoteFunction(
                           QuoteFunctionDTO
                           .newBuilder()
                           .setRiskBased(
                              RiskBased.newBuilder()))
                        .build();

        TraderTO trader = TraderTO.newBuilder().setOid(1).setSettings(traderSetting).build();

        CommoditySold commoditySold = new CommoditySold() {
            private static final long serialVersionUID = 1L;

            @Override
            public @NonNull @PolyRead CommoditySoldSettings getSettings() {
                return Mockito.mock(CommoditySoldSettings.class);
            }

            @Override
            public double getEffectiveCapacity() {
                return 0;
            }
        };

        ProtobufToAnalysis.populateCommoditySold(commSoldTOBuilder.build(), commoditySold, trader);

        float precision = 0.001f;
        assertEquals(commUsed, commoditySold.getQuantity(), precision);
        assertEquals(commUsed, commoditySold.getStartQuantity(), precision);
        assertEquals(commPeakUsed, commoditySold.getPeakQuantity(), precision);
        assertEquals(commPeakUsed, commoditySold.getStartPeakQuantity(), precision);
        assertEquals(commMaxUsed, commoditySold.getMaxQuantity(), precision);
        assertEquals(commSoldCap, commoditySold.getCapacity(), precision);
        assertEquals(numConusmer, commoditySold.getNumConsumers(), precision);
        assertEquals(commUsed, commoditySold.getHistoricalOrElseCurrentQuantity(), precision);
        assertTrue(commoditySold.isThin());

        //If the commodity is not set the value for historical quantity should be -1
        ProtobufToAnalysis.populateCommoditySold(
                        commSoldTOBuilder
                            .setHistoricalQuantity(historicalUsed)
                            .build(),
                        commoditySold,
                        trader);

        assertEquals(historicalUsed, commoditySold.getHistoricalOrElseCurrentQuantity(), precision);

    }

    @Test
    @Ignore
    public final void testPopulateTraderSettings() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testTraderState() {
        TraderStateTO active = TraderStateTO.ACTIVE;
        TraderStateTO inactive = TraderStateTO.INACTIVE;
        assertEquals(TraderState.ACTIVE, ProtobufToAnalysis.traderState(active));
        assertEquals(TraderState.INACTIVE, ProtobufToAnalysis.traderState(inactive));
    }

    @Test
    @Ignore
    public final void testAddTrader() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting ActionDTOs.

    @Test
    @Ignore
    public final void testAction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting CommunicationDTOs.

    @Test
    @Ignore
    public final void testPopulateUpdatingFunctions() {
        fail("Not yet implemented"); // TODO
    }

} // end ProtobufToAnalysisTest class
