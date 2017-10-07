package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.*;

import java.util.List;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Constant;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.StandardWeighted;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Delta;

public class PlacementIntegrationTest {

    @Test
    public void testPlacementDecisions() {
        CommoditySpecificationTO cpuSpecTO =
                        CommoditySpecificationTO.newBuilder().setBaseType(0).setType(1).build();
        CommoditySpecificationTO bicliqueSpecTO =
                        CommoditySpecificationTO.newBuilder().setType(2).setBaseType(3).build();
        CommoditySpecificationTO storageProvisionSpecTO =
                        CommoditySpecificationTO.newBuilder().setBaseType(4).setType(5).build();

        CommodityBoughtTO cpuBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(100)
                        .setPeakQuantity(100).setSpecification(cpuSpecTO).build();
        CommodityBoughtTO bicliqueBoughtTO =
                        CommodityBoughtTO.newBuilder().setSpecification(bicliqueSpecTO).build();
        CommodityBoughtTO storageProvisionBoughtTO = CommodityBoughtTO.newBuilder().setQuantity(50)
                        .setPeakQuantity(50).setSpecification(storageProvisionSpecTO).build();

        PriceFunctionTO standardPriceTO = PriceFunctionTO.newBuilder().setStandardWeighted(
                        StandardWeighted.newBuilder().setWeight(
                                        1).build()).build();
        UpdatingFunctionTO ufTO = UpdatingFunctionTO.newBuilder().setDelta(Delta.newBuilder()
                                                                           .build()).build();
        PriceFunctionTO constantPriceTO = PriceFunctionTO.newBuilder().setConstant(
                        Constant.newBuilder().setValue(0.1f).build()).build();
        CommoditySoldSettingsTO standardSettingTO = CommoditySoldSettingsTO.newBuilder()
                        .setPriceFunction(standardPriceTO).setUpdateFunction(ufTO).build();
        CommoditySoldSettingsTO constantSettingTO = CommoditySoldSettingsTO.newBuilder()
                        .setPriceFunction(constantPriceTO).setUpdateFunction(ufTO).build();

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
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(true).build())
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
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(false).build())
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
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(false).build())
                        .addCommoditiesSold(cpuSoldByPM1).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0).build();
        TraderTO pm2TO = TraderTO.newBuilder().setOid(45678).setType(66666)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(false).build())
                        .addCommoditiesSold(cpuSoldByPM2).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0).build();
        TraderTO st1TO = TraderTO.newBuilder().setOid(56789).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(false).build())
                        .addCommoditiesSold(storageSoldByST1).addCommoditiesSold(bicliqueSoldTO)
                        .addCliques(0).build();
        TraderTO st2TO = TraderTO.newBuilder().setOid(67890).setType(77777)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(false).build())
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
        List<Action> actions = Placement.placementDecisions(economy);
        assertTrue(actions.size()==2);
        assertTrue(actions.get(0) instanceof Move);
        assertEquals(shopAloneVM, actions.get(0).getActionTarget());

    }
}