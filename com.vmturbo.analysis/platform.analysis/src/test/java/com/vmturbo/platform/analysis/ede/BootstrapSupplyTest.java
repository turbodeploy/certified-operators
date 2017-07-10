package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

public class BootstrapSupplyTest {

    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final int ST_TYPE = 2;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification ST_AMT = new CommoditySpecification(2);

    @Test
    public void testShopTogetherBootstrapWithEnoughSupply() {
        Economy economy = new Economy();
        // create two pms, one is smaller another is bigger in terms of cpu capacity
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(100);
        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(Arrays.asList(1l)));
        pm2.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(200);
        // create two storages with same configuration except the clique
        // st1 only associates with pm1 and st2 only associates with pm2
        Trader st1 = economy.addTrader(ST_TYPE, TraderState.ACTIVE, new Basket(ST_AMT),
                        new HashSet<>(Arrays.asList(0l)));
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setCapacity(300);
        Trader st2 = economy.addTrader(ST_TYPE, TraderState.ACTIVE, new Basket(ST_AMT),
                        new HashSet<>(Arrays.asList(1l)));
        st2.getCommoditiesSold().get(st2.getBasketSold().indexOf(ST_AMT)).setCapacity(300);
        // create a vm1 that is requesting higher cpu than its current supplier
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(CPU, MEM));
        sl1.setQuantity(sl1.getBasket().indexOf(CPU), 150);
        sl1.setMovable(true);
        sl1.move(pm1);
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setQuantity(150);
        ShoppingList sl2 = economy.addBasketBought(vm1, new Basket(ST_AMT));
        sl2.setQuantity(sl2.getBasket().indexOf(ST_AMT), 100);
        sl2.setMovable(true);
        sl2.move(st1);
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setQuantity(100);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrapForIndividualBuyer(economy, vm1);
        assertTrue(bootStrapActionList.size() == 1);
        Action compoundMove = bootStrapActionList.get(0);
        assertEquals(ActionType.COMPOUND_MOVE, compoundMove.getType());
        Move expect1 = new Move(economy, sl1, pm1, pm2);
        Move expect2 = new Move(economy, sl2, st1, st2);
        List<Move> moves = ((CompoundMove)compoundMove).getConstituentMoves();
        assertTrue(moves.size() == 2);
        // order of moves in compound move is not deterministic
        assertTrue((expect1.equals(moves.get(0)) && expect2.equals(moves.get(1)))
                        || expect1.equals(moves.get(1)) && expect2.equals(moves.get(0)));
    }

    @Test
    public void testShopTogetherBootstrapWithNewSupply() {
        Economy economy = new Economy();
        // create one pm with smaller capacity
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(100);
        // create one storages with adequate capacity
        Trader st1 = economy.addTrader(ST_TYPE, TraderState.ACTIVE, new Basket(ST_AMT),
                        new HashSet<>(Arrays.asList(0l)));
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setCapacity(300);
        // create a vm that is requesting higher cpu than any seller in market
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(CPU, MEM));
        sl1.setQuantity(sl1.getBasket().indexOf(CPU), 200);
        sl1.setMovable(true);
        sl1.move(pm1);
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setQuantity(200);
        ShoppingList sl2 = economy.addBasketBought(vm1, new Basket(ST_AMT));
        sl2.setQuantity(sl2.getBasket().indexOf(ST_AMT), 100);
        sl2.setMovable(true);
        sl2.move(st1);
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setQuantity(100);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.shopTogetherBootstrapForIndividualBuyer(economy, vm1);
        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.PROVISION_BY_DEMAND, bootStrapActionList.get(0).getType());
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(1).getType());
        ProvisionByDemand provisionByDemand = (ProvisionByDemand)bootStrapActionList.get(0);
        List<Move> moves = ((CompoundMove)bootStrapActionList.get(1)).getConstituentMoves();
        assertEquals(sl1, provisionByDemand.getModelBuyer());
        assertEquals(pm1, provisionByDemand.getModelSeller());
        Move expect1 = new Move(economy, sl1, pm1, provisionByDemand.getProvisionedSeller());
        assertTrue(expect1.equals(moves.get(0)));
    }
}
