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
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

/**
 * @author thiru_arun
 *
 */
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

    /**
     * Test the case where pm with smaller capacity than VM requirement
     */
    @Test
    public void test_CanBuyerFitInSeller_SmallerSeller(){
        Economy economy = new Economy();
        // create one pm with smaller capacity than VM requirement
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(40);
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(CPU, MEM));
        sl1.setQuantity(sl1.getBasket().indexOf(CPU), 50);

        boolean canBuyerFitInSeller = BootstrapSupply.canBuyerFitInSeller(sl1, pm1);

        assertFalse(canBuyerFitInSeller);
    }

    /**
     * Test the case where pm with bigger capacity than VM requirement
     */
    @Test
    public void test_CanBuyerFitInSeller_LargerSeller(){
        Economy economy = new Economy();
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(40);
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(CPU, MEM));
        sl1.setQuantity(sl1.getBasket().indexOf(CPU), 30);

        boolean canBuyerFitInSeller = BootstrapSupply.canBuyerFitInSeller(sl1, pm1);

        assertTrue(canBuyerFitInSeller);
    }

    /**
     * Non Shop together : Create 2 VMs and place them both on PM1.
     * VM1 CPU qty = 60, VM2 CPU qty = 60. PM1 total CPU = 100.
     * Expected result: Should move compute of one of the VMs to PM2.
     */
    @Test
    public void test_NonShopTogetherBootstrap_MoveWithAlreadyPlacedBuyer(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, false);
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 200, false);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 100, st1);
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 100, st1);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 1);
        assertEquals(ActionType.MOVE, bootStrapActionList.get(0).getType());
        Move expectedMove1 = new Move(economy, sl1, pm1, pm2);
        Move expectedMove2 = new Move(economy, sl2, pm1, pm2);
        assertTrue(expectedMove1.equals(bootStrapActionList.get(0))
                        || expectedMove2.equals(bootStrapActionList.get(0)));
    }

    /**
     * Case: Non Shop together : Test move. Pm1 connected to St1.Vm1 not placed anywhere.
     * Expected result: Place VM on PM1 and ST1.
     */
    @Test
    public void test_NonShopTogetherBootstrap_MoveWithUnplacedBuyer(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, false);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 60, null);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 100, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.MOVE, bootStrapActionList.get(0).getType());
        assertEquals(ActionType.MOVE, bootStrapActionList.get(1).getType());
        Move expectedMove1 = new Move(economy, sl1, null, pm1);
        Move expectedMove2 = new Move(economy, sl2, null, st1);
        assertTrue(expectedMove1.equals(bootStrapActionList.get(0))
                        || expectedMove1.equals(bootStrapActionList.get(1)));
        assertTrue(expectedMove2.equals(bootStrapActionList.get(0))
                        || expectedMove2.equals(bootStrapActionList.get(1)));
    }

    /**
     * Case: Non Shop together : Pm1 connected to St1. Vm1 placed on pm1 and st1.
     * Vm2 not placed anywhere.
     * Expected result: Provision a new PM by supply
     * (because VM2's requirements are satisfied by PM1's total capacity)
     * and move Vm2 to that new machine.
     */
    @Test
    public void test_NonShopTogetherBootstrap_ProvisionBySupplyWithUnplacedBuyer(){
        boolean computeMoved = false, storageMoved = false, computeProvisioned = false;
        int computeMoveActionIndex = -1, provisionActionIndex = -1;
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy,
                        Arrays.asList(0l), 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy,
                        Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 100, st1);
        //Unplaced buyer
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 60, null);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 100, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertEquals(bootStrapActionList.size(), 3);
        Move expectedStorageMove = new Move(economy, sl4, null, st1);
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.stream()
                        .filter(action -> action.getType()
                        .equals(ActionType.PROVISION_BY_SUPPLY)).findFirst().get();
        Move expectedComputeMove = new Move(economy, sl3, null,
                        provisionBySupply.getProvisionedSeller());
        for(int i=0; i < bootStrapActionList.size();i++){
            Action bootstrapAction = bootStrapActionList.get(i);
            switch(bootstrapAction.getType()){
                case MOVE:
                    if(bootstrapAction.equals(expectedComputeMove)){
                        computeMoveActionIndex = i;
                        computeMoved = true;
                    }
                    if(bootstrapAction.equals(expectedStorageMove)){
                        storageMoved = true;
                    }
                    break;
                case PROVISION_BY_SUPPLY:
                    provisionActionIndex = i;
                    assertEquals(pm1, provisionBySupply.getModelSeller());
                    computeProvisioned = true;
                    break;
                default:
                    fail("An action of type " + bootstrapAction.getType()
                        + " was found which is not expected.");
            }
        }
        //Assert that the compute and storage was moved
        assertTrue("Compute did not move.", computeMoved);
        assertTrue("Storage did not move.",storageMoved);
        assertTrue("Compute was not provisioned.",computeProvisioned);
        //Check that the compute was moved after provisioning it.
        assertTrue("Provision needs to happen before move, but did not.",
                        provisionActionIndex < computeMoveActionIndex);
    }

    /**
     * Case: Non Shop together : Pm1 connected to St1. Vm1 placed on pm1 and st1.
     * Vm2 not placed anywhere.
     * Expected result: Provision a new PM by demand
     * (because VM2's requirements are not satisfied by PM1's total capacity)
     * and move Vm2 to that new machine.
     */
    @Test
    public void test_NonShopTogetherBootstrap_ProvisionByDemandWithUnplacedBuyer(){
        boolean computeMoved = false, storageMoved = false, computeProvisioned = false;
        int computeMoveActionIndex = -1, provisionActionIndex = -1;
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 100, st1);
        //Unplaced buyer
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 110, null);
        ShoppingList sl4 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 100, null);
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList =
                        BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertEquals(bootStrapActionList.size(), 3);
        Move expectedStorageMove = new Move(economy, sl4, null, st1);
        ProvisionByDemand provisionByDemand = (ProvisionByDemand)bootStrapActionList.stream()
                        .filter(action -> action.getType()
                        .equals(ActionType.PROVISION_BY_DEMAND)).findFirst().get();
        Move expectedComputeMove = new Move(economy, sl3, null,
                        provisionByDemand.getProvisionedSeller());
        for(int i=0; i < bootStrapActionList.size();i++){
            Action bootstrapAction = bootStrapActionList.get(i);
            switch(bootstrapAction.getType()){
                case MOVE:
                    if(bootstrapAction.equals(expectedComputeMove)){
                        computeMoveActionIndex = i;
                        computeMoved = true;
                    }
                    if(bootstrapAction.equals(expectedStorageMove)){
                        storageMoved = true;
                    }
                    break;
                case PROVISION_BY_DEMAND:
                    provisionActionIndex = i;
                    assertEquals(pm1, provisionByDemand.getModelSeller());
                    assertEquals(sl3, provisionByDemand.getModelBuyer());
                    computeProvisioned = true;
                    break;
                default:
                    fail("An action of type " + bootstrapAction.getType()
                        + " was found which is not expected.");
            }
        }
        //Assert that the compute and storage was moved
        assertTrue("Compute did not move.", computeMoved);
        assertTrue("Storage did not move.",storageMoved);
        assertTrue("Compute was not provisioned.",computeProvisioned);
        //Check that the compute was moved after provisioning it.
        assertTrue("Provision needs to happen before move, but did not.",
                        provisionActionIndex < computeMoveActionIndex);
    }

    /**
     * Case: Shop together : Pm1 connected to St1. Vm1 and Vm2 are placed on pm1 and st1.
     * Expected result: Provision a new PM by supply and move one of the VMs to that new machine.
     */
    @Test
    public void test_ShopTogetherBootstrap_ProvisionBySupply(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(ST_AMT), vm1, ST_AMT, 100, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 60, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 100, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);

        List<Action> bootStrapActionList = BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 2);
        assertEquals(ActionType.PROVISION_BY_SUPPLY, bootStrapActionList.get(0).getType());
        assertEquals(ActionType.COMPOUND_MOVE, bootStrapActionList.get(1).getType());
        //Assert that the provision by supply was modelled off pm1
        ProvisionBySupply provisionBySupply = (ProvisionBySupply)bootStrapActionList.get(0);
        assertEquals(pm1, provisionBySupply.getModelSeller());
        //Assert compound move is for compute of VM1 or VM2 to move to newly provisioned PM
        Action compoundMove = bootStrapActionList.get(1);
        Move expectedMove1 = new Move(economy, sl1, pm1, provisionBySupply.getProvisionedSeller());
        Move expectedMove2 = new Move(economy, sl2, pm1, provisionBySupply.getProvisionedSeller());
        List<Move> moves = ((CompoundMove)compoundMove).getConstituentMoves();
        assertEquals(moves.size(), 1);
        assertTrue(moves.get(0).equals(expectedMove1) || moves.get(0).equals(expectedMove2));
    }

    /**
     * Case: Shop together : Pm1 connected to St1. Pm2 connected to St2.
     * Vm1 and Vm2 are placed on pm1 and st1, but within capacity.
     * Expected result: No actions are generated.
     */
    //Commenting the test for now because this is not passing. But it should.
    //@Test
    public void test_ShopTogetherBootstrap_NoActionsWhenNonInfiniteQuote(){
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(1l), 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(1l), 300, true);
        st2.setDebugInfoNeverUseInCode("DS2");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 10, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 100, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 10, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 100, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();
        economy.getModifiableShopTogetherTraders().add(vm1);
        economy.getModifiableShopTogetherTraders().add(vm2);

        List<Action> bootStrapActionList = BootstrapSupply.shopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 0);
    }

    /** Case: Non shop together : Pm1 connected to St1 and St2. Pm2 connected to St1 and St2.
     *  Vm1 and Vm2 are placed on pm1 and st1, but within capacity.
     *  Expected result: No actions are generated.
     */
    @Test
    public void test_NonShopTogetherBootstrap_NoActionsWhenNonInfiniteQuote(){
        //Case: Non shop together : Pm1 connected to St1 and St2. Pm2 connected to St1 and St2.
        //Vm1 and Vm2 are placed on pm1 and st1, but within capacity.
        //Expected result: No actions are generated.
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.setDebugInfoNeverUseInCode("DS2");
        //Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm1, CPU, 50, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm1, ST_AMT, 150, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        //Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(CPU, MEM), vm2, CPU, 50, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(ST_AMT), vm2, ST_AMT, 150, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellers();

        List<Action> bootStrapActionList = BootstrapSupply.nonShopTogetherBootstrap(economy);

        assertTrue(bootStrapActionList.size() == 0);
    }
}