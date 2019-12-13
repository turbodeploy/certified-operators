package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * Test the functionality of {@link Provision#ProvisionDecisions}.
 *
 */
@RunWith(JUnitParamsRunner.class)
public class ProvisionTest {

    // Methods

    /**
     * Helper method to assert the number of all actions
     *
     * @param actions the list of actions generated in each test
     * @param the number of all actions that should be generated in each test
     */
    private void assertAllCount(List<Action> actions, int value) {
        assertTrue(actions.size() == value);
    }

    /**
     * Helper method to assert the number of PROVISION_BY_SUPPLY actions
     *
     * @param actions the list of actions generated in each test
     * @param the number of PROVISION_BY_SUPPLY actions that should be generated in each test
     */
    private void assertProvisionBySupplyCount(List<Action> actions, int value) {
        assertTrue(actions.stream().
            filter(action -> action.getType() == ActionType.PROVISION_BY_SUPPLY).count() == value);
    }

    /**
     * Helper method to assert the number of ACTIVATE actions
     *
     * @param actions the list of actions generated in each test
     * @param the number of ACTIVATE actions that should be generated in each test
     */
    private void assertActivateCount(List<Action> actions, int value) {
        assertTrue(actions.stream().
            filter(action -> action.getType() == ActionType.ACTIVATE).count() == value);
    }

    /**
     * Helper method to assert the number of MOVE actions
     *
     * @param actions the list of actions generated in each test
     * @param the number of MOVE actions that should be generated in each test
     */
    private void assertMoveCount(List<Action> actions, int value) {
        assertTrue(actions.stream().
            filter(action -> action.getType() == ActionType.MOVE).count() == value);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: ProvisionTest({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11})")
    public final void testProvisionDecisions(int pm1Cpu, int pm1Mem, int st1Sto, int vm1Cpu, int vm1Mem, int vm1Sto,
                    int vm2Cpu, int vm2Mem, int vm2Sto, int allActions, int provisionBySupplyActions, int moveActions, int baseTypeOfReasonCommodity) {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), pm1Cpu, pm1Mem, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), st1Sto, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{vm1Cpu, vm1Mem}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{vm1Sto}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{vm2Cpu, vm2Mem}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{vm2Sto}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, allActions);
        if (baseTypeOfReasonCommodity == -1) {
            assertProvisionBySupplyCount(actions, provisionBySupplyActions);
        } else {
            // Match reason commodity, based on utilization values, that led to provision
            ProvisionBySupply provisionBySupplyAction =
                            (ProvisionBySupply)actions.stream().findFirst()
                                            .filter(action -> action
                                                            .getType() == ActionType.PROVISION_BY_SUPPLY)
                                            .get();
            assertTrue(provisionBySupplyAction.getReason()
                            .getBaseType() == baseTypeOfReasonCommodity);
        }
        assertMoveCount(actions, moveActions);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestProvisionDecisions() {
        return new Object[][] {
           {100, 100, 300, 30, 30, 100, 30, 30, 100, 0, 0, 0, -1},
           {100, 100, 300, 46, 46, 140, 46, 46, 140, 4, 2, 2, -1},
           {100, 100, 300, 46, 46, 100, 46, 46, 100, 2, 1, 1, TestUtils.CPU.getBaseType()},
           {100, 100, 300, 49, 44, 140, 49, 44, 140, 4, 2, 2, -1},
           {100, 100, 300, 46, 30, 140, 46, 30, 140, 2, 1, 1, TestUtils.ST_AMT.getBaseType()},
           {100, 100, 300, 49, 44, 100, 49, 44, 100, 2, 1, 1, TestUtils.CPU.getBaseType()},
           {100, 100, 300, 44, 49, 100, 44, 49, 100, 2, 1, 1, TestUtils.MEM.getBaseType()},
           {100, 100, 300, 46, 30, 100, 46, 30, 100, 0, 0, 0, -1},
           {100, 100, 300, 30, 30, 140, 30, 30, 140, 2, 1, 1, TestUtils.ST_AMT.getBaseType()},
           {100, 100, 300, 30, 30, 140, 30, 30, 140, 2, 1, 1, TestUtils.ST_AMT.getBaseType()},
           {100, 100, 300, 30, 50, 100, 30, 50, 100, 2, 1, 1, TestUtils.MEM.getBaseType()}
        };
     }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * All resources have utilization above the max desired utilization, this would justify cloning.
     * All buyers are unmovable.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_UnmovableBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{145}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{145}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        for (Market market : economy.getMarkets()) {
            for (ShoppingList sl : market.getBuyers()) {
                sl.setMovable(false);
            }
        }

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }

    /**
     * Setup economy with 2 PM selling CPU and memory, 2 DS selling storage
     * and 2 VMs buying from the same PM and the same DS.
     * All resources have utilization above the max desired utilization, this would justify cloning.
     * There is an empty PM and an empty DS.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_EmptyPMAndDSExist() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{145}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{145}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 2);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * All resources have utilization above the max desired utilization, this would justify cloning.
     * There is a lot of overhead in PM and DS.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_Overhead() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.getCommoditiesSold().get(0).setQuantity(95.0);
        pm1.getCommoditiesSold().get(1).setQuantity(95.0);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.getCommoditiesSold().get(0).setQuantity(290.0);
        st1.setDebugInfoNeverUseInCode("DS1");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * All resources have utilization above the max desired utilization, this would justify cloning.
     * Economy forced to stop.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_EcomonyForcedToStop() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        pm1.getSettings().setMaxDesiredUtil(0.9);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{145}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{48, 48}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{145}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        economy.setForceStop(true);

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * All the sellers are not clonable.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_AllSellersNotClonable() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.getSettings().setCloneable(false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.getSettings().setCloneable(false);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * All the buyers are guaranteed.
     * Expected result: 0 PROVISION_BY_SUPPLY actions.
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_AllBuyersGuaranteed() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.getSettings().setCloneable(false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.getSettings().setCloneable(false);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.getSettings().setGuaranteedBuyer(true);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.getSettings().setGuaranteedBuyer(true);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM.
     * Expected result: 1 ACTIVATE action (for PM) and 1 PROVISION_BY_SUPPLY action (for DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMInactive() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertActivateCount(actions, 1);
        assertProvisionBySupplyCount(actions, 1);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive DS.
     * Expected result: 1 ACTIVATE action (for DS) and 1 PROVISION_BY_SUPPLY action (for PM).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_DSInactive() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertActivateCount(actions, 1);
        assertProvisionBySupplyCount(actions, 1);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM and 1 inactive DS.
     * Expected result: 2 ACTIVATE actions (for PM and DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMAndDSInactive() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertActivateCount(actions, 2);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM with not enough CPU and memory.
     * Expected result: 2 PROVISION_BY_SUPPLY actions (for PM and DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMInactiveNotEnoughCPUMemory() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 30, 30, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 2);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive DS with not enough storage.
     * Expected result: 2 PROVISION_BY_SUPPLY actions (for PM and DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_DSInactiveNotEnoughStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 100, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 2);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM with not enough CPU and memory and 1 inactive DS with not enough storage.
     * Expected result: 2 ACTIVATE actions (for PM and DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMAndDSInactiveNotEnoughCPUMemoryStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 30, 30, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 100, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 2);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM with not enough CPU and memory and 1 inactive DS.
     * Expected result: 1 PROVISION_BU_SUPPLY action (for PM) and 1 ACTIVATE action (for DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMAndDSInactiveNotEnoughCPUMemory() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 30, 30, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 1);
        assertActivateCount(actions, 1);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying from PM and DS.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There is 1 inactive PM and 1 inactive DS with not enough storage.
     * Expected result: 1 PROVISION_BY_SUPPLY action (for DS) and 1 ACTIVATE action (for PM).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_PMAndDSInactiveNotEnoughStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.changeState(TraderState.INACTIVE);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader st2 = TestUtils.createStorage(economy, Arrays.asList(0l), 100, true);
        st2.getSettings().setMaxDesiredUtil(0.9);
        st2.changeState(TraderState.INACTIVE);
        st2.setDebugInfoNeverUseInCode("DS2");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        // Place vm2 on pm1 and st1.
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{140}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 1);
        assertActivateCount(actions, 1);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 2 PMs selling CPU and memory
     * and 2 VMs buying from PMs.
     * CPU, memory and storage utilization higher than max desired utilization.
     * There are 2 inactive PMs from which 1 does not have enough CPU and memory
     * Expected result: 1 PROVISION_BY_SUPPLY and 1 ACTIVATE action (for the 2 PMs).
     */
    @Test
    public void testProvisionDecisions_TwoHighUtilCPUMemory_TwoPMInactive1NotEnoughCPUMemory() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.getSettings().setMaxDesiredUtil(0.9);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader pm3 = TestUtils.createPM(economy, Arrays.asList(0l), 30, 30, true);
        pm3.getSettings().setMaxDesiredUtil(0.9);
        pm3.changeState(TraderState.INACTIVE);
        pm3.setDebugInfoNeverUseInCode("PM3");
        Trader pm4 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm4.getSettings().setMaxDesiredUtil(0.9);
        pm4.changeState(TraderState.INACTIVE);
        pm4.setDebugInfoNeverUseInCode("PM4");
        // Place vm1 and vm2 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{46, 46}, pm1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{46, 46}, pm1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        // Place vm3 and vm4 on pm2.
        Trader vm3 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm3, new double[]{46, 46}, pm2);
        vm3.setDebugInfoNeverUseInCode("VM1");
        Trader vm4 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm4, new double[]{46, 46}, pm2);
        vm2.setDebugInfoNeverUseInCode("VM2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 4);
        assertProvisionBySupplyCount(actions, 1);
        assertActivateCount(actions, 1);
        assertMoveCount(actions, 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VMs buying from PM and DS.
     * All resources have utilization above the max desired utilization, this would justify cloning.
     * There is only 1 buyer.
     * Expected result: 0 PROVISION_BY_SUPPLY actions (for PM and DS).
     */
    @Test
    public void testProvisionDecisions_HighUtilCPUMemoryStorageAmount_OneBuyer() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.getSettings().setMaxDesiredUtil(0.9);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        st1.getSettings().setMaxDesiredUtil(0.9);
        st1.setDebugInfoNeverUseInCode("DS1");
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{95, 95}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{290}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        List<Action> actions = Provision.provisionDecisions(economy, new Ledger(economy),
                                                                        new Ede());

        assertAllCount(actions, 0);
    }
}