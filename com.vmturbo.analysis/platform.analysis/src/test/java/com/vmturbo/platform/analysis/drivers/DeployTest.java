package com.vmturbo.platform.analysis.drivers;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

public class DeployTest {

    /**
     * Set up AnalysisServer
     *
     * @return AnalysisServer
     */
    private AnalysisServer setUpAnalysisServer() {
        ThreadFactory factory =
                        new ThreadFactoryBuilder().setNameFormat("analysis-client-%d").build();
        ExecutorService threadPool = Executors.newFixedThreadPool(1, factory);
        AnalysisServer analysisServer = new AnalysisServer(threadPool);
        return analysisServer;
    }

    /**
     * Set up AnalysisInstanceInfo
     *
     * @return AnalysisInstanceInfo
     */
    private AnalysisInstanceInfo setUpAnalysisInstanceInfo() {
        AnalysisInstanceInfo analysisInstanceInfo = new AnalysisInstanceInfo();
        analysisInstanceInfo.setRealTime(false);
        analysisInstanceInfo.setProvisionEnabled(false);
        analysisInstanceInfo.setSuspensionEnabled(false);
        analysisInstanceInfo.setResizeEnabled(false);
        analysisInstanceInfo.setBalanceDeploy(true);
        analysisInstanceInfo.setMarketData("Deploy");
        analysisInstanceInfo.setMarketName("Deploy");
        return analysisInstanceInfo;
    }

    /**
     * Set up Topology
     *
     * @return Topology
     */
    private Topology setUpTopology() {
        Topology topology = new Topology();
        topology.setTopologyId(123);
        return topology;
    }

    /**
     * Add shopping lists with oids to topology map
     *
     * @param topology
     * @param sls
     */
    private void addSLsToTopology(Topology topology, List<ShoppingList> sls) {
        BiMap<ShoppingList, Long> slOidMap = topology.getModifiableShoppingListOids();
        for (int i = 0; i < sls.size(); i++) {
            slOidMap.put(sls.get(i), (long)i);
        }
    }

    /**
     * Add traders with oids to topology map
     *
     * @param topology
     * @param traders
     */
    private void addTradersToTopology(Topology topology, List<Trader> traders) {
        BiMap<Trader, Long> traderOidMap = topology.getModifiableTraderOids();
        for (int i = 0; i < traders.size(); i++) {
            traderOidMap.put(traders.get(i), (long)i);
        }
    }

    /**
     * Case: Balance deploy but there is no space for the added vm, even though there is originally
     * a free host. The deploy VM does not find placement as an existing vm from pm1 moves to pm2.
     * Expected result: The deployed VM is unplaced.
     */
    @Test
    public void testDeployBalanceNoSpace() {
        AnalysisServer analysisServer = setUpAnalysisServer();
        AnalysisInstanceInfo analysisInstanceInfo = setUpAnalysisInstanceInfo();
        Topology topology = setUpTopology();
        Economy economy = topology.getEconomyForTesting();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 1000, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList slPM1VM1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{70, 0}, pm1);
        ShoppingList slST1VM1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList slPM2VM2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{70, 0}, pm1);
        ShoppingList slST1VM2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        Trader deployVM = TestUtils.createVM(economy);
        ShoppingList slPMDeployVM = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), deployVM, new double[]{70, 0}, null);
        ShoppingList slSTDeployVM = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), deployVM, new double[]{100}, null);
        economy.getPlacementEntities().add(deployVM);
        deployVM.setDebugInfoNeverUseInCode("deployVM");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        economy.getModifiablePreferentialSls().addAll(Stream.of(slPM1VM1, slST1VM1, slPM2VM2, slST1VM2)
                                                            .collect(Collectors.toList()));
        deployVM.getSettings().setIsShopTogether(true);
        economy.getModifiableShopTogetherTraders().add(deployVM);
        addSLsToTopology(topology, Stream.of(slPM1VM1, slST1VM1, slPM2VM2, slST1VM2, slPMDeployVM,
                                                slSTDeployVM).collect(Collectors.toList()));
        addTradersToTopology(topology, Stream.of(pm1, st1, pm2, vm1, vm2,deployVM)
                                                            .collect(Collectors.toList()));
        AnalysisResults analysisResults = analysisServer.generateAnalysisResult(analysisInstanceInfo, topology);
        List<ActionTO> actionsList = analysisResults.getActionsList();
        assertTrue(actionsList.size() == 1);
        MoveTO move = actionsList.get(0).getMove();
        ShoppingList resultSL = topology.getShoppingListOids().inverse().get(move.getShoppingListToMove());
        assertTrue(!resultSL.equals(slPMDeployVM));
        assertTrue(!resultSL.equals(slSTDeployVM));
        assertTrue(!deployVM.equals(resultSL.getBuyer()));
        assertTrue(slPMDeployVM.getSupplier() == null);
        assertTrue(slSTDeployVM.getSupplier() == null);
        analysisServer.close();
    }

    /**
     * Case: Balance deploy and there is enough space for the added vm.
     * Expected result: The deployed VM is placed.
     */
    @Test
    public void testDeployBalanceWithSpace() {
        AnalysisServer analysisServer = setUpAnalysisServer();
        AnalysisInstanceInfo analysisInstanceInfo = setUpAnalysisInstanceInfo();
        Topology topology = setUpTopology();
        Economy economy = topology.getEconomyForTesting();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 1000, true);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader pm2 = TestUtils.createPM(economy, Arrays.asList(0l), 200, 100, true);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList slPM1VM1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{70, 0}, pm1);
        ShoppingList slST1VM1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList slPM2VM2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{70, 0}, pm1);
        ShoppingList slST1VM2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        vm2.setDebugInfoNeverUseInCode("VM2");
        Trader deployVM = TestUtils.createVM(economy);
        ShoppingList slPMDeployVM = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), deployVM, new double[]{70, 0}, null);
        ShoppingList slSTDeployVM = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), deployVM, new double[]{100}, null);
        economy.getPlacementEntities().add(deployVM);
        deployVM.setDebugInfoNeverUseInCode("deployVM");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        economy.getModifiablePreferentialSls().addAll(Stream.of(slPM1VM1, slST1VM1, slPM2VM2, slST1VM2)
                                                            .collect(Collectors.toList()));
        deployVM.getSettings().setIsShopTogether(true);
        economy.getModifiableShopTogetherTraders().add(deployVM);
        addSLsToTopology(topology, Stream.of(slPM1VM1, slST1VM1, slPM2VM2, slST1VM2, slPMDeployVM,
                                                slSTDeployVM).collect(Collectors.toList()));
        addTradersToTopology(topology, Stream.of(pm1, st1, pm2, vm1, vm2,deployVM)
                                                            .collect(Collectors.toList()));
        AnalysisResults analysisResults = analysisServer.generateAnalysisResult(analysisInstanceInfo, topology);
        List<ActionTO> actionsList = analysisResults.getActionsList();
        assertTrue(actionsList.size() == 1);
        CompoundMoveTO compoundMove = actionsList.get(0).getCompoundMove();
        MoveTO move1 = compoundMove.getMoves(0);
        MoveTO move2 = compoundMove.getMoves(1);
        ShoppingList resultSL1 = topology.getShoppingListOids().inverse().get(move1.getShoppingListToMove());
        ShoppingList resultSL2 = topology.getShoppingListOids().inverse().get(move2.getShoppingListToMove());
        assertTrue(resultSL1.equals(slPMDeployVM) || resultSL1.equals(slSTDeployVM));
        assertTrue(resultSL2.equals(slPMDeployVM) || resultSL2.equals(slSTDeployVM));
        assertTrue(deployVM.equals(resultSL1.getBuyer()));
        assertTrue(deployVM.equals(resultSL2.getBuyer()));
        assertTrue(slPMDeployVM.getSupplier().equals(pm2));
        assertTrue(slSTDeployVM.getSupplier().equals(st1));
        analysisServer.close();
    }
}