package com.vmturbo.systest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;

/**
 * System-level test for plan-over-plan.
 **/

public class PlanOverPlanSysTest extends SystemTestBase {

    private static final Logger logger = LogManager.getLogger();
    private static final String LIVE_MARKET_UUID = "Market";

    // JSON for scenario to just rebalance, i.e. a "CUSTOM" scenario with no changes
    private final static String REBALANCE_SCENARIO_JSON = "/rebalance-scenario.json";
    private ScenarioApiDTO rebalanceScenarioInputDTO;

    // JSON for scenario to add 5 copies of a given VM
    private final static String ADD_LOAD_SCENARIO_JSON = "/add-load-scenario.json";
    private ScenarioApiDTO addLoadScenarioInputDTO;

    @Before
    public void setup() {
        rebalanceScenarioInputDTO = (new Gson()).fromJson(new InputStreamReader(
                this.getClass().getResourceAsStream(REBALANCE_SCENARIO_JSON)),
                ScenarioApiDTO.class);

        addLoadScenarioInputDTO = (new Gson()).fromJson(new InputStreamReader(
                this.getClass().getResourceAsStream(ADD_LOAD_SCENARIO_JSON)),
                ScenarioApiDTO.class);

    }

    /**
     * Run a plan-over-plan. The first scenario is simply a rebalance - no additions or removals.
     * The second scenario adds load.
     */
    @Test
    public void testPlanOverPlanExecution() throws Exception {

        // Arrange

        // create target, discover, run a plan
        String targetId = createStressProbeTarget(CREATE_PROBE_TIMEOUT);
        waitForDiscovery(targetId, DISCOVERY_TIMEOUT);
        int originalentityCount = publishTopology();
        logger.info("original entity count: {}", originalentityCount);

        String rebalanceScenarioId = createScenario("rebalance", rebalanceScenarioInputDTO);
        logger.info("rebalance scenarioId: {}", rebalanceScenarioId);
        String planId = runPlan(LIVE_MARKET_UUID, rebalanceScenarioId);

        logger.info("rebalance planId: {}<", planId);
        waitForPlanToFinish(planId, PLAN_EXECUTION_TIMEOUT);

        MarketApiDTO step1Market = getMarket(planId);

        // Act

        // add a listener for plan progress notifications
        final PlanNotificationListener listener = new PlanNotificationListener();
        addApiWebsocketListener(listener);

        // run a plan beginning from the output planId
        String addLoadScenarioId = createScenario("addLoad", addLoadScenarioInputDTO);
        logger.info("addLoad scenarioId: {}", addLoadScenarioId);

        String secondPlanId = runPlan(planId, addLoadScenarioId);
        logger.info("second planId: {}", secondPlanId);

        waitForPlanToFinish(secondPlanId, PLAN_EXECUTION_TIMEOUT);
        logger.info("plan-over-plan complete");

        // wait progress notifications to reach 100%; may have already happened
        waitForPlanProgress(listener, PLAN_EXECUTION_TIMEOUT);

        // Assert

        // check that the two steps are run using the same plan ID
        MarketApiDTO step2Market = getMarket(secondPlanId);
        assertThat(step1Market.getUuid(), equalTo(planId));
        assertThat(step2Market.getUuid(), equalTo(planId));

        logger.info("before: {}", (new Gson()).toJson(step1Market));
        logger.info("after: {}", (new Gson()).toJson(step2Market));

        // check the number of VMs
        // todo

    }

}
