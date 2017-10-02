package com.vmturbo.systest;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.WebSocketContainer;
import javax.xml.ws.http.HTTPException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.api.dto.MarketApiDTO;
import com.vmturbo.api.dto.ScenarioApiDTO;
import com.vmturbo.api.dto.TargetApiDTO;
import com.vmturbo.api.dto.input.ScenarioApiInputDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.test.utilities.utils.StressProbeUtils;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Abstract class for System Tests to inherit. Provides convenience methods to wrapper
 * commonly used External REST API calls.
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SystemTestConfig.class,
        loader = AnnotationConfigContextLoader.class)
public class SystemTestBase {

    @Autowired
    private SystemTestConfig systemTestConfig;

    // time to delay during a retry loop waiting for long-running operations
    private static final int RETRY_DELAY_MS = 5000;

    // time to wait for probe to be registered
    public static final Duration CREATE_PROBE_TIMEOUT = Duration.ofSeconds(10);

    // discovery for tiny topology using stressprobe should be fast
    public static final Duration DISCOVERY_TIMEOUT = Duration.ofSeconds(30);

    // plan execution expected to be much less than 5 minutes
    public static final Duration PLAN_EXECUTION_TIMEOUT = Duration.ofMinutes(5);

    // number of SEs in the sample topology
    private static final int TOPOLOGY_SIZE = 500;

    // Maximum percentage value for watching plan progress notifications.
    private static final int PLAN_PROGRESS_MAX_PCT = 100;

    private static final Logger logger = LogManager.getLogger();

    public String createStressProbeTarget(Duration timeout) throws
            InterruptedException {

        String newTargetUri = "/targets";

        TargetApiDTO targetApiRequest = StressProbeUtils.createStressprobeTargetRequest(TOPOLOGY_SIZE);
        Duration elapsed = Duration.ZERO;
        TargetApiDTO targetApiDTO = null;
        while (elapsed.compareTo(timeout) < 0) {
            try {
                targetApiDTO = systemTestConfig.externalApiRequestor()
                        .externalApiPostRequest(newTargetUri, TargetApiDTO.class, targetApiRequest);
            } catch (HTTPException e) {
                logger.info("Attempt to create stressprobe failed ({}) continuing to wait",
                        e.getStatusCode());
                e.printStackTrace();
            }
            try {
                Thread.sleep(RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interruption waiting for discovery response");
            }
            elapsed = elapsed.plus(Duration.ofMillis(RETRY_DELAY_MS));
        }
        if (targetApiDTO == null) {
            throw new RuntimeException("Null response adding target");
        }
        return targetApiDTO.getUuid();
    }

    protected void waitForDiscovery(@Nonnull String targetId, @Nonnull final Duration timeout) {
        String newTargetUri = "/targets/" + targetId;

        Duration elapsed = Duration.ZERO;
        while (elapsed.compareTo(timeout) < 0) {
            TargetApiDTO targetInfo = getRequest(newTargetUri, TargetApiDTO.class);
            if (targetInfo.getStatus().equals("Validated")) {
                return;
            }
            try {
                Thread.sleep(RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interruption waiting for discovery response");
            }
            elapsed = elapsed.plus(Duration.ofMillis(RETRY_DELAY_MS));
        }
        throw new RuntimeException("discovery did not complete after " + timeout);
    }

    /**
     * Request a broadcast and wait for it to complete. Calls the TopologyProcessor API since
     * the External API does not include this functionality.
     *
     * @return the number of entities published in the topology.
     */
    protected int publishTopology() throws Exception {

        final CompletableFuture<Integer> entitiesFuture = new CompletableFuture<>();
        systemTestConfig.topologyProcessor().addEntitiesListener(
                new TestEntitiesListener(entitiesFuture));
        systemTestConfig.topologyService().requestTopologyBroadcast(
                TopologyBroadcastRequest.getDefaultInstance());
        int numEntitiesReceived = entitiesFuture.get(10, TimeUnit.MINUTES);
        logger.info("Received {} entities", numEntitiesReceived);
        return numEntitiesReceived;
    }

    private static class TestEntitiesListener implements EntitiesListener {

        private final CompletableFuture<Integer> entitiesFuture;

        private TestEntitiesListener(@Nonnull final CompletableFuture<Integer> entitiesFuture) {
            this.entitiesFuture = entitiesFuture;
        }

        @Override
        public void onTopologyNotification(TopologyInfo topologyInfo,
                                           @Nonnull RemoteIterator<TopologyDTO.TopologyEntityDTO> topologyDTOs) {
            int entityCount = 0;
            while (topologyDTOs.hasNext()) {
                try {
                    entityCount += topologyDTOs.nextChunk().size();
                } catch (Exception e) {
                    logger.error("Error during topology broadcast reading: ", e);
                    entitiesFuture.complete(-1);
                }
            }

            entitiesFuture.complete(entityCount);
        }
    }

    protected MarketApiDTO getMarket(String marketUuid) {
        return getRequest("/markets/" + marketUuid, MarketApiDTO.class);
    }

    protected String createScenario(String scenarioName, ScenarioApiInputDTO scenarioInputDTO) {

        final String newScenarioUri = "/scenarios/" + scenarioName;

        logger.info("creating scenario: {}", newScenarioUri);
        ScenarioApiDTO newScenario = postRequest(newScenarioUri,
                ScenarioApiDTO.class,
                scenarioInputDTO);
        logger.info("new scenario id: {} == {}", newScenario.getDisplayName(), newScenario.getUuid());

        return newScenario.getUuid();
    }

    protected String runPlan(String marketId, String scenarioId) {
        String runPlanUri = "/markets/" + marketId + "/scenarios/" + scenarioId;

        MarketApiDTO newMarket = postRequest(runPlanUri,
                MarketApiDTO.class, null);
        if (newMarket == null) {
            throw new RuntimeException("No response creating newMarket");
        }
        return newMarket.getUuid();
    }

    /**
     * Sleep/wait for the given plan to finish execution by fetching the plan status.
     *
     * @param planId ID of the plan to check status for
     * @param timeout {@link Duration after which the test will fail}
     * @throws InterruptedException if the sleep() is interrupted while waiting
     * @throws RuntimeException if either the market status is unexpected or the test times out
     */
    protected void waitForPlanToFinish(@Nonnull String planId, @Nonnull Duration timeout) throws InterruptedException {
        logger.info("waiting for plan to finish");
        String marketsRequestUri = "/markets/" + planId;
        Duration elapsed = Duration.ZERO;
        while (elapsed.compareTo(timeout) < 0) {
            MarketApiDTO marketInfo = getRequest(marketsRequestUri, MarketApiDTO.class);
            if (marketInfo == null) {
                throw new RuntimeException("Market info for " + planId + " is null");
            }
            // allowable values for state taken from MarketApiDTO
            switch (marketInfo.getState()) {
                case "SUCCEEDED":
                    return;
                case "CREATED":
                case "READY_TO_START":
                case "RUNNING":
                case "COPYING":
                    // continue waiting
                    break;
                case "STOPPING":
                case "STOPPED":
                case "DELETING":
                default:
                    throw new RuntimeException("Unexpected market state: " + marketInfo.getState());
            }
            try {
                Thread.sleep(RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interruption waiting for discovery response");
            }
            elapsed = elapsed.plus(Duration.ofMillis(RETRY_DELAY_MS));
        }
        throw new RuntimeException("Plan Market did not complete after " + timeout);
    }

    /**
     * Wait for plan progress to reach the desired max value (PLAN_PROGRESS_MAX_PCT = 100).
     * The listener should have previously been registered using
     * {@link #addApiWebsocketListener(Object)}.
     *
     * @param planNotificationListener the {@link PlanNotificationListener} that is currently registered
     * @param timeout {@link Duration} after which test test will fail
     * @throws RuntimeException if the plan does not complete within the given time frame.
     */
    protected void waitForPlanProgress(@Nonnull PlanNotificationListener planNotificationListener,
                                       @Nonnull Duration timeout) {
        Duration elapsed = Duration.ZERO;
        while (elapsed.compareTo(timeout) < 0) {
            if (planNotificationListener.getProgress() >= PLAN_PROGRESS_MAX_PCT) {
                return;
            }
            try {
                logger.info("sleeping waiting for plan progress: {}", planNotificationListener.getProgress());
                Thread.sleep(RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interruption waiting for discovery response");
            }
            elapsed = elapsed.plus(Duration.ofMillis(RETRY_DELAY_MS));
        }
        throw new RuntimeException("Plan progress did not reach " + PLAN_PROGRESS_MAX_PCT);
    }

    /**
     * Register a listener for async notifications from the external API.
     * Notifications are sent as protobufs via websocket.
     *
     * @param listener the @ClientEndpoint object to register for this websocket.
     * @throws IOException if we cannot connect to the remote server
     * @throws DeploymentException if there's an error deploying websockets locally
     */
    protected void addApiWebsocketListener(Object listener) throws IOException,
            DeploymentException {

        URI uri = systemTestConfig.apiWebsocketUri();
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        logger.info("Connecting to " + uri);
        container.connectToServer(listener, uri);
    }


    /**
     * Issue a REST GET request.
     *
     * See {@link ExternalApiRequestor} for error handling.
     *
     * @param requestUrl url for the GET request
     * @param responseClass class marker for the expected response
     * @param <T> class of response expected
     * @return the response from the request, which will be of type T
     */
    private <T> T getRequest(String requestUrl, Class<T> responseClass) {
        return systemTestConfig.externalApiRequestor().externalApiGetRequest(requestUrl,
                responseClass);
    }

    /**
     * Issue a REST POST request.
     *
     * See {@link ExternalApiRequestor} for error handling.
     *
     * @param requestUrl url for the POST request
     * @param responseClass class market for the expected response
     * @param requestData data to send with the request
     * @param <T> type of the response expected
     * @param <R> type of the request data to be sent
     * @return the response from the request, which willbe of type T
     */
    private <T, R> T postRequest(String requestUrl, Class<T> responseClass, R requestData) {
        return systemTestConfig.externalApiRequestor().externalApiPostRequest(requestUrl,
                responseClass, requestData);
    }

}


