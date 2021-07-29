package com.vmturbo.mediation.webhook;

import static com.vmturbo.mediation.webhook.WebhookProbeTest.createWorkflow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Test running actions against a http server that is running locally.
 */
public class WebhookProbeLocalServerTest {
    private static final long ACTION_CREATION_TIME = 1609367961000L;
    private static final long ACTION_UPDATE_TIME = 1609367994000L;
    private static final String ACCEPTED_BY = "administrator(3391249354768)";
    private static final long ACTION_UUID = 144151046183109L;
    private static final long VM_UUID = 23525323L;
    private static final String VM_LOCAL_NAME = "vm-27442";
    private static final String VM_DISPLAY_NAME = "turbonomic-t8c";
    private static final String TARGET_ADDRESS = "hp-dl365.corp.vmturbo.com";
    private static final String RESIZE_ACTION_DESC = "Resize up VCPU for Virtual Machine turbonomic-t8c from 2 to 3";
    private static final String MOVE_RISK_DESC = "Q4 VCPU, Mem Congestion";
    private static final int OLD_CPU_CAPACITY = 2;
    private static final int NEW_CPU_CAPACITY = 3;

    private static final ActionExecutionDTO ON_PREM_RESIZE_ACTION =
            ActionExecutionDTO.newBuilder()
                    .setActionOid(ACTION_UUID)
                    .setActionState(ActionResponseState.IN_PROGRESS)
                    .setCreateTime(ACTION_CREATION_TIME)
                    .setUpdateTime(ACTION_UPDATE_TIME)
                    .setAcceptedBy(ACCEPTED_BY)
                    .setActionType(ActionItemDTO.ActionType.RESIZE)
                    .addActionItem(ActionItemDTO.newBuilder()
                            .setActionType(ActionItemDTO.ActionType.RESIZE)
                            .setUuid(String.valueOf(ACTION_UUID))
                            .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                                    .setId("Test1")
                                    .setTurbonomicInternalId(VM_UUID)
                                    .setDisplayName(VM_DISPLAY_NAME)
                                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                                            .setNamespace("DEFAULT")
                                            .setName("LocalName")
                                            .setValue(VM_LOCAL_NAME)
                                            .build())
                                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                                            .setNamespace("DEFAULT")
                                            .setName("targetAddress")
                                            .setValue(TARGET_ADDRESS)
                                            .build())
                                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                            .setCurrentComm(CommonDTO.CommodityDTO.newBuilder()
                                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                                    .setCapacity(OLD_CPU_CAPACITY)
                                    .build())
                            .setNewComm(CommonDTO.CommodityDTO.newBuilder()
                                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                                    .setCapacity(NEW_CPU_CAPACITY)
                                    .build())
                            .setDescription(RESIZE_ACTION_DESC)
                            .setRisk(ActionItemDTO.Risk.newBuilder()
                                    .setSeverity(ActionItemDTO.Risk.Severity.CRITICAL)
                                    .setCategory(ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE)
                                    .setDescription(MOVE_RISK_DESC)
                                    .addAffectedCommodity(CommonDTO.CommodityDTO.CommodityType.VCPU)
                            )
                    )
                    .build();


    private static CachingHttpHandler handler;
    private static HttpServer server;
    private static ThreadPoolExecutor executor;
    private static WebhookProbe probe;
    private static IProgressTracker progressTracker;

    private static final Logger logger = LogManager.getLogger();

    private final WebhookAccount account = new WebhookAccount();

    /**
     * Setup an http server for test.
     *
     * @throws IOException if port cannot get claimed
     */
    @BeforeClass
    public static void setupServer() throws IOException {
        handler = new CachingHttpHandler();
        server = HttpServer.create(new InetSocketAddress("localhost", 28121), 0);
        executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);
        server.createContext("/", handler);
        server.setExecutor(executor);
        server.start();
        logger.info("Server started on port 28121");

        // set up probe related stuff
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        progressTracker = mock(IProgressTracker.class);
    }

    /**
     * Shuts down the server and threadpool.
     */
    @AfterClass
    public static void shutdownServer() {
        logger.info(" Shutting down server started on port 28121");
        server.stop(0);
        executor.shutdownNow();
    }

    /**
     * Reset handler before each request.
     */
    @Before
    public void resetHandler() {
        handler.resetHandler();
    }

    /**
     * Tests the case that workflow is a simple get call without body.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulGetMethod() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121/get/method", null, "GET",
                        null, null, null))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null);
    }

    /**
     * Tests the case that workflow is get call with  a templated body without body that results in failure.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testFailedUrlTemplatedGetMethod() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121/get/$action.uuid", null, "GET",
                        null, null, null))
                .build();

        handler.setResponseCode(404);

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.FAILED, "GET", "/get/" + ACTION_UUID, null);
    }

    /**
     * Tests a post call with a fixed message to an endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPostRequestWithBody() throws InterruptedException {
        // ARRANGE
        final String address = "/postEndpoint";
        final String payload = "{\"message\": \"sample message\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121" + address, payload, "POST",
                        null, null, null))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "POST", address, payload);
    }

    /**
     * Tests a post call with a templated message to an endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPostRequestWithATemplatedBody() throws InterruptedException {
        // ARRANGE
        final String address = "/";
        final String payload = "{\"id\": \"$action.uuid\", \"type\": \"$action.actionType\", \"commodity\":"
                + " \"$action.risk.reasonCommodities.toArray()[0]\", \"to\": \"$action.newValue\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121" + address, payload, "POST",
                        null, null, null))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "POST", address,
                "{\"id\": \"144151046183109\", \"type\": \"RESIZE\", \"commodity\": \"VCPU\", \"to\": \"3.0\"}");
    }

    /**
     * Tests a put call with a templated message to an templated url endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPutRequestWithATemplatedBody() throws InterruptedException {
        // ARRANGE
        final String address = "/actions/$action.uuid";
        final String payload = "{\"description\": \"$action.details\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121" + address, payload, "PUT",
                        null, null, null))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "PUT", "/actions/" + ACTION_UUID,
                "{\"description\": \"Resize up VCPU for Virtual Machine turbonomic-t8c from 2 to 3\"}");
    }

    /**
     * Tests the case that workflow is a get call with basic auth.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testBasicAuth() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121/get/method", null, "GET",
                        AuthenticationMethod.BASIC, "testUser", "testPass"))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null);

        // make sure request has basic auth header
        assertThat(handler.getLastRequest().get().headers.get("Authorization"),
                equalTo(Collections.singletonList("Basic dGVzdFVzZXI6dGVzdFBhc3M=")));
    }

    private void verifyResults(ActionResult result, ActionResponseState expectedState, String expectedMethod,
                               String expectedAddress, String expectedPayload) {
        assertThat(result.getState(), is(expectedState));
        Optional<Request> request = handler.getLastRequest();
        assertTrue(request.isPresent());
        assertThat(request.get().method, is(expectedMethod));
        assertThat(request.get().url, is(expectedAddress));
        assertThat(request.get().payload, is(expectedPayload));
    }

    /**
     * A response handler that caches the last request made to it.
     */
    private static class CachingHttpHandler implements HttpHandler {
        private Request lastRequest = null;
        private int responseCode = 200;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String body;
            if (exchange.getRequestBody().available() != 0) {
                body = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8.name());
            } else {
                body = null;
            }

            lastRequest = new Request(exchange.getRequestMethod(), exchange.getRequestURI().toString(), body,
                    exchange.getRequestHeaders());

            exchange.sendResponseHeaders(responseCode, 0);
            exchange.getResponseBody().close();
        }

        void setResponseCode(int responseCode) {
            this.responseCode = responseCode;
        }

        Optional<Request> getLastRequest() {
            return Optional.ofNullable(lastRequest);
        }

        void resetHandler() {
            lastRequest = null;
            responseCode = 200;
        }
    }

    /**
     * Keeps information about an http request.
     */
    private static class Request {
        String method;
        String url;
        String payload;
        Map<String, List<String>> headers;

        Request(String method, String url, String payload, Map<String, List<String>> headers) {
            this.method = method;
            this.url = url;
            this.payload = payload;
            this.headers = headers;
        }
    }
}
