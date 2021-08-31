package com.vmturbo.mediation.webhook;

import static com.vmturbo.mediation.webhook.WebhookProbeTest.createWorkflow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.api.dto.workflow.RequestHeader;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.TargetOperationException;
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
        server.createContext("/not/found", exchange -> {
            exchange.sendResponseHeaders(404, 0);
            exchange.getResponseBody().close();
        });
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
                        null, null, null, Collections.emptyList()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                Collections.emptyList());
    }

    /**
     * Test request with no headers.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithNoHeaders() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/get/method", null, "GET", null, null, null,
                        Collections.emptyList())).build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                Collections.emptyList());
    }

    /**
     * Test request with multiple headers with the same name and different values.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithMultipleHeadersWithTheSameName() throws InterruptedException {
        // ARRANGE
        final List<RequestHeader> requestHeaders = Arrays.asList(
                new RequestHeader("header_name", "header_value_1"),
                new RequestHeader("header_name", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/get/method", null, "GET", null, null, null,
                        requestHeaders)).build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                requestHeaders);
    }

    /**
     * Test request with multiple different headers.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithMultipleDifferentHeaders() throws InterruptedException {
        // ARRANGE
        final List<RequestHeader> requestHeaders = Arrays.asList(
                new RequestHeader("header_name_1", "header_value_1"),
                new RequestHeader("header_name_2", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/get/method", null, "GET", null, null, null,
                        requestHeaders)).build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                requestHeaders);
    }

    /**
     * In a HTTP request, the only mandatory header is the 'Host' field.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testHttpServerReceivedRequiredHeaders() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/get/method", null, "GET", null, null, null,
                        Collections.emptyList())).build();

        // ACT
        probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        final Optional<Map<String, List<String>>> headers = handler.getLastRequestHeaders();

        // ASSERT
        assertTrue(headers.isPresent());
        assertTrue(headers.get().containsKey(HttpHeaders.HOST));
    }

    /**
     * Tests that the http server did indeed receive the http headers being sent.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testHttpServerReceivedHeaders() throws InterruptedException {
        // ARRANGE
        final List<RequestHeader> requestHeaders = Arrays.asList(
                new RequestHeader("header_name_1", "header_value_1"),
                new RequestHeader("header_name_2", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/get/method", null, "GET", null, null, null,
                        requestHeaders)).build();

        // ACT
        probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        final Optional<Map<String, List<String>>> headers = handler.getLastRequestHeaders();

        // ASSERT
        assertTrue(headers.isPresent());
        final Map<String, List<String>> headersMap = headers.get();
        assertTrue(headersMap.containsKey("header_name_1"));
        assertTrue(headersMap.containsKey("header_name_2"));
        assertEquals(headersMap.get("header_name_1").get(0), "header_value_1");
        assertEquals(headersMap.get("header_name_2").get(0), "header_value_2");
    }

    /**
     * Tests that an invalid endpoint returns a failed action result.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testInvalidEndpoint() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow("http://localhost:28121/not/found", null, "GET", null, null, null,
                        Collections.emptyList())).build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        assertEquals(ActionResponseState.FAILED, result.getState());
    }

    /**
     * Tests the case that workflow is a simple get call without body. The success code is 201 rather 200.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulGetMethod201() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121/get/method", null, "GET",
                        null, null, null, Collections.emptyList()))
                .build();

        handler.setResponseCode(201);

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                Collections.emptyList());
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
                        null, null, null, Collections.emptyList()))
                .build();

        handler.setResponseCode(404);
        handler.setResponseBody("XYXYXY");

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.FAILED, "GET", "/get/" + ACTION_UUID, null,
                Collections.emptyList());
        assertTrue(result.getDescription().contains("404"));
        assertTrue(result.getDescription().contains("XYXYXY"));
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
                        null, null, null, Collections.emptyList()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "POST", address, payload,
                Collections.emptyList());
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
                        null, null, null, Collections.emptyList()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "POST", address,
                "{\"id\": \"144151046183109\", \"type\": \"RESIZE\", \"commodity\": \"VCPU\", \"to\": \"3.0\"}",
                Collections.emptyList());
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
                        null, null, null, Collections.emptyList()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "PUT", "/actions/" + ACTION_UUID,
                "{\"description\": \"Resize up VCPU for Virtual Machine turbonomic-t8c from 2 to 3\"}",
                Collections.emptyList());
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
                        AuthenticationMethod.BASIC, "testUser", "testPass", Collections.emptyList()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, "GET", "/get/method", null,
                Collections.emptyList());

        // make sure request has basic auth header
        assertThat(handler.getLastRequest().get().headers.get("Authorization"),
                equalTo(Collections.singletonList("Basic dGVzdFVzZXI6dGVzdFBhc3M=")));
    }

    /**
     * Test sending newly generated actions to a webhook.
     *
     * @throws InterruptedException if something goes wrong.
     * @throws TargetOperationException if probe did not manage to communicate with a target.
     */
    @Test
    public void testSendingOnGenActionsToWebhook() throws InterruptedException, TargetOperationException {
        // ARRANGE
        final String address = "/actions/$action.uuid";
        final String payload = "{\"description\": \"$action.details\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121" + address, payload, "PUT",
                        AuthenticationMethod.NONE, null, null, Collections.emptyList()))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.PENDING_ACCEPT)
                .setTimestamp(System.currentTimeMillis())
                .build();

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    /**
     * Tests audit actions to webhook.
     *
     * @throws InterruptedException if something goes wrong.
     * @throws TargetOperationException if probe did not manage to communicate with a target.
     */
    @Test
    public void testSendingOnGenActionsToWebhookFailure() throws InterruptedException, TargetOperationException {
        // ARRANGE
        final String address = "/actions/$action.uuid";
        final String payload = "{\"description\": \"$action.details\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("http://localhost:28121" + address, payload, "PUT",
                        AuthenticationMethod.NONE, null, null, Collections.emptyList()))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.PENDING_ACCEPT)
                .setTimestamp(System.currentTimeMillis())
                .build();

        handler.setResponseCode(404);

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    /**
     * Test to verify the cleared actions are not audited.
     *
     * @throws InterruptedException if an error occurs.
     * @throws TargetOperationException if the Webhook probe cannot communicate with the target.
     */
    @Test
    public void testClearedActionsAreNotAudited() throws InterruptedException, TargetOperationException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION
                .toBuilder()
                .setWorkflow(createWorkflow("", "", "PUT",
                        AuthenticationMethod.NONE, null, null))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.CLEARED)
                .setTimestamp(System.currentTimeMillis())
                .build();

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    private void verifyResults(ActionResult result, ActionResponseState expectedState, String expectedMethod,
            String expectedAddress, String expectedPayload, final List<RequestHeader> headers) {
        assertThat(result.getState(), is(expectedState));
        Optional<Request> request = handler.getLastRequest();
        assertTrue(request.isPresent());
        assertThat(request.get().method, is(expectedMethod));
        assertThat(request.get().url, is(expectedAddress));
        assertThat(request.get().payload, is(expectedPayload));
        // verify headers if any
        if (!headers.isEmpty()) {
            final Map<String, List<String>> sentHeaders = convertListOfHeadersIntoMap(headers);
            final Map<String, List<String>> receivedHeaders = request.get().headers;
            sentHeaders.forEach((key, value) -> {
                final List<String> headerValues = receivedHeaders.get(key);
                Assert.assertTrue(CollectionUtils.isEqualCollection(headerValues, value));
            });
        }
    }

    private Map<String, List<String>> convertListOfHeadersIntoMap(
            final List<RequestHeader> headers) {
        final Map<String, List<String>> sentHeaders = new HashMap<>();
        for (RequestHeader requestHeader : headers) {
            sentHeaders.computeIfAbsent(requestHeader.getName(), k -> new ArrayList<>()).add(
                    requestHeader.getValue());
        }
        return sentHeaders;
    }

    /**
     * A response handler that caches the last request made to it.
     */
    private static class CachingHttpHandler implements HttpHandler {
        private Request lastRequest = null;
        private int responseCode = 200;
        private String responseBody = null;
        private Headers lastHeaders = null;

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

            lastHeaders = exchange.getRequestHeaders();

            if (responseBody != null) {
                final byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(responseCode, response.length);
                exchange.getResponseBody().write(response);
            } else {
                exchange.sendResponseHeaders(responseCode, 0);
            }
            exchange.getResponseBody().close();
        }

        void setResponseCode(int responseCode) {
            this.responseCode = responseCode;
        }

        void setResponseBody(String responseBody) {
            this.responseBody = responseBody;
        }

        Optional<Request> getLastRequest() {
            return Optional.ofNullable(lastRequest);
        }

        Optional<Map<String, List<String>>> getLastRequestHeaders() {
            return Optional.ofNullable(lastHeaders);
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
