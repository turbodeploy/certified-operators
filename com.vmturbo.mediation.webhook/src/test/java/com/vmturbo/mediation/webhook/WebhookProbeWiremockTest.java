package com.vmturbo.mediation.webhook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Uses wiremock data we recorded in {@link WebhookProbeTestIT#testWiremockRecord()} to see the
 * webhook probe.
 */
public class WebhookProbeWiremockTest {

    private static final long RESIZE_ACTION_OID = 9223304701146931424L;
    private static final String TEST_SEVERITY = "CRITICAL";
    private static final String TEST_SUBCATEGORY = "Compliance";

    private static final EntityProperty TARGET_TYPE_PROPERTY =
        EntityProperty.newBuilder()
            .setNamespace("DEFAULT")
            .setName("TargetType")
            .setValue("vCenter")
            .build();

    private static final EntityDTO TARGET_ENTITY =
        EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("423fb423-7c15-4afe-f316-a0e8e490e065")
            .setDisplayName("Mysql-172.103")
            .addEntityProperties(
                EntityProperty.newBuilder()
                    .setNamespace("DEFAULT")
                    .setName("LocalName")
                    .setValue("vm-70")
                    .build()
            )
            .addEntityProperties(TARGET_TYPE_PROPERTY)
            .build();

    private static final EntityDTO HOSTED_BY_ENTITY =
        EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("34313339-3330-5553-4537-30314e384436")
            .setDisplayName("dc17-host-03.eng.vmturbo.com")
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace("DEFAULT")
                .setName("LocalName")
                .setValue("host-623")
                .build())
            .addEntityProperties(TARGET_TYPE_PROPERTY)
            .build();

    private static final double VCPU_USED = 2883.433349609375;

    /**
     * When the endpoint returns 200, the action should succeed.
     *
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testValidEndpoint() throws InterruptedException {
        final WireMockServer server = startServer("normalRequest");
        try {
            ActionResult result = executeAction(
                    createExecutedAction("http://localhost:" + server.port() + "/runAction"));
            Assert.assertEquals(ActionResponseState.SUCCEEDED, result.getState());
        } finally {
            server.stop();
        }
    }

    /**
     * When the endpoint returns 404, we should fail the action.
     *
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testNotFound() throws InterruptedException {
        final WireMockServer server = startServer("endpointNotFound");
        try {
            ActionResult result = executeAction(
                    createExecutedAction("http://localhost:" + server.port() + "/notFound"));
            Assert.assertEquals(ActionResponseState.FAILED, result.getState());
        } finally {
            server.stop();
        }
    }

    /**
     * Starts a wiremock server using the provided classpath as the mock data to return.
     *
     * @param directory The location in the JVM's classpath that contains the mock data.
     * @return an reference to the service that must be shutdown by the caller.
     */
    private WireMockServer startServer(String directory) {
        final WireMockConfiguration options =
            WireMockConfiguration.options()
                .dynamicPort()
                .dynamicHttpsPort()
                .usingFilesUnderClasspath("com/vmturbo/mediation/webhook/wiremock/" + directory);
        WireMockServer server = new WireMockServer(options);
        server.start();

        return server;
    }

    private ActionResult executeAction(ActionExecutionDTO actionExecutionDTO) throws InterruptedException {
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        IProgressTracker progressTracker = mock(IProgressTracker.class);
        return probe.executeAction(actionExecutionDTO,
            new WebhookAccount(),
            new HashMap<>(),
            progressTracker);
    }

    private ActionExecutionDTO createExecutedAction(String webhookURL) {
        return ActionExecutionDTO.newBuilder()
                .setActionType(ActionType.RESIZE)
                .setActionOid(RESIZE_ACTION_OID)
                .setExplanation("Test resize action")
                .setSubCategory(TEST_SUBCATEGORY)
                .setSeverity(TEST_SEVERITY)
                .setWorkflow(Workflow.newBuilder()
                        .setId("Webhook")
                        .addProperty(Property.newBuilder()
                                .setName("TEMPLATED_ACTION_BODY")
                                .setValue("Send this to the webhook endpoint")
                                .build())
                        .addProperty(Property.newBuilder()
                                .setName("URL")
                                .setValue(webhookURL)
                                .build())
                        .addProperty(Property.newBuilder()
                                .setName("HTTP_METHOD")
                                .setValue(HttpMethodType.POST.name())
                                .build())
                        .build())
                .addActionItem(
                        ActionItemDTO.newBuilder()
                                .setUuid(String.valueOf(RESIZE_ACTION_OID))
                                .setActionType(ActionType.RESIZE)
                                .setTargetSE(TARGET_ENTITY)
                                .setHostedBySE(HOSTED_BY_ENTITY)
                                .setSavings((float)0.0)
                                .setSavingUnit("")
                                .setCommodityAttribute(CommodityAttribute.Capacity)
                                .setCurrentComm(CommodityDTO.newBuilder()
                                        .setCommodityType(CommodityType.VCPU)
                                        .setCapacity(1.0)
                                        .setUsed(VCPU_USED)
                                        .build())
                                .setNewComm(CommodityDTO.newBuilder()
                                        .setCommodityType(CommodityType.VCPU)
                                        .setCapacity(2.0)
                                        .setUsed(VCPU_USED)
                                        .build())
                                .build()
                ).build();
    }
}
