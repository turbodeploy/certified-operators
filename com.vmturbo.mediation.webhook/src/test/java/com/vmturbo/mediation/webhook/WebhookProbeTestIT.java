package com.vmturbo.mediation.webhook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.recording.RecordSpec;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;

import org.junit.Test;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Test probe communication to a real webhook endpoint. It's an IT test so that it does not run
 * as part of the build. It makes it easy to manually test sending actions to a web hook without
 * having to launch all of Turbo.
 */
public class WebhookProbeTestIT {

    private static final String WEBHOOK_PREFIX = "http://10.10.168.162:5000";

    /**
     * Runs the probe against a real endpoint, and records the requests and responses so they can
     * be used in your tests.
     *
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testWiremockRecord() throws InterruptedException {
        WireMockServer server = recordingServer("wiremock-output");

        ActionExecutionDTO actionExecutionDTO = ActionExecutionDTO.newBuilder()
            .setActionOid(1L)
            .setActionType(ActionType.RESIZE)
            .setWorkflow(Workflow.newBuilder()
                .setId("Webhook")
                .addProperty(Property.newBuilder()
                    .setName("TEMPLATED_ACTION_BODY")
                    .setValue("Send this to the webhook endpoint")
                    .build())
                .build())
            .build();
        executeAction("http://localhost:" + server.port() + "/getGroups", actionExecutionDTO);

        server.stopRecording();
    }

    private WireMockServer recordingServer(String dir) {
        File files = new File(dir + "/__files");
        if (!files.exists()) {
            files.mkdirs();
        }
        File mappings = new File(dir + "/mappings");
        if (!mappings.exists()) {
            mappings.mkdirs();
        }
        final WireMockConfiguration options = WireMockConfiguration.options()
            .dynamicPort()
            .usingFilesUnderDirectory(dir);
        final RecordSpec recordSpecs =
            new RecordSpecBuilder().forTarget(WEBHOOK_PREFIX)
                .extractTextBodiesOver(0)
                .build();
        WireMockServer server = new WireMockServer(options);
        server.start();
        server.startRecording(recordSpecs);
        return server;
    }

    private ActionResult executeAction(String url, ActionExecutionDTO actionExecutionDTO) throws InterruptedException {
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        IProgressTracker progressTracker = mock(IProgressTracker.class);
        return probe.executeAction(actionExecutionDTO,
            new WebhookAccount(
                "Test webhook endpoint",
                url,
                HttpMethodType.POST.name(),
                "{id: $actionOid}"
            ),
            new HashMap<>(),
            progressTracker);
    }
}
