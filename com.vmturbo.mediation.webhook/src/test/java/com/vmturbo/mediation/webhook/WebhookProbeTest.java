package com.vmturbo.mediation.webhook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.webhook.WebhookProbeTest.WebhookProperties.WebhookPropertiesBuilder;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests the WebhookProbeTest.
 */
public class WebhookProbeTest {
    /**
     * If validation fails, then discovery should also fail.
     */
    @Test
    public void testFailingValidationFailsDiscovery() {
        WebhookProbe webhookProbe = Mockito.spy(new WebhookProbe());
        // return succeeding validation
        when(webhookProbe.validateTarget(any())).thenReturn(
                ValidationResponse.newBuilder()
                        .addErrorDTO(ErrorDTO.newBuilder()
                                .setDescription("something bad happened")
                                .setSeverity(ErrorSeverity.CRITICAL)
                                .build())
                        .build());

        final WebhookAccount account = new WebhookAccount();
        DiscoveryResponse response = webhookProbe.discoverTarget(account);

        Assert.assertEquals(0, response.getWorkflowList().size());
        Assert.assertEquals(1, response.getErrorDTOList().size());
    }

    /**
     * Makes sure the probe supports version two action types.
     */
    @Test
    public void testSupportsVersion2ActionTypes() {
        WebhookProbe probe = new WebhookProbe();
        Assert.assertTrue(probe.supportsVersion2ActionTypes());
    }

    /**
     * When we don't receive required webhook data using for sending request to workflow server
     * (e.g. URL_PROPERTY, HTTP_METHOD_PROPERTY), we should fail execution.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testWebhookDataNotProvided() throws Exception {

        final IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        final IProbeContext probeContext = mock(IProbeContext.class);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);

        final WebhookProbe probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        final IProgressTracker progressTracker = mock(IProgressTracker.class);

        ActionExecutionDTO actionExecutionDTO = createActionExecutionDTO(new WebhookPropertiesBuilder()
                .setTemplatedActionBody("test")
                .build());

        // this method should throw an exception
        final ActionResult result = probe.executeAction(actionExecutionDTO,
                new WebhookAccount(),
                new HashMap<>(),
                progressTracker);
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
    }

    private ActionExecutionDTO createActionExecutionDTO(WebhookProperties webhookProperties) {
        return ActionExecutionDTO.newBuilder().setActionType(ActionType.RESIZE)
            .addActionItem(ActionExecution.ActionItemDTO.newBuilder()
                    .setActionType(ActionType.RESIZE)
                    .setUuid("1363")
                    .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                            .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                            .setId("4521")
                            .build())
                    .build()).setWorkflow(createWorkflow(webhookProperties))
                .build();
    }

    /**
     * Creates a workflow with input parameters.
     *
     * @param webhookProperties the webhook properties.
     * @return the workflow object created.
     */
    public static Workflow createWorkflow(WebhookProperties webhookProperties) {
        Workflow.Builder workflow = Workflow.newBuilder()
                .setId("Webhook");

        if (webhookProperties.getUrl() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.URL)
                    .setValue(webhookProperties.getUrl())
                    .build());
        }

        if (webhookProperties.getHttpMethod() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.HTTP_METHOD)
                    .setValue(webhookProperties.getHttpMethod())
                    .build());
        }

        if (webhookProperties.getTemplatedActionBody() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.TEMPLATED_ACTION_BODY)
                    .setValue(webhookProperties.getTemplatedActionBody())
                    .build());
        }

        if (webhookProperties.getUsername() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.USER_NAME)
                    .setValue(webhookProperties.getUsername())
                    .build());
        }

        if (webhookProperties.getPassword() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.PASSWORD)
                    .setValue(webhookProperties.getPassword())
                    .build());
        }

        if (webhookProperties.getTrustSelfSignedCertificate() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME)
                    .setValue(webhookProperties.getTrustSelfSignedCertificate())
                    .build());
        }

        if (webhookProperties.getAuthenticationMethod() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.AUTHENTICATION_METHOD)
                    .setValue(webhookProperties.getAuthenticationMethod())
                    .build());
        }

        workflow.addProperty(Property.newBuilder()
                .setName(WebhookConstants.HAS_TEMPLATE_APPLIED)
                .setValue(String.valueOf(webhookProperties.hasTemplateApplied()))
                .build());

        if (webhookProperties.getHeaders() != null) {
            workflow.addAllProperty(populateHeadersAsProperties(webhookProperties.getHeaders()));
        }

        return workflow.build();
    }

    /**
     * Represent request headers as workflow properties.
     * Used the same conversion logic as we have in production in TP (AbstractActionExecutionContext#getWebhookProperties)
     *
     * @param headers request headers
     * @return the list of workflow properties
     */
    private static List<Property> populateHeadersAsProperties(
            @Nonnull List<Pair<String, String>> headers) {
        final List<Property> result = new ArrayList<>(headers.size());
        final AtomicInteger headerCount = new AtomicInteger();
        headers.forEach(header -> {
            String webhookHeaderName = String.format(WebhookConstants.HEADER_NAME,
                    headerCount.get());
            String webhookHeaderValue = String.format(WebhookConstants.HEADER_VALUE,
                    headerCount.getAndIncrement());

            final Property headerName = Property.newBuilder().setName(webhookHeaderName).setValue(
                    header.getKey()).build();

            final Property headerValue = Workflow.Property.newBuilder()
                    .setName(webhookHeaderValue)
                    .setValue(header.getValue())
                    .build();

            result.add(headerName);
            result.add(headerValue);
        });
        return result;
    }

    /**
     * Utility class to hold webhook properties.
     */
    static class WebhookProperties {
        private final String url;
        private final String httpMethod;
        private final String authenticationMethod;
        private final String username;
        private final String password;
        private final String trustSelfSignedCertificate;
        private final String templatedActionBody;
        private final List<Pair<String, String>> headers;
        private final boolean hasTemplateApplied;

        private WebhookProperties(String url, String httpMethod, String authenticationMethod,
                String username, String password, String trustSelfSignedCertificate,
                String templatedActionBody, List<Pair<String, String>> headers,
                boolean hasTemplateApplied) {
            this.url = url;
            this.httpMethod = httpMethod;
            this.authenticationMethod = authenticationMethod;
            this.username = username;
            this.password = password;
            this.trustSelfSignedCertificate = trustSelfSignedCertificate;
            this.templatedActionBody = templatedActionBody;
            this.headers = headers;
            this.hasTemplateApplied = hasTemplateApplied;
        }

        public String getUrl() {
            return url;
        }

        public String getHttpMethod() {
            return httpMethod;
        }

        public String getAuthenticationMethod() {
            return authenticationMethod;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getTrustSelfSignedCertificate() {
            return trustSelfSignedCertificate;
        }

        public String getTemplatedActionBody() {
            return templatedActionBody;
        }

        public List<Pair<String, String>> getHeaders() {
            return headers;
        }

        public boolean hasTemplateApplied() {
            return hasTemplateApplied;
        }

        /**
         * WebhookProperties builder class.
         */
        public static class WebhookPropertiesBuilder {
            private String url;
            private String httpMethod;
            private String authenticationMethod;
            private String username;
            private String password;
            private String trustSelfSignedCertificate;
            private String templatedActionBody;
            private List<Pair<String, String>> headers;
            private boolean hasTemplateApplied;

            public WebhookPropertiesBuilder setUrl(String url) {
                this.url = url;
                return this;
            }

            public WebhookPropertiesBuilder setHttpMethod(String httpMethod) {
                this.httpMethod = httpMethod;
                return this;
            }

            public WebhookPropertiesBuilder setAuthenticationMethod(String authenticationMethod) {
                this.authenticationMethod = authenticationMethod;
                return this;
            }

            public WebhookPropertiesBuilder setUsername(String username) {
                this.username = username;
                return this;
            }

            public WebhookPropertiesBuilder setPassword(String password) {
                this.password = password;
                return this;
            }

            public WebhookPropertiesBuilder setTrustSelfSignedCertificate(
                    String trustSelfSignedCertificate) {
                this.trustSelfSignedCertificate = trustSelfSignedCertificate;
                return this;
            }

            public WebhookPropertiesBuilder setTemplatedActionBody(String templatedActionBody) {
                this.templatedActionBody = templatedActionBody;
                return this;
            }

            public WebhookPropertiesBuilder setHeaders(List<Pair<String, String>> headers) {
                this.headers = headers;
                return this;
            }

            public WebhookPropertiesBuilder setHasTemplateApplied(boolean hasTemplateApplied) {
                this.hasTemplateApplied = hasTemplateApplied;
                return this;
            }

            public WebhookProperties build() {
                return new WebhookProperties(url, httpMethod, authenticationMethod, username, password,
                        trustSelfSignedCertificate, templatedActionBody, headers, hasTemplateApplied);
            }
        }
    }
}