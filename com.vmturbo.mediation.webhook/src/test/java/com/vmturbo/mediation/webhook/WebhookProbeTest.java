package com.vmturbo.mediation.webhook;

import static com.vmturbo.mediation.webhook.WebhookProbeLocalServerTest.ON_PREM_RESIZE_ACTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.connector.common.HttpMethodType;
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
    final IProgressTracker progressTracker = mock(IProgressTracker.class);
    final WebhookAccount account = new WebhookAccount();

    private WebhookProbe probe;

    /**
     * Prepares for the testing.
     */
    @Before
    public void prepareTest() {
        IProbeContext probeContext = mock(IProbeContext.class);
        // set up probe related stuff
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);
        probe = new WebhookProbe();
        probe.initialize(probeContext, null);
    }

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
        ActionExecutionDTO actionExecutionDTO = createActionExecutionDTO(new WebhookPropertiesBuilder()
                .setTemplatedActionBody("test")
                .build());

        // this method should throw an exception
        final ActionResult result = probe.executeAction(actionExecutionDTO,
                account,
                Collections.emptyMap(),
                progressTracker);
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
    }

    /**
     * Test the case that an invalid url is sent to probe.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testWebhookWithInvalidUrl() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
          createWorkflow(new WebhookPropertiesBuilder()
            .setUrl("https://127.0.0.1/{cloud=com.vmturbo.api.dto.entityaspect."
                    + "Cloud@91302a19}. cloudAspect.resourceGroup.displayName/")
            .setHttpMethod(HttpMethodType.GET.name())
            .build()))
        .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO,
                account,
                Collections.emptyMap(),
                progressTracker);

        // ASSERT
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
        Assert.assertEquals("The url \"https://127.0.0.1/{cloud=com.vmturbo.api.dto.entityaspect"
                + ".Cloud@91302a19}. cloudAspect.resourceGroup.displayName/\" is not valid.",
                result.getDescription());
    }

    /**
     * Test the case that an invalid url is sent to probe.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testWebhookWithInvalidOAuthUrl() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
            createWorkflow(new WebhookPropertiesBuilder()
                .setUrl("http://127.0.0.1/")
                .setHttpMethod(HttpMethodType.GET.name())
                .setAuthenticationMethod(WebhookConstants.AuthenticationMethod.OAUTH.name())
                .setOAuthUrl("https://127.0.0.1/{cloud=com.vmturbo.api.dto.entityaspect."
                        + "Cloud@91302a19}. cloudAspect.resourceGroup.displayName/")
                .setClientId("testClient")
                .setClientSecret("testSecret")
                .setGrantType(WebhookConstants.GrantType.CLIENT_CREDENTIALS.name())
                .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO,
                account,
                Collections.emptyMap(),
                progressTracker);

        // ASSERT
        Assert.assertEquals(ActionResponseState.FAILED, result.getState());
        Assert.assertEquals("The OAuth authorization server url \"https://127.0.0.1/"
                + "{cloud=com.vmturbo.api.dto.entityaspect.Cloud@91302a19}. cloudAspect."
                + "resourceGroup.displayName/\" is not valid.", result.getDescription());
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

        if (webhookProperties.getClientId() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.CLIENT_ID)
                    .setValue(webhookProperties.getClientId())
                    .build());
        }

        if (webhookProperties.getClientSecret() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.CLIENT_SECRET)
                    .setValue(webhookProperties.getClientSecret())
                    .build());
        }

        if (webhookProperties.getOAuthUrl() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.AUTHORIZATION_SERVER_URL)
                    .setValue(webhookProperties.getOAuthUrl())
                    .build());
        }

        if (webhookProperties.getScope() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.SCOPE)
                    .setValue(webhookProperties.getScope())
                    .build());
        }

        if (webhookProperties.getGrantType() != null) {
            workflow.addProperty(Property.newBuilder()
                    .setName(WebhookConstants.GRANT_TYPE)
                    .setValue(webhookProperties.getGrantType())
                    .build());
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
        private final String clientId;
        private final String clientSecret;
        private final String oAuthUrl;
        private final String scope;
        private final String grantType;

        private WebhookProperties(String url, String httpMethod, String authenticationMethod,
                String username, String password, String trustSelfSignedCertificate,
                String templatedActionBody, List<Pair<String, String>> headers,
                boolean hasTemplateApplied, String clientId, String clientSecret,
                String oAuthUrl, String scope, String grantType) {
            this.url = url;
            this.httpMethod = httpMethod;
            this.authenticationMethod = authenticationMethod;
            this.username = username;
            this.password = password;
            this.trustSelfSignedCertificate = trustSelfSignedCertificate;
            this.templatedActionBody = templatedActionBody;
            this.headers = headers;
            this.hasTemplateApplied = hasTemplateApplied;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.oAuthUrl = oAuthUrl;
            this.scope = scope;
            this.grantType = grantType;
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

        public String getClientId() {
            return clientId;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public String getOAuthUrl() {
            return oAuthUrl;
        }

        public String getScope() {
            return scope;
        }

        public String getGrantType() {
            return grantType;
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
            private String clientId;
            private String clientSecret;
            private String oAuthUrl;
            private String scope;
            private String grantType;

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

            public WebhookPropertiesBuilder setClientId(String clientId) {
                this.clientId = clientId;
                return this;
            }

            public WebhookPropertiesBuilder setClientSecret(String clientSecret) {
                this.clientSecret = clientSecret;
                return this;
            }

            public WebhookPropertiesBuilder setOAuthUrl(String oAuthUrl) {
                this.oAuthUrl = oAuthUrl;
                return this;
            }

            public WebhookPropertiesBuilder setScope(String scope) {
                this.scope = scope;
                return this;
            }

            public WebhookPropertiesBuilder setGrantType(String grantType) {
                this.grantType = grantType;
                return this;
            }

            public WebhookProperties build() {
                return new WebhookProperties(url, httpMethod, authenticationMethod, username, password,
                        trustSelfSignedCertificate, templatedActionBody, headers, hasTemplateApplied,
                        clientId, clientSecret, oAuthUrl, scope, grantType);
            }
        }
    }
}