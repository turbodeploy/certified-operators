package com.vmturbo.topology.processor.actions.data.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo.AuthenticationMethod;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo.HttpMethod;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo.RequestHeader;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;

/**
 * Tests the {@link WebhookContext} class.
 */
public class WebhookContextTest {

    private final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);

    /**
     * Test extracting properties.
     *
     * @throws ContextCreationException if failed to extract webhook properties.
     */
    @Test
    public void testGetProperties() throws ContextCreationException {
        final int workflowId = 0;
        final String url = "url";
        final String template = "template";
        final String username = "username";
        final String headerName = "header-name";
        final String headerValue = "header-value";
        final boolean trustSelfSigned = false;
        final WebhookInfo.AuthenticationMethod authenticationMethod = AuthenticationMethod.NONE;

        WorkflowDTO.Workflow workflow = WorkflowDTO.Workflow.newBuilder()
                .setId(workflowId)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setName("workflow")
                        .setWebhookInfo(WebhookInfo.newBuilder()
                                .setHttpMethod(HttpMethod.PUT)
                                .setUrl(url)
                                .setTemplate(template)
                                .setAuthenticationMethod(authenticationMethod)
                                .setTrustSelfSignedCertificates(trustSelfSigned)
                                .setUsername(username)
                                .addHeaders(RequestHeader.newBuilder()
                                        .setName(headerName)
                                        .setValue(headerValue)
                                        .build())
                                .build())
                        .build())
                .build();

        List<Property> properties = WebhookContext.getProperties(workflow, secureStorageClient);

        Optional<String> usernameProperty = getWebhookPropertyValue(WebhookConstants.USER_NAME,
                properties);
        assertTrue(usernameProperty.isPresent());

        Optional<String> trustSelfSignedProperty = getWebhookPropertyValue(
                WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME, properties);
        assertTrue(trustSelfSignedProperty.isPresent());

        Optional<String> authenticationMethodProperty = getWebhookPropertyValue(
                WebhookConstants.AUTHENTICATION_METHOD, properties);
        assertTrue(authenticationMethodProperty.isPresent());

        Optional<String> headerNameProperty = getWebhookPropertyValue(
                String.format(WebhookConstants.HEADER_NAME, 0), properties);
        assertTrue(headerNameProperty.isPresent());

        Optional<String> headerValueProperty = getWebhookPropertyValue(
                String.format(WebhookConstants.HEADER_VALUE, 0), properties);
        assertTrue(headerValueProperty.isPresent());

        assertEquals(usernameProperty.get(), username);
        assertEquals(trustSelfSignedProperty.get(), String.valueOf(trustSelfSigned));
        assertEquals(authenticationMethodProperty.get(), authenticationMethod.toString());
        assertEquals(headerNameProperty.get(), headerName);
        assertEquals(headerValueProperty.get(), headerValue);
    }

    /**
     * Extracts specific webhook properties from {@link Workflow.Property}.
     *
     * @param webhookPropertyName the property identifier using {@link WebhookConstants}.
     * @param webhookProperties list of {@link Workflow.Property}.
     * @return webhook property
     */
    private Optional<String> getWebhookPropertyValue(@Nonnull final String webhookPropertyName,
            @Nonnull final List<Property> webhookProperties) {
        return webhookProperties.stream()
                .filter(property -> webhookPropertyName.equals(property.getName()))
                .map(Property::getValue)
                .findFirst();
    }
}
