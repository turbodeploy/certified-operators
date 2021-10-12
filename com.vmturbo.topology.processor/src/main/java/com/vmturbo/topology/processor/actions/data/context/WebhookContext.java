package com.vmturbo.topology.processor.actions.data.context;

import static com.vmturbo.common.protobuf.utils.StringConstants.WEBHOOK_OAUTH_CLIENT_SECRET_SUBJECT;
import static com.vmturbo.common.protobuf.utils.StringConstants.WEBHOOK_PASSWORD_SUBJECT;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AUTHENTICATION_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AUTHORIZATION_SERVER_URL;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.CLIENT_ID;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.CLIENT_SECRET;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.GRANT_TYPE;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.HTTP_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.PASSWORD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.SCOPE;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.USER_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo.AuthenticationMethod;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo.OAuthData;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;

/**
 * Utility class to extract webhook properties.
 */
public class WebhookContext {

    private WebhookContext() {}

    /**
     * Creates a list of {@link Workflow.Property} associated with webhooks.
     *
     * @param workflow The {@link WorkflowDTO.WorkflowInfo} to extract the webhook properties from.
     * @param secureStorageClient The {@link SecureStorageClient} to access webhook sensitive data.
     *
     * @return list of {@link Workflow.Property} containing webhook properties.
     * @throws ContextCreationException if failed to extract webhook fields.
     */
    @Nonnull
    public static List<Property> getProperties(@Nonnull final WorkflowDTO.Workflow workflow,
            @Nonnull SecureStorageClient secureStorageClient) throws ContextCreationException {
        if (!workflow.hasWorkflowInfo() || !workflow.getWorkflowInfo().hasWebhookInfo()) {
            return Collections.emptyList();
        }

        List<Workflow.Property> webhookProperties = new ArrayList<>();
        WorkflowDTO.WorkflowInfo.WebhookInfo webhookInfo =
                workflow.getWorkflowInfo().getWebhookInfo();
        if (!webhookInfo.hasHttpMethod() || !webhookInfo.hasUrl()) {
            throw new ContextCreationException(
                    "The HTTP METHOD, URL are required parameters for Webhook workflows");
        }

        webhookProperties.add(Workflow.Property.newBuilder()
                .setName(HTTP_METHOD)
                .setValue(webhookInfo.getHttpMethod().name())
                .build());

        if (webhookInfo.hasTrustSelfSignedCertificates()) {
            String trustedValue = Boolean.toString(webhookInfo.getTrustSelfSignedCertificates());
            webhookProperties.add(Workflow.Property.newBuilder().setName(
                    TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME).setValue(trustedValue).build());
        }

        if (webhookInfo.hasAuthenticationMethod()) {
            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(AUTHENTICATION_METHOD)
                    .setValue(webhookInfo.getAuthenticationMethod().name())
                    .build());
        }

        if (webhookInfo.hasUsername()) {
            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(USER_NAME)
                    .setValue(webhookInfo.getUsername())
                    .build());
        }

        if (webhookInfo.getAuthenticationMethod() == AuthenticationMethod.BASIC) {
            final String password;
            try {
                password = secureStorageClient.getValue(WEBHOOK_PASSWORD_SUBJECT,
                        Long.toString(workflow.getId())).orElseThrow(
                        () -> new IllegalStateException(
                                "Cannot retrieve the password for workflow " + workflow.getId()));
            } catch (CommunicationException e) {
                throw new ContextCreationException(
                        "Cannot get the password from the secure storage for"
                                + " webhook workflow with ID" + workflow.getId(), e);
            }

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(PASSWORD)
                    .setValue(password)
                    .build());
        } else if (webhookInfo.getAuthenticationMethod() == AuthenticationMethod.OAUTH) {
            if (!webhookInfo.hasOAuth()) {
                throw new ContextCreationException(
                        "The OAuth data is required for Webhook workflows using OAuth "
                                + "authentication method.");
            }
            final OAuthData oAuthData = webhookInfo.getOAuth();
            final String clientSecret;
            try {
                clientSecret = secureStorageClient.getValue(WEBHOOK_OAUTH_CLIENT_SECRET_SUBJECT,
                        Long.toString(workflow.getId())).orElseThrow(
                        () -> new IllegalStateException(
                                "Cannot retrieve the clientSecret for workflow " + workflow.getId()));
            } catch (CommunicationException e) {
                throw new ContextCreationException(
                        "Cannot get the clientSecret from the secure storage for"
                                + " webhook workflow with ID" + workflow.getId(), e);
            }

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(CLIENT_ID)
                    .setValue(oAuthData.getClientId())
                    .build());

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(CLIENT_SECRET)
                    .setValue(clientSecret)
                    .build());

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(AUTHORIZATION_SERVER_URL)
                    .setValue(oAuthData.getAuthorizationServerUrl())
                    .build());

            if (oAuthData.hasScope()) {
                webhookProperties.add(Workflow.Property.newBuilder()
                        .setName(SCOPE)
                        .setValue(oAuthData.getScope())
                        .build());
            }

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(GRANT_TYPE)
                    .setValue(oAuthData.getGrantType().name())
                    .build());
        }

        // Given a list of key-value pairs (name, value) for headers
        // we create a webhook property for each element in the pair
        final AtomicInteger headerCount = new AtomicInteger();
        webhookInfo.getHeadersList().forEach(header -> {
            String webhookHeaderName = String.format(WebhookConstants.HEADER_NAME,
                    headerCount.get());
            String webhookHeaderValue = String.format(WebhookConstants.HEADER_VALUE,
                    headerCount.getAndIncrement());
            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(webhookHeaderName)
                    .setValue(header.getName())
                    .build());

            webhookProperties.add(Workflow.Property.newBuilder()
                    .setName(webhookHeaderValue)
                    .setValue(header.getValue())
                    .build());
        });

        return webhookProperties;
    }
}
