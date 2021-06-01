package com.vmturbo.mediation.webhook;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookQuery;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookResponse;
import com.vmturbo.mediation.webhook.connector.WebhookBody;
import com.vmturbo.mediation.webhook.connector.WebhookConnector;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IActionExecutor;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Webhook probe supports sending HTTP requests to specified URL endpoint for PRE,POST, and REPLACE events.
 */
public class WebhookProbe
        implements IDiscoveryProbe<WebhookAccount>,
            IActionExecutor<WebhookAccount> {

    private static final String TEMPLATED_ACTION_BODY_PROPERTY = "TEMPLATED_ACTION_BODY";
    private static final String URL_PROPERTY = "URL";
    private static final String HTTP_METHOD_PROPERTY = "HTTP_METHOD";
    private WebhookProperties probeConfiguration;

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
            @Nullable ProbeConfiguration configuration) {
        final IPropertyProvider propertyProvider = probeContext.getPropertyProvider();
        probeConfiguration = new WebhookProperties(propertyProvider);
    }

    @Nonnull
    @Override
    public Class<WebhookAccount> getAccountDefinitionClass() {
        return WebhookAccount.class;
    }

    /**
     * Validate the target.
     *
     * @param accountValues Account definition map.
     * @return The message of target validation status.
     */
    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull WebhookAccount accountValues) {
        return ValidationResponse.newBuilder()
                .build();
    }

    /**
     * DiscoverTarget.
     *
     * @param accountValues the credentials to connect to Webhook, which are not needed for discovery.
     * @return the DiscoveryResponse.
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull WebhookAccount accountValues) {
        // We need to make sure that we can still connect to webhook to ensure that the credentials
        // are still valid. Without this double check, if a customer rediscovers a target that
        // failed validation, the target will appear OK even thought validation fails.
        final ValidationResponse validationResponse = validateTarget(accountValues);

        if (validationResponse.getErrorDTOList().isEmpty()) {
            return DiscoveryResponse.getDefaultInstance();
        } else {
            final List<String> errors = validationResponse.getErrorDTOList()
                    .stream()
                    .map(ErrorDTO::getDescription)
                    .collect(Collectors.toList());
            return SDKUtil.createDiscoveryErrorAndNotification(String.format(
                    "Discovery of the target %s failed on validation step with errors: %s",
                    accountValues, String.join(", ", errors)));
        }
    }

    @Override
    public boolean supportsVersion2ActionTypes() {
        return true;
    }

    private Optional<String> getBody(ActionExecutionDTO actionExecutionDto) {
        return actionExecutionDto.getWorkflow().getPropertyList().stream()
            .filter(property -> TEMPLATED_ACTION_BODY_PROPERTY.equals(property.getName()))
            .findAny()
            .map(Property::getValue);
    }

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDto,
            @Nonnull final WebhookAccount accountValues,
            @Nullable final Map<String, Discovery.AccountValue> secondaryAccountValuesMap,
            @Nonnull final IProgressTracker progressTracker) throws InterruptedException {
        try (WebhookConnector webhookConnector = new WebhookConnector(
                createWebhookCredentials(actionExecutionDto.getWorkflow().getPropertyList()),
                probeConfiguration)) {
            final Optional<String> bodyOpt = getBody(actionExecutionDto);
            if (!bodyOpt.isPresent()) {
                logger.error("For action oid: " + actionExecutionDto.getActionOid()
                    + " and account " + accountValues
                    + " action orchestrator did not fill in TEMPLATED_ACTION_BODY");
                return new ActionResult(
                    ActionResponseState.FAILED,
                    "ActionOrchestrator did not fill in TEMPLATED_ACTION_BODY");
            }
            String body = bodyOpt.get();
            final WebhookQuery webhookQuery = new WebhookQuery(
                    getWebhookMethodType(actionExecutionDto.getWorkflow().getPropertyList()),
                    new WebhookBody(body));
            final WebhookResponse webhookResponse = webhookConnector.execute(webhookQuery);
            logger.trace("Response from webhook endpoint: {}", webhookResponse.getResponseBody());
            return new ActionResult(ActionResponseState.SUCCEEDED,
                "Webhook successfully executed the action.");
        } catch (WebhookException | IOException e) {
            logger.error("There was an issue contacting the webhook", e);
            return new ActionResult(ActionResponseState.FAILED,
                    ExceptionUtils.getMessage(e) + "\n" + ExceptionUtils.getStackTrace(e));
        }
    }

    private HttpMethodType getWebhookMethodType(final List<Property> webhookProperties)
            throws WebhookException {
        final Optional<String> webhookHttpMethodType = getWebhookPropertyValue(HTTP_METHOD_PROPERTY, webhookProperties);
        if (!webhookHttpMethodType.isPresent()) {
            throw new WebhookException(
                    "Webhook http method type was not provided in the list of workflow properties");
        }
        return HttpMethodType.valueOf(webhookHttpMethodType.get());
    }

    /**
     * Creates webhook credentials used for sending request to webhook server.
     *
     * @param webhookProperties the list of webhook properties
     * @return the webhook credentials
     * @throws WebhookException if all data required for sending request to webhook server weren't provided
     */
    @Nonnull
    private WebhookCredentials createWebhookCredentials(final List<Property> webhookProperties)
            throws WebhookException {
        final Optional<String> webhookURL = getWebhookPropertyValue(URL_PROPERTY,
                webhookProperties);
        if (!webhookURL.isPresent()) {
            throw new WebhookException(
                    "Webhook url was not provided in the list of workflow properties");
        }
        final Optional<String> webhookHttpMethodType = getWebhookPropertyValue(HTTP_METHOD_PROPERTY, webhookProperties);
        if (!webhookHttpMethodType.isPresent()) {
            throw new WebhookException(
                    "Webhook http method type was not provided in the list of workflow properties");
        }
        return new WebhookCredentials(webhookURL.get(), webhookHttpMethodType.get(),
                probeConfiguration.getConnectionTimeout());
    }

    private Optional<String> getWebhookPropertyValue(@Nonnull final String webhookPropertyName,
            @Nonnull final List<Property> webhookProperties) {
        return webhookProperties.stream().filter(
                property -> webhookPropertyName.equals(property.getName())).findAny().map(
                Property::getValue);
    }

    @Nonnull
    @Override
    public List<ActionPolicyDTO> getActionPolicies() {
        /*
         * As an action orchestrator, this probe can execute almost every action, that is possible
         * in the world. As we do not have a separate feature for action orchestration, we are
         * masking the functionality under action execution. As a result, we have to provide
         * something from this method, while it is ignored on the server side.
         */
        return Collections.singletonList(
            ActionPolicyDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build());
    }
}
