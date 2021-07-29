package com.vmturbo.mediation.webhook;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AUTHENTICATION_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.HTTP_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.PASSWORD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TEMPLATED_ACTION_BODY;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.URL;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.USER_NAME;

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
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.runtime.parser.ParseException;

import com.vmturbo.api.conversion.action.ActionToApiConverter;
import com.vmturbo.api.conversion.action.SdkActionInformationProvider;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookQuery;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookResponse;
import com.vmturbo.mediation.webhook.connector.WebhookBody;
import com.vmturbo.mediation.webhook.connector.WebhookConnector;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;
import com.vmturbo.mediation.webhook.template.Velocity;
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

    private WebhookProperties probeConfiguration;

    private static final ActionToApiConverter CONVERTER = new ActionToApiConverter();

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

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDto,
            @Nonnull final WebhookAccount accountValues,
            @Nullable final Map<String, Discovery.AccountValue> secondaryAccountValuesMap,
            @Nonnull final IProgressTracker progressTracker) throws InterruptedException {
        // get the ActionApiDTO
        final ActionApiDTO actionApiDTO = CONVERTER.convert(
                new SdkActionInformationProvider(actionExecutionDto),
                true,
                0L,
                false);

        try (WebhookConnector webhookConnector = new WebhookConnector(
                createWebhookCredentials(actionExecutionDto.getWorkflow().getPropertyList(), actionApiDTO),
                probeConfiguration)) {

            final WebhookBody body;
            try {
                body = getWebhookBody(actionExecutionDto, actionApiDTO);
            } catch (WebhookException ex) {
                logger.error("failed to apply template for body action with ID {}",
                        actionExecutionDto.getActionOid(), ex);
                return new ActionResult(
                        ActionResponseState.FAILED,
                        "Applying template to action failed.");
            }

            final WebhookQuery webhookQuery = new WebhookQuery(
                    getWebhookMethodType(actionExecutionDto.getWorkflow().getPropertyList()),
                    body);

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

    /**
     * Returns the body of the request or null if the webhook call has no body.
     *
     * @param actionExecutionDto the action execution request.
     * @param actionApiDTO the API object representation of the action.
     * @return the {@link WebhookBody} used in request or null.
     * @throws WebhookException if the applying template fails.
     */
    @Nullable
    private WebhookBody getWebhookBody(@Nonnull final ActionExecutionDTO actionExecutionDto,
                                       @Nonnull ActionApiDTO actionApiDTO) throws WebhookException {
        final Optional<String> template = getWebhookPropertyValue(TEMPLATED_ACTION_BODY,
                actionExecutionDto.getWorkflow().getPropertyList());

        if (template.isPresent()) {
            // apply the template to call body
            return new WebhookBody(applyTemplate(template.get(), actionApiDTO));
        } else {
            logger.debug("The body will be not set for action with Id {}.", actionExecutionDto.getActionOid());
            return null;
        }
    }

    /**
     * Applies a template using an {@link ActionApiDTO}.
     *
     * @param template the message template.
     * @param apiDTO the {@link ActionApiDTO} used for applying template.
     * @return the result message after applying template.
     * @throws WebhookException if somethings goes wrong in applying template.
     */
    @Nonnull
    private String applyTemplate(@Nonnull String template, @Nonnull ActionApiDTO apiDTO) throws WebhookException {
        try {
            return Velocity.apply(template, apiDTO);
        } catch (ParseException | IOException | MethodInvocationException e) {
            throw new WebhookException(String.format("Applying template \"%s\" failed.", template), e);
        }
    }

    private HttpMethodType getWebhookMethodType(final List<Property> webhookProperties)
            throws WebhookException {
        final Optional<String> webhookHttpMethodType = getWebhookPropertyValue(HTTP_METHOD, webhookProperties);
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
     * @param actionApiDTO the {@link ActionApiDTO} object
     * @return the webhook credentials
     * @throws WebhookException if all data required for sending request to webhook server weren't provided
     */
    @Nonnull
    private WebhookCredentials createWebhookCredentials(final List<Property> webhookProperties, ActionApiDTO actionApiDTO)
            throws WebhookException {
        final String webhookURL = getWebhookPropertyValue(URL,
                webhookProperties).orElseThrow(() -> new WebhookException(
                "Webhook url was not provided in the list of workflow properties."));

        final String templatedUrl = applyTemplate(webhookURL, actionApiDTO);

        final Optional<String> webhookHttpMethodType = getWebhookPropertyValue(HTTP_METHOD, webhookProperties);
        if (!webhookHttpMethodType.isPresent()) {
            throw new WebhookException(
                    "Webhook http method type was not provided in the list of workflow properties");
        }

        // get authentication info
        final AuthenticationMethod authenticationMethod = getWebhookPropertyValue(AUTHENTICATION_METHOD, webhookProperties)
                .map(AuthenticationMethod::valueOf).orElse(AuthenticationMethod.NONE);
        final String userName = getWebhookPropertyValue(USER_NAME, webhookProperties)
                .orElse(null);
        final String password = getWebhookPropertyValue(PASSWORD, webhookProperties)
                .orElse(null);

        return new WebhookCredentials(templatedUrl, webhookHttpMethodType.get(),
                probeConfiguration.getConnectionTimeout(), authenticationMethod, userName, password);
    }

    private Optional<String> getWebhookPropertyValue(@Nonnull final String webhookPropertyName,
            @Nonnull final List<Property> webhookProperties) throws WebhookException {
        List<String> propertyValues = webhookProperties.stream()
                .filter(property -> webhookPropertyName.equals(property.getName()))
                .map(Property::getValue)
                .collect(Collectors.toList());

        if (propertyValues.size() == 1) {
            return Optional.of(propertyValues.get(0));
        } else if (propertyValues.isEmpty()) {
            return Optional.empty();
        } else {
            throw new WebhookException(String.format("More than one %s property found!", webhookProperties));
        }
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
