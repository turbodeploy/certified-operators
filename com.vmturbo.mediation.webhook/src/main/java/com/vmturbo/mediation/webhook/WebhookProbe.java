package com.vmturbo.mediation.webhook;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AUTHENTICATION_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AUTHORIZATION_SERVER_URL;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.CLIENT_ID;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.CLIENT_SECRET;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.GRANT_TYPE;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.HTTP_METHOD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.PASSWORD;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.SCOPE;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TEMPLATED_ACTION_BODY;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.USER_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookQuery;
import com.vmturbo.mediation.webhook.connector.WebhookBody;
import com.vmturbo.mediation.webhook.connector.WebhookConnector;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;
import com.vmturbo.mediation.webhook.http.BasicHttpResponse;
import com.vmturbo.mediation.webhook.oauth.AccessTokenResponse;
import com.vmturbo.mediation.webhook.oauth.GrantType;
import com.vmturbo.mediation.webhook.oauth.OAuthCredentials;
import com.vmturbo.mediation.webhook.oauth.OAuthParameters;
import com.vmturbo.mediation.webhook.oauth.TokenManager;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
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
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IActionAudit;
import com.vmturbo.platform.sdk.probe.IActionExecutor;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntry;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.TargetOperationException;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Webhook probe supports sending HTTP requests to specified URL endpoint for PRE,POST, and REPLACE events.
 */
public class WebhookProbe
        implements IDiscoveryProbe<WebhookAccount>, IActionExecutor<WebhookAccount>,
        IActionAudit<WebhookAccount> {
    private static final String WEBHOOK_PROBE_PERSISTENT_DATA = "webhook-probe-persistent-data";

    private TokenManager tokenManager;

    private WebhookProperties webhookProperties;

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
            @Nullable ProbeConfiguration configuration) {
        final IPropertyProvider propertyProvider = probeContext.getPropertyProvider();
        webhookProperties = new WebhookProperties(propertyProvider);
        IProbeDataStoreEntry<byte[]> probePersistentData;
        try {
            probePersistentData = probeContext.getTargetFeatureCategoryData(WEBHOOK_PROBE_PERSISTENT_DATA);
        } catch (TargetOperationException e) {
            logger.error("Was not able to get Webhook probe persistent data.", e);
            probePersistentData = null;
        }
        tokenManager = new TokenManager(probePersistentData);
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

    @Nonnull
    @Override
    public Collection<ActionErrorDTO> auditActions(@Nonnull final WebhookAccount account,
            @Nonnull final Collection<ActionEventDTO> actionEvents)
            throws InterruptedException {
        for (ActionEventDTO actionEventDTO : actionEvents) {
            ActionExecutionDTO action = actionEventDTO.getAction();
            if (actionEventDTO.getNewState() == ActionResponseState.CLEARED) {
                logger.debug("Skip auditing CLEARED action with OID: " + action.getActionOid());
                continue;
            }
            try {
                prepareAndExecuteWebhookQuery(actionEventDTO.getAction());
            } catch (IOException | HttpConnectorException ex) {
                handleException(ex, actionEventDTO.getAction());
            }
        }
        return Collections.emptyList();
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
        final String exceptionMessage;
        try {
            prepareAndExecuteWebhookQuery(actionExecutionDto);
            return new ActionResult(ActionResponseState.SUCCEEDED,
                    "The call to webhook endpoint was successful.");
        } catch (IOException | HttpConnectorException ex) {
            exceptionMessage = handleException(ex, actionExecutionDto);
            return new ActionResult(ActionResponseState.FAILED, exceptionMessage);
        }
    }

    /**
     * Returns the body of the request or null if the webhook call has no body.
     *
     * @param actionExecutionDto the action execution request.
     * @return the {@link WebhookBody} used in request or null.
     * @throws WebhookException if there are more than one body template property.
     */
    @Nullable
    private WebhookBody getWebhookBody(@Nonnull final ActionExecutionDTO actionExecutionDto)
            throws WebhookException {
        final List<Property> webhookProperties = actionExecutionDto.getWorkflow().getPropertyList();
        return getWebhookPropertyValue(TEMPLATED_ACTION_BODY, webhookProperties)
                .map(WebhookBody::new)
                .orElse(null);
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
     * Gets the webhook headers.
     *
     * @param webhookProperties list of webhook properties.
     * @return collection of http headers for webhook request.
     * @throws WebhookException if there's multiple values found under the same key.
     */
    private Collection<Header> getWebhookHeaders(final List<Property> webhookProperties)
            throws WebhookException {
        List<Header> webhookHeaders = new ArrayList<>();
        int headerCount = 0;
        // iterate until there are no more headers.
        while (true) {
            Optional<String> headerName = getWebhookPropertyValue(
                    String.format(WebhookConstants.HEADER_NAME, headerCount),
                    webhookProperties);

            Optional<String> headerValue = getWebhookPropertyValue(
                    String.format(WebhookConstants.HEADER_VALUE, headerCount++),
                    webhookProperties);

            if (!headerName.isPresent() || !headerValue.isPresent()) {
                break;
            }
            webhookHeaders.add(new BasicHeader(headerName.get(), headerValue.get()));
        }

        return webhookHeaders;
    }

    /**
     * Creates webhook credentials used for sending request to webhook server.
     *
     * @param actionExecutionDTO the action execution request
     * @return the webhook credentials
     * @throws WebhookException if all data required for sending request to webhook server weren't provided
     */
    @Nonnull
    private WebhookCredentials createWebhookCredentials(final ActionExecutionDTO actionExecutionDTO)
            throws WebhookException {
        final List<Property> webhookProperties = actionExecutionDTO.getWorkflow().getPropertyList();

        final Optional<String> webhookHttpMethodType = getWebhookPropertyValue(HTTP_METHOD, webhookProperties);
        if (!webhookHttpMethodType.isPresent()) {
            throw new WebhookException(
                    "Webhook http method type was not provided in the list of workflow properties");
        }

        // get url
        final String webhookUrl = getWebhookPropertyValue(WebhookConstants.URL, webhookProperties)
            .orElseThrow(() ->
                new WebhookException("Webhook url was not provided in the list of workflow properties."));

        // get authentication info
        final AuthenticationMethod authenticationMethod = getWebhookPropertyValue(AUTHENTICATION_METHOD, webhookProperties)
                .map(AuthenticationMethod::valueOf).orElse(AuthenticationMethod.NONE);
        final String userName = getWebhookPropertyValue(USER_NAME, webhookProperties)
                .orElse(null);
        final String password = getWebhookPropertyValue(PASSWORD, webhookProperties)
                .orElse(null);

        // get HTTPS configuration parameters
        final boolean trustSelfSignedCertificates = getWebhookPropertyValue(TRUST_SELF_SIGNED_CERTIFICATES_PARAM_NAME, webhookProperties)
                .map(Boolean::parseBoolean)
                .orElse(false);

        // oAuth related fields
        String oAuthUrl = null;
        String clientId = null;
        String clientSecret = null;
        GrantType grantType = null;
        final String scope = getWebhookPropertyValue(SCOPE, webhookProperties).orElse(null);

        if (authenticationMethod == AuthenticationMethod.OAUTH) {
            oAuthUrl = getWebhookPropertyValue(AUTHORIZATION_SERVER_URL,
                    webhookProperties).orElseThrow(() -> new WebhookException(
                    "Missing required oAuth field: 'Authorization Server URL'."));

            clientId = getWebhookPropertyValue(CLIENT_ID, webhookProperties).orElseThrow(
                    () -> new WebhookException(
                            "Missing required oAuth field: 'Client ID'."));

            clientSecret = getWebhookPropertyValue(CLIENT_SECRET, webhookProperties).orElseThrow(
                    () -> new WebhookException(
                            "Missing or could not find required oAuth field: 'Client Secret'."));

            grantType = getWebhookPropertyValue(GRANT_TYPE, webhookProperties).map(
                    GrantType::valueOf).orElseThrow(() -> new WebhookException(
                    "Missing or could not find required oAuth field: 'Grant Type'."));
        }

        return new WebhookCredentials(webhookUrl, webhookHttpMethodType.get(),
                this.webhookProperties.getConnectionTimeout(), authenticationMethod, userName,
                password, trustSelfSignedCertificates, oAuthUrl, clientId, clientSecret,
                grantType, scope);
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

    private void prepareAndExecuteWebhookQuery(final ActionExecutionDTO actionExecutionDto)
            throws HttpConnectorException, IOException, InterruptedException {
        WebhookCredentials webhookCredentials = createWebhookCredentials(actionExecutionDto);
        try {
            executeWebhookQuery(actionExecutionDto, webhookCredentials);
        } catch (WebhookException e) {
            Integer statusCode = e.getResponseCode();
            if (webhookCredentials.getAuthenticationMethod() == AuthenticationMethod.OAUTH
                    && statusCode != null && statusCode >= 400 && statusCode < 500) {
                logger.debug("The call to the endpoint failed with exception for webhook "
                  + "{}. Will try to refresh token.", webhookCredentials, e);
                // attempt another webhook execution if we have a new Access Token.
                if (tokenManager.renewAccessToken(getOAuthCredentials(webhookCredentials))) {
                    logger.debug("The new access token is the different from the old one. "
                                + "Using it to make the new call. (webhook info: {})",
                            webhookCredentials);
                    executeWebhookQuery(actionExecutionDto, webhookCredentials);
                    return;
                } else {
                    logger.debug("The new access token is the same as old one. Ruling out "
                      + "expired access token as the cause of exception. (webhook info: {})",
                            webhookCredentials);
                }
            }
            throw e;
        }
    }


    private void executeWebhookQuery(final ActionExecutionDTO actionExecutionDto, WebhookCredentials webhookCredentials)
            throws HttpConnectorException, IOException, InterruptedException {
        try (WebhookConnector webhookConnector = new WebhookConnector(
                webhookCredentials, webhookProperties)) {
            setAuthorizationHeader(webhookCredentials, webhookConnector);
            final WebhookBody body = getWebhookBody(actionExecutionDto);
            final List<Property> webhookProperties =
                    actionExecutionDto.getWorkflow().getPropertyList();
            final WebhookQuery webhookQuery = new WebhookQuery(
                    getWebhookMethodType(webhookProperties), body,
                    getWebhookHeaders(webhookProperties));
            final BasicHttpResponse basicHttpResponse = webhookConnector.execute(webhookQuery);
            logger.info("The webhook call for action {} succeeded http status code {}.",
                    actionExecutionDto.getActionOid(), basicHttpResponse.getResponseCode());
        }
    }

    private void setAuthorizationHeader(@Nonnull WebhookCredentials webhookCredentials,
            @Nonnull WebhookConnector webhookConnector)
            throws HttpConnectorException, IOException, InterruptedException {
        if (webhookCredentials.getAuthenticationMethod() == AuthenticationMethod.OAUTH) {
            final AccessTokenResponse accessToken = tokenManager.requestAccessToken(
                    getOAuthCredentials(webhookCredentials));
            webhookConnector.getWebhookQueryConverter().setHeader(
                    OAuthParameters.AUTHORIZATION.getParameterId(),
                    String.format("%s %s", accessToken.getTokenType(),
                            accessToken.getAccessToken()));
        }
    }

    private String handleException(Exception ex, ActionExecutionDTO actionExecutionDTO) {
        if (ex instanceof WebhookException) {
            return handleWebhookException((WebhookException)ex, actionExecutionDTO);
        } else {
            logger.error("Calling webhook endpoint failed for action with id {} because of an exception.",
                    actionExecutionDTO, ex);
            return "Failed as the result of an exception";
        }
    }

    private String handleWebhookException(WebhookException ex, ActionExecutionDTO actionExecutionDto) {
        if (ex.getResponseCode() != null) {
            logger.error("Calling webhook endpoint failed for action with id {} as endpoint return status code {} "
              + "and body: \n {}", actionExecutionDto.getActionOid(), ex.getResponseCode(), ex.getResponseBody());
            // this error happened because of unexpected http response code, we return the response code and body
            return "Status code " + ex.getResponseCode() + " was returned from server with body:\n"
                    + ex.getResponseBody();
        } else {
            logger.error("Calling webhook endpoint failed for action with id {}", actionExecutionDto, ex);
            // there another cause for the exception. Just return exception message
            return ex.getMessage();
        }
    }

    private OAuthCredentials getOAuthCredentials(WebhookCredentials credentials) {
        return credentials.getOAuthCredentials().orElseThrow(
                () -> new IllegalStateException(
                        "Missing OAuth fields when Authentication method is OAUTH."));
    }
}
