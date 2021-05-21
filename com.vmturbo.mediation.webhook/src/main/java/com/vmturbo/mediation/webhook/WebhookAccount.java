package com.vmturbo.mediation.webhook;

import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.MANDATORY;
import static com.vmturbo.platform.sdk.probe.AccountValue.Constraint.OPTIONAL;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;
import com.vmturbo.platform.sdk.probe.AccountValue.Constraint;

/**
 * Account values for the Webhook probe.
 */
@AccountDefinition
public class WebhookAccount {

    @AccountValue(targetId = true, displayName = "Name", constraint = MANDATORY,
            description = "Display name of the webhook")
    private final String displayName;

    @AccountValue(displayName = "URL", constraint = MANDATORY,
            description = "The full URL to the endpoint. For example https://www.endpoint.com/executeAction"
    )
    private final String url;

    @AccountValue(
            displayName = "HTTP Method",
            description = "Select the HTTP method",
            allowedValues = {"GET", "POST", "PUT", "DELETE"},
            constraint = Constraint.MANDATORY
    )
    private final String httpMethod;

    @AccountValue(displayName = "Template", constraint = OPTIONAL,
            description = "The request body template to use for the webhook")
    private final String template;

    /**
     * Default, no-args constructor is required by the vmturbo-sdk-plugin during the build process.
     */
    public WebhookAccount() {
        displayName = "";
        url = "";
        template = "";
        httpMethod = "";
    }

    /**
     * Account values for the Webhook probe.
     *
     * @param displayName display name of the webhook.
     * @param url the url endpoint.
     * @param httpMethod the http method to use.
     * @param template the body of the request.
     */
    public WebhookAccount(
            @Nonnull String displayName,
            @Nonnull String url,
            @Nonnull String httpMethod,
            @Nonnull String template) {
        this.displayName = displayName;
        this.url = url;
        this.httpMethod = httpMethod;
        this.template = template;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getUrl() {
        return url;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getTemplate() {
        return template;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WebhookAccount)) {
            return false;
        }
        final WebhookAccount other = (WebhookAccount)obj;
        return Objects.equal(this.displayName, other.displayName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(displayName);
    }

    @Override
    public String toString() {
        // Make sure you do not place any customer secrets in toString()!!!
        return MoreObjects.toStringHelper(this)
                .add("displayName", getDisplayName())
                .add("url", getUrl())
                .add("httpMethod", getHttpMethod())
                .add("template", getTemplate())
                .toString();
    }
}
