package com.vmturbo.mediation.webhook.connector;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.connector.common.HttpBody;

/**
 * Body for webhook call.
 */
public class WebhookBody implements HttpBody {

    private final String webhookBody;

    /**
     * Constructor.
     *
     * @param webhookBody the templated webhook body
     */
    public WebhookBody(@Nonnull String webhookBody) {
        this.webhookBody = Objects.requireNonNull(webhookBody);
    }

    /**
     * Return webhook body.
     *
     * @return the webhook body
     */
    public String getWebhookBody() {
        return webhookBody;
    }
}
