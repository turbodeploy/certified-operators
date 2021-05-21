package com.vmturbo.mediation.webhook.connector;

import com.vmturbo.mediation.connector.common.HttpConnectorException;

/**
 * The wrapper exception class for all Webhook operation errors.
 */
public class WebhookException extends HttpConnectorException {

    private static final long serialVersionUID = -7270838047059539519L;

    /**
     * Class constructor.
     *
     * @param msg The input exception message.
     * @param cause The exception that caused this.
     */
    public WebhookException(String msg, final Throwable cause) {
        super(msg, cause);
    }
}
