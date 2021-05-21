package com.vmturbo.mediation.webhook.connector;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;

import com.vmturbo.mediation.connector.common.credentials.PortAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.SecureAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TargetAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TimeoutAwareCredentials;
import com.vmturbo.mediation.webhook.WebhookAccount;

/**
 * Webhook credentials.
 */
public class WebhookCredentials
        implements TimeoutAwareCredentials, TargetAwareCredentials, SecureAwareCredentials, PortAwareCredentials {

    private final String url;
    private final String methodType;
    private final long timeout;

    /**
     * Creates a {@link WebhookCredentials} instance.
     *
     * @param account - Account information needed by an Webhook probe.
     * @param timeout - timeout value which will restrict time to interconnect with server.
     */
    public WebhookCredentials(@Nonnull WebhookAccount account, long timeout) {
        this.url = account.getUrl();
        this.methodType = account.getHttpMethod();
        this.timeout = timeout;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return methodType;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    /**
     * Only need this so that WebhookQueryConverter will work.
     * @return url since it's not used.
     */
    @Nonnull
    @Override
    public String getNameOrAddress() {
        return getUrl();
    }

    /**
     * Only need this so that WebhookQueryConverter will work.
     * @return False since it's not used.
     */
    @Override
    public boolean isSecure() {
        return false;
    }

    /**
     * Only need this so that WebhookQueryConverter will work.
     * @return 0 since it's not used.
     */
    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String toString() {
        // Make sure you do not place any customer secrets in toString()!!!
        return MoreObjects.toStringHelper(this)
            .add("url", getNameOrAddress())
            .add("method", getMethod())
            .add("port", getPort())
            .add("secure", isSecure())
            .add("timeout", getTimeout())
            .toString();
    }
}
