package com.vmturbo.mediation.webhook.connector;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;

import com.vmturbo.mediation.connector.common.credentials.PortAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.SecureAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TargetAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TimeoutAwareCredentials;

/**
 * Webhook credentials.
 */
public class WebhookCredentials
        implements TimeoutAwareCredentials, TargetAwareCredentials, SecureAwareCredentials, PortAwareCredentials {

    private static final Pattern HOST_PATTERN = Pattern.compile("(https?://[^/]*)(?:/.*|$)");

    private final String url;
    private final String methodType;
    private final long timeout;
    private final AuthenticationMethod authenticationMethod;
    private final String userName;
    private final String password;
    private final boolean trustSelfSignedCertificates;

    /**
     * Creates a {@link WebhookCredentials} instance.
     *
     * @param url - webhook url
     * @param httpMethodType - http method type used for sending webhook request
     * @param timeout - timeout value which will restrict time to interconnect with server.
     * @param authenticationMethod the method used to authenticate to the endpoint.
     * @param userName the username for authentication.
     * @param password the password for authentication.
     * @param trustSelfSignedCertificates true, if the user has indicated that this webhook should
     *        trust self-signed certificates when connecting
     */
    public WebhookCredentials(@Nonnull String url, @Nonnull String httpMethodType, long timeout,
                              @Nonnull AuthenticationMethod authenticationMethod, @Nullable String userName,
                              @Nullable String password, boolean trustSelfSignedCertificates) {
        this.url = Objects.requireNonNull(url);
        this.methodType = Objects.requireNonNull(httpMethodType);
        this.timeout = timeout;
        this.authenticationMethod = Objects.requireNonNull(authenticationMethod);
        this.userName = userName;
        this.password = password;
        this.trustSelfSignedCertificates = trustSelfSignedCertificates;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return methodType;
    }

    /**
     * Returns true if the user has indicated that this webhook should trust self-signed
     * certificates when connecting.
     *
     * @return true, if the user has indicated that this webhook should trust self-signed
     *         certificates when connecting; Otherwise, false.
     */
    public boolean isTrustSelfSignedCertificates() {
        return trustSelfSignedCertificates;
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
     *
     * <p>The reason we don't use this is that we allow the user to customize the URI using the
     * velocity template engine. Therefore, the URI is already constructed and will not be
     * assembled based on these parameters.</p>
     *
     * @return False since it's not used.
     */
    @Override
    public boolean isSecure() {
        return false;
    }

    /**
     * Only need this so that WebhookQueryConverter will work.
     *
     * <p>The reason we don't use this is that we allow the user to customize the URI using the
     * velocity template engine. Therefore, the URI is already constructed and will not be
     * assembled based on these parameters.</p>
     *
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
            .add("authenticationType", getAuthenticationMethod())
            .add("username", getUserName())
            .toString();
    }

    /**
     * The method of authentication used.
     *
     * @return method of authentication used.
     */
    public AuthenticationMethod getAuthenticationMethod() {
        return authenticationMethod;
    }

    /**
     * Gets the username for authentication.
     *
     * @return the username.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Get the password for authentication.
     *
     * @return the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * The url without the path section.
     *
     * @return the url without the path section.
     */
    public String getUrlWithoutPath() {
        Matcher matcher = HOST_PATTERN.matcher(url);

        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return url;
        }
    }
}
