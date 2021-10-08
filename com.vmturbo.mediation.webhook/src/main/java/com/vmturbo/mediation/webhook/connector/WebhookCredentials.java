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
import com.vmturbo.mediation.webhook.oauth.GrantType;

/**
 * Webhook credentials.
 */
public class WebhookCredentials
        implements TimeoutAwareCredentials, TargetAwareCredentials, SecureAwareCredentials, PortAwareCredentials {

    private static final Pattern HOST_PATTERN = Pattern.compile("(https?://[^/]*)(?:/.*|$)");

    private final String webhookUrl;
    private final String methodType;
    private final long timeout;
    private final AuthenticationMethod authenticationMethod;
    private final String userName;
    private final String password;
    private final boolean trustSelfSignedCertificates;
    private final String oAuthUrl;
    private final String clientID;
    private final String clientSecret;
    private final GrantType grantType;
    private final String scope;

    /**
     * Creates a {@link WebhookCredentials} instance.
     *
     * @param webhookUrl - webhook url
     * @param httpMethodType - http method type used for sending webhook request
     * @param timeout - timeout value which will restrict time to interconnect with server.
     * @param authenticationMethod the method used to authenticate to the endpoint.
     * @param userName the username for authentication.
     * @param password the password for authentication.
     * @param trustSelfSignedCertificates true, if the user has indicated that this webhook should
     * trust self-signed certificates when connecting.
     * @param oAuthUrl the oAuth endpoint.
     * @param clientID the oAuth clientId.
     * @param clientSecret the oAuth client secret.
     * @param grantType the grant type used for the oauth request.
     * @param scope the scope.
     */
    public WebhookCredentials(@Nonnull String webhookUrl, @Nonnull String httpMethodType, long timeout,
                              @Nonnull AuthenticationMethod authenticationMethod, @Nullable String userName,
                              @Nullable String password, boolean trustSelfSignedCertificates, @Nullable String oAuthUrl,
                              @Nullable String clientID, @Nullable String clientSecret, @Nullable GrantType grantType,
                              @Nullable String scope) {
        this.webhookUrl = Objects.requireNonNull(webhookUrl);
        this.methodType = Objects.requireNonNull(httpMethodType);
        this.timeout = timeout;
        this.authenticationMethod = Objects.requireNonNull(authenticationMethod);
        this.userName = userName;
        this.password = password;
        this.trustSelfSignedCertificates = trustSelfSignedCertificates;
        this.oAuthUrl = oAuthUrl;
        this.clientID = clientID;
        this.clientSecret = clientSecret;
        this.grantType = grantType;
        this.scope = scope;
    }

    public String getWebhookUrl() {
        return webhookUrl;
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
        return getWebhookUrl();
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
        Matcher matcher = HOST_PATTERN.matcher(webhookUrl);

        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return webhookUrl;
        }
    }

    @Nullable
    public String getOAuthUrl() {
        return oAuthUrl;
    }

    @Nullable
    public String getClientID() {
        return clientID;
    }

    @Nullable
    public String getClientSecret() {
        return clientSecret;
    }

    @Nullable
    public GrantType getGrantType() {
        return grantType;
    }

    @Nullable
    public String getScope() {
        return scope;
    }
}
