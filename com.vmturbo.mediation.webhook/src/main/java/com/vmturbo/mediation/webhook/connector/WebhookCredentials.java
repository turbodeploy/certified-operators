package com.vmturbo.mediation.webhook.connector;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.mediation.connector.common.credentials.PortAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.SecureAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TargetAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TimeoutAwareCredentials;
import com.vmturbo.mediation.webhook.oauth.GrantType;
import com.vmturbo.mediation.webhook.oauth.OAuthCredentials;

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
    private final OAuthCredentials oAuthCredentials;

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
        this.oAuthCredentials =
                authenticationMethod == AuthenticationMethod.OAUTH ? new OAuthCredentials(oAuthUrl,
                        clientID, clientSecret, grantType, scope, trustSelfSignedCertificates) : null;
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

    public Optional<OAuthCredentials> getOAuthCredentials() {
        return Optional.ofNullable(oAuthCredentials);
    }

    @Override
    public String toString() {
        return "WebhookCredentials{"
                + "webhookUrl='" + webhookUrl + '\''
                + ", methodType='" + methodType + '\''
                + ", timeout=" + timeout
                + ", authenticationMethod=" + authenticationMethod
                + ", userName='" + userName + '\''
                + ", trustSelfSignedCertificates=" + trustSelfSignedCertificates
                + ", oAuthCredentials=" + oAuthCredentials
                + '}';
    }
}
