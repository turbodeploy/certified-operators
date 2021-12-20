package com.vmturbo.mediation.webhook.oauth;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.annotation.Immutable;

import com.vmturbo.mediation.connector.common.credentials.PortAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.SecureAwareCredentials;
import com.vmturbo.mediation.connector.common.credentials.TargetAwareCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;

/**
 * Class that holds OAuth related fields.
 */
@Immutable
public class OAuthCredentials
        implements TargetAwareCredentials, SecureAwareCredentials, PortAwareCredentials {
    private final String oAuthUrl;
    private final URI oAuthUri;
    private final String clientID;
    private final String clientSecret;
    private final GrantType grantType;
    private final String scope;
    private final boolean trustSelfSignedCertificates;

    /**
     * OAuthCredentials constructor.
     *
     * @param oAuthUrl the authorization server.
     * @param clientID the client id.
     * @param clientSecret the client secret.
     * @param grantType the grant type.
     * @param scope the scope.
     * @param trustSelfSignedCertificates if the request should trust any certificate.
     * @throws WebhookException if the url is not valid.
     */
    public OAuthCredentials(@Nonnull String oAuthUrl, @Nonnull String clientID,
                            @Nonnull String clientSecret, @Nonnull GrantType grantType,
                            @Nullable String scope, boolean trustSelfSignedCertificates)
                            throws WebhookException {
        this.oAuthUrl = oAuthUrl;
        try {
            oAuthUri = URI.create(oAuthUrl);
        } catch (IllegalArgumentException ex) {
            throw new WebhookException("The OAuth authorization server url \""
                    + oAuthUrl + "\" is not valid.");
        }
        this.clientID = clientID;
        this.clientSecret = clientSecret;
        this.grantType = grantType;
        this.scope = scope;
        this.trustSelfSignedCertificates = trustSelfSignedCertificates;
    }

    public String getOAuthUrl() {
        return oAuthUrl;
    }

    public URI getOAuthUri() {
        return oAuthUri;
    }

    public String getClientID() {
        return clientID;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public GrantType getGrantType() {
        return grantType;
    }

    @Nullable
    public String getScope() {
        return scope;
    }

    public boolean isTrustSelfSignedCertificates() {
        return trustSelfSignedCertificates;
    }

    /**
     * Inherited method that is not used.
     * @return url since it's not used.
     */
    @Nonnull
    @Override
    public String getNameOrAddress() {
        return oAuthUrl;
    }

    /**
     * Inherited method that is not used.
     * @return False since it's not used.
     */
    @Override
    public boolean isSecure() {
        return false;
    }

    /**
     * Inherited method that is not used.
     *
     * @return 0 since it's not used.
     */
    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String toString() {
        return "OAuthCredentials{" + "oAuthUrl='" + oAuthUrl + '\'' + ", clientID='" + clientID
                + '\'' + ", grantType=" + grantType + ", scope='" + scope + '\''
                + ", trustSelfSignedCertificates=" + trustSelfSignedCertificates + '}';
    }
}
