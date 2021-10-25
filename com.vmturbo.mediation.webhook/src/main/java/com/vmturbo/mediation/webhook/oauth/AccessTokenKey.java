package com.vmturbo.mediation.webhook.oauth;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.annotation.Immutable;

/**
 * Class that acts as a key. Each key is associated with a {@link AccessTokenResponse}.
 */
@Immutable
public class AccessTokenKey implements Serializable {
    private final String oAuthUrl;
    private final String clientID;
    private final String clientSecretHash;
    private final GrantType grantType;
    private final String scope;

    /**
     * AccessTokenKey constructor.
     * @param oAuthUrl the authorization url.
     * @param clientID the client id.
     * @param clientSecret the client secret which is stored as a hash. clientSecret
     * is sensitive and we only care if there has been a change.
     * @param grantType the grant type.
     * @param scope the scope.
     */
    public AccessTokenKey(String oAuthUrl, String clientID, String clientSecret,
            GrantType grantType, @Nullable String scope) {
        this.oAuthUrl = oAuthUrl;
        this.clientID = clientID;
        this.clientSecretHash = DigestUtils.md5Hex(clientSecret);
        this.grantType = grantType;
        this.scope = scope;
    }

    /**
     * Create a access token from a {@link OAuthCredentials}.
     * @param credentials the oauth fields.
     * @return An AccessTokenKey.
     */
    public static AccessTokenKey create(@Nonnull OAuthCredentials credentials) {
        return new AccessTokenKey(credentials.getOAuthUrl(), credentials.getClientID(),
                credentials.getClientSecret(), credentials.getGrantType(), credentials.getScope());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof AccessTokenKey)) {
            return false;
        }

        AccessTokenKey accessTokenKey = (AccessTokenKey)o;
        return Objects.equals(this.oAuthUrl, accessTokenKey.oAuthUrl) && Objects.equals(
                this.clientID, accessTokenKey.clientID) && Objects.equals(this.clientSecretHash,
                accessTokenKey.clientSecretHash) && Objects.equals(this.grantType,
                accessTokenKey.grantType) && Objects.equals(this.scope, accessTokenKey.scope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.oAuthUrl, this.clientID, this.clientSecretHash, this.grantType,
                this.scope);
    }

    @Override
    public String toString() {
        return "AccessTokenKey{" + "oAuthUrl='" + oAuthUrl + '\'' + ", clientID='" + clientID + '\''
                + ", grantType=" + grantType + ", scope='" + scope + '\'' + '}';
    }
}
