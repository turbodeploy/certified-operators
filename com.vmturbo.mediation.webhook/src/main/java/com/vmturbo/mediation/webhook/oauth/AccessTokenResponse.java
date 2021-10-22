package com.vmturbo.mediation.webhook.oauth;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.gson.annotations.SerializedName;

import org.apache.http.annotation.Immutable;

import com.vmturbo.mediation.connector.common.Response;

/**
 * The response from an oauth server when requesting for an access token.
 */
@Immutable
public class AccessTokenResponse implements Response {
    /**
     * Safe buffer to protect against race conditions where we actually have an expired token.
     */
    public static final long SAFETY_BUFFER = TimeUnit.SECONDS.toMillis(10);

    @SerializedName("access_token")
    private final String access_token;
    @SerializedName("token_type")
    private final String token_type;
    @SerializedName("expires_in")
    private final long expires_in;
    @SerializedName("refresh_token")
    private final String refresh_token;
    @SerializedName("scope")
    private final String scope;
    @SerializedName("error")
    private final String error;
    @SerializedName("error_description")
    private final String error_description;
    // the time at which the token was created (in ms).
    private final long timestamp;

    /**
     * No arg constructor to set 'timestamp' variable using gson library.
     */
    public AccessTokenResponse() {
        access_token = null;
        token_type = null;
        expires_in = 0L;
        refresh_token = null;
        scope = null;
        error = null;
        error_description = null;
        timestamp = System.currentTimeMillis();
    }

    /**
     * AccessTokenResponse constructor.
     * @param access_token the access token.
     * @param token_type the token type.
     * @param expires_in the time in seconds from which the token expires.
     * @param refresh_token the refresh token (to be used to request a new token).
     * @param scope the scope.
     * @param error the error.
     * @param error_description the error description.
     */
    public AccessTokenResponse(String access_token, String token_type, long expires_in,
            String refresh_token, String scope, String error, String error_description) {
        this.access_token = access_token;
        this.token_type = token_type;
        this.expires_in = expires_in;
        this.refresh_token = refresh_token;
        this.scope = scope;
        this.error = error;
        this.error_description = error_description;
        timestamp = System.currentTimeMillis();
    }

    @Nullable
    public String getAccessToken() {
        return access_token;
    }

    @Nullable
    public String getTokenType() {
        return token_type;
    }

    public long getExpiresIn() {
        return expires_in;
    }

    @Nullable
    public String getRefreshToken() {
        return refresh_token;
    }

    @Nullable
    public String getScope() {
        return scope;
    }

    @Nullable
    public String getError() {
        return error;
    }

    @Nullable
    public String getErrorDescription() {
        return error_description;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Determines if an access token has been expired by using the following fields:
     *  - `expires_in`, represents when the token will expire, in seconds (from the time it was issued).
     *  - 'timestamp', represents the time at which the token was issued (in milliseconds).
     *  - 'SAFETY_BUFFER', represents a 10 second buffer that assumes the token expires 10 seconds earlier.
     *
     * @return if the access token has expired.
     */
    public boolean hasExpired() {
        return System.currentTimeMillis() > timestamp + TimeUnit.SECONDS.toMillis(expires_in) - SAFETY_BUFFER;
    }

    @Override
    public String toString() {
        return "AccessTokenResponse{" + "access_token='" + access_token + '\'' + ", token_type='"
                + token_type + '\'' + ", expires_in=" + expires_in + ", refresh_token='"
                + refresh_token + '\'' + ", scope='" + scope + '\'' + ", error='" + error + '\''
                + ", error_description='" + error_description + '\'' + ", timestamp=" + timestamp
                + '}';
    }
}
