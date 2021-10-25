package com.vmturbo.mediation.webhook.oauth;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.connector.common.http.HttpParameter;

/**
 * Class responsible for creating the AccessTokenRequest Body.
 */
public class AccessTokenRequestBody {

    private final OAuthCredentials oAuthCredentials;
    private final Map<HttpParameter, String> baseRequestParams;

    /**
     * AccessTokenRequestBody Constructor.
     *
     * @param oAuthCredentials the oauth data.
     */
    public AccessTokenRequestBody(@Nonnull OAuthCredentials oAuthCredentials) {
        this.oAuthCredentials = oAuthCredentials;
        baseRequestParams = new HashMap<HttpParameter, String>() {{
            if (oAuthCredentials.getScope() != null) {
                put(OAuthParameters.SCOPE, oAuthCredentials.getScope());
            }
        }};
    }

    /**
     * Create http post body request using refresh token.
     *
     * @param refreshToken the refresh token.
     * @return A map containing the fields used to construct a http body.
     */
    @Nonnull
    public Map<HttpParameter, String> createBodyUsingRefreshToken(@Nonnull String refreshToken) {
        return new HashMap<HttpParameter, String>(baseRequestParams) {{
            put(OAuthParameters.GRANT_TYPE, GrantType.REFRESH_TOKEN.getValue());
            put(OAuthParameters.REFRESH_TOKEN, refreshToken);
        }};
    }

    /**
     * Create http post body request using implied grant type.
     *
     * @return A map containing the fields used to construct a http body.
     */
    @Nonnull
    public Map<HttpParameter, String> createBodyUsingImpliedGrantType() {
        return new HashMap<HttpParameter, String>(baseRequestParams) {{
            put(OAuthParameters.CLIENT_ID, oAuthCredentials.getClientID());
            put(OAuthParameters.CLIENT_SECRET, oAuthCredentials.getClientSecret());
            put(OAuthParameters.GRANT_TYPE, oAuthCredentials.getGrantType().getValue());
        }};
    }
}
