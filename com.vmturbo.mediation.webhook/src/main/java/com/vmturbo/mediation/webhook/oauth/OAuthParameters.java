package com.vmturbo.mediation.webhook.oauth;

import com.vmturbo.mediation.webhook.http.BasicHttpParameter;

/**
 * Class that holds related oAuth constants.
 */
public class OAuthParameters {
    /**
     * The client-id header.
     */
    public static final BasicHttpParameter CLIENT_ID = new BasicHttpParameter("client_id");
    /**
     * the client-secret header.
     */
    public static final BasicHttpParameter CLIENT_SECRET = new BasicHttpParameter("client_secret");
    /**
     * the grant type header.
     */
    public static final BasicHttpParameter GRANT_TYPE = new BasicHttpParameter("grant_type");
    /**
     * the scope header.
     */
    public static final BasicHttpParameter SCOPE = new BasicHttpParameter("scope");
    /**
     * the authorization header.
     */
    public static final BasicHttpParameter AUTHORIZATION = new BasicHttpParameter("Authorization");
    /**
     * the fresh token header.
     */
    public static final BasicHttpParameter REFRESH_TOKEN = new BasicHttpParameter("refresh_token");


    private OAuthParameters() {}
}
