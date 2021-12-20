package com.vmturbo.mediation.webhook.oauth;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.connector.common.HttpConnector;
import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpConnectorFactory;
import com.vmturbo.mediation.connector.common.HttpConnectorSettings;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.ParametersBasedHttpBody;
import com.vmturbo.mediation.connector.common.http.HttpParameter;
import com.vmturbo.mediation.webhook.connector.WebhookException;

/**
 * Class to test AccessTokenConnector.
 */
public class AccessTokenConnectorTest {

    private OAuthCredentials credentials;
    private HttpConnectorFactory<HttpConnectorSettings, OAuthCredentials> connectorFactory;
    private AccessTokenConnector accessTokenConnector;
    private HttpConnector httpConnector;
    private AccessTokenQuery accessTokenQuery;

    /**
     * Initial set up for tests.
     * @throws WebhookException if something goes wrong.
     */
    @Before
    public void init() throws WebhookException {
        credentials = new OAuthCredentials("http://google.com", "abc", "123", GrantType.CLIENT_CREDENTIALS, "wide", false);
        connectorFactory = Mockito.spy(
                AccessTokenConnector.createConnectorFactory(credentials, 30000));
        accessTokenConnector = new AccessTokenConnector(credentials, connectorFactory);
        httpConnector = Mockito.spy(HttpConnector.class);
        Map<HttpParameter, String> accessTokenParameters = new HashMap<HttpParameter, String>() {{
            put(OAuthParameters.CLIENT_ID, credentials.getClientID());
            put(OAuthParameters.CLIENT_SECRET, credentials.getClientSecret());
            put(OAuthParameters.GRANT_TYPE, credentials.getGrantType().getValue());
            put(OAuthParameters.SCOPE, credentials.getScope());
        }};
        accessTokenQuery = new AccessTokenQuery(HttpMethodType.POST,
                new ParametersBasedHttpBody(accessTokenParameters), Collections.emptyList());
    }

    /**
     * Test failed execution of webhook.
     *
     * @throws Exception {@link WebhookException} should be thrown.
     */
    @Test(expected = AccessTokenRequestException.class)
    public void testFailedAccessTokenRequestResponse() throws Exception {
        // ARRANGE
        Mockito.when(connectorFactory.getConnector(credentials)).thenReturn(httpConnector);
        Mockito.when(httpConnector.execute(Mockito.any())).thenThrow(
                new HttpConnectorException("response code was not 200"));

        // ACT
        accessTokenConnector.execute(accessTokenQuery);
    }

    /**
     * Test success execution of webhook.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testSuccessAccessTokenRequestResponse() throws Exception {
        // ARRANGE
        Mockito.when(connectorFactory.getConnector(credentials)).thenReturn(httpConnector);
        Mockito.when(httpConnector.execute(Mockito.any())).thenReturn(
                new AccessTokenResponse("123", "Bearer", 30, "abc", "wide", "", ""));

        // ACT
        final AccessTokenResponse accessTokenResponse = accessTokenConnector.execute(
                accessTokenQuery);

        // ASSERT
        Assert.assertEquals("123", accessTokenResponse.getAccessToken());
        Assert.assertEquals("Bearer", accessTokenResponse.getTokenType());
        Assert.assertEquals(30, accessTokenResponse.getExpiresIn());
        Assert.assertEquals("abc", accessTokenResponse.getRefreshToken());
        Assert.assertEquals("wide", accessTokenResponse.getScope());
        Assert.assertEquals("", accessTokenResponse.getError());
        Assert.assertEquals("", accessTokenResponse.getErrorDescription());
        Assert.assertNotEquals(0L, accessTokenResponse.getTimestamp());
    }

    /**
     * HttpConnectorException is translated to Webhook Exception.
     *
     * @throws Exception a WebhookException should be thrown.
     */
    @Test(expected = AccessTokenRequestException.class)
    public void testUnableToInitializeConnection() throws Exception {
        when(connectorFactory.getConnector(credentials)).thenThrow(
                new HttpConnectorException("Testing connection initialization issue"));
        accessTokenConnector.execute(accessTokenQuery);
    }
}
