package com.vmturbo.mediation.webhook.connector;

import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.connector.common.HttpConnector;
import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpConnectorFactory;
import com.vmturbo.mediation.connector.common.HttpConnectorSettings;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookQuery;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookResponse;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;

/**
 * Tests the webhook connector.
 */
public class WebhookConnectorTest {

    private WebhookCredentials webhookCredentials;
    private HttpConnectorFactory<HttpConnectorSettings, WebhookCredentials> connectorFactory;
    private WebhookConnector webhookConnector;
    private HttpConnector httpConnector;
    private WebhookQuery webhookPostQuery;

    /**
     * Initial set up for tests.
     */
    @Before
    public void init() {
        webhookCredentials = new WebhookCredentials("http://fake_webhook:142/endpoint",
                HttpMethodType.POST.name(), 30000L, AuthenticationMethod.BASIC, null, null, false);
        connectorFactory = Mockito.spy(WebhookConnector.createConnectorFactoryBuilder(30000,
                webhookCredentials).build());
        webhookConnector = new WebhookConnector(webhookCredentials, connectorFactory);
        httpConnector = Mockito.spy(HttpConnector.class);
        webhookPostQuery = new WebhookQuery(HttpMethodType.POST, new WebhookBody("Fake webhook body"));
    }

    /**
     * Test failed execution of webhook.
     *
     * @throws Exception {@link WebhookException} should be thrown.
     */
    @Test(expected = WebhookException.class)
    public void testFailedWebhookResponse() throws Exception {
        // ARRANGE
        Mockito.when(connectorFactory.getConnector(webhookCredentials)).thenReturn(httpConnector);
        Mockito.when(httpConnector.execute(Mockito.any())).thenThrow(new HttpConnectorException("response code was not 200"));

        // ACT
        webhookConnector.execute(webhookPostQuery);
    }

    /**
     * Test success execution of webhook.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testSuccessWebhookResponse() throws Exception {
        // ARRANGE
        Mockito.when(connectorFactory.getConnector(webhookCredentials)).thenReturn(httpConnector);
        Mockito.when(httpConnector.execute(Mockito.any())).thenReturn(new WebhookResponse(200,
                "This is the response body"));

        // ACT
        final WebhookResponse webhookResponse = webhookConnector.execute(webhookPostQuery);

        // ASSERT
        Assert.assertEquals("This is the response body", webhookResponse.getResponseBody());
    }

    /**
     * HttpConnectorException is translated to Webhook Exception.
     *
     * @throws Exception a WebhookException should be thrown.
     */
    @Test(expected = WebhookException.class)
    public void testUnableToInitializeConnection() throws Exception {
        when(connectorFactory.getConnector(webhookCredentials)).thenThrow(new HttpConnectorException("Testing connection initialization issue"));
        webhookConnector.execute(webhookPostQuery);
    }
}