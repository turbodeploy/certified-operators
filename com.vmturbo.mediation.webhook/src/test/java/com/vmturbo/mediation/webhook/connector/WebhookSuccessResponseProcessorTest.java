package com.vmturbo.mediation.webhook.connector;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookResponse;

/**
 * Verify {@link WebhookSuccessResponseProcessor}.
 */
public class WebhookSuccessResponseProcessorTest {

    private WebhookSuccessResponseProcessor webhookSuccessResponseProcessor = new WebhookSuccessResponseProcessor();

    /**
     * No HttpEntity should not throw an exception.
     *
     * @throws HttpConnectorException should not be thrown.
     */
    @Test
    public void testNoResponseBody() throws HttpConnectorException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getEntity()).thenReturn(null);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        WebhookResponse webhookResponse = webhookSuccessResponseProcessor.process(null, response, null);
        Assert.assertEquals("", webhookResponse.getResponseBody());
    }

    /**
     * IOException should be translated to HttpConnectorException.
     *
     * @throws Exception HttpConnectorException should be thrown.
     */
    @Test(expected = HttpConnectorException.class)
    public void testInvalidResponseBody() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenThrow(new IOException());
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        // should throw HttpConnectorException
        webhookSuccessResponseProcessor.process(null, response, null);
    }

}
