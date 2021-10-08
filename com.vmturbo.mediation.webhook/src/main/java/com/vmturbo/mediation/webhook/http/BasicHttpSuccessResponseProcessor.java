package com.vmturbo.mediation.webhook.http;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpConnectorSettings;
import com.vmturbo.mediation.connector.common.HttpQuery;
import com.vmturbo.mediation.connector.common.Response;
import com.vmturbo.mediation.connector.common.http.response.processor.HttpResponseProcessor;

/**
 * The class is responsible for handling HTTP response in case of success HTTP code (eg. 200).
 */
public class BasicHttpSuccessResponseProcessor implements
        HttpResponseProcessor<HttpConnectorSettings, HttpQuery<?>, Response, HttpConnectorException> {

    @Nonnull
    @Override
    public BasicHttpResponse process(
            @Nonnull HttpQuery<?> httpQuery,
            @Nonnull CloseableHttpResponse response,
            @Nonnull HttpConnectorSettings settings) throws HttpConnectorException {
        try {
            final HttpEntity entity = response.getEntity();
            if (entity == null) {
                return new BasicHttpResponse(response.getStatusLine().getStatusCode(), "");
            } else {
                return new BasicHttpResponse(response.getStatusLine().getStatusCode(),
                        EntityUtils.toString(response.getEntity(), "UTF-8"));
            }
        } catch (IOException e) {
            throw new HttpConnectorException(e);
        }
    }
}
