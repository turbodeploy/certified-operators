package com.vmturbo.mediation.webhook.http;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContexts;

/**
 * Class that holds common connector components.
 */
public class ConnectorCommon {

    private ConnectorCommon() {}

    /**
     * Configure the builder to trust all certificates.
     *
     * @param builder The HTTP builder to configure
     *
     * @return an SSL socket factory that accepts all certificates. This has already been
     *         applied to the builder.
     */
    public static SSLConnectionSocketFactory trustAllCertificates(HttpClientBuilder builder) {
        // Allow all certificates. We have no (easy) way for customers to
        // install their own CA certificates right now.
        try {
            SSLContext sslcontext = SSLContexts.custom()
                    .loadTrustMaterial(null, new TrustAllStrategy())
                    .build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext,
                    new NoopHostnameVerifier());
            builder.setSSLSocketFactory(sslsf);

            // In case anyone else wants this
            return sslsf;
        } catch ( KeyManagementException | NoSuchAlgorithmException | KeyStoreException e ) {
            // We're not actually accessing any trust store here so none of these
            // exceptions should be possible.
            throw new RuntimeException("Could not set up to trust self-signed certificates (this should never happen)", e);
        }
    }

    /**
     * Constructs a http client to be used by a httpConnector.
     *
     * @param timeout the timeout used for a request.
     * @param isTrustSelfSignedCertificates if the request should trust any certificate.
     * @return  the http client.
     */
    public static CloseableHttpClient createHttpClient(final int timeout,
            boolean isTrustSelfSignedCertificates) {
        HttpClientBuilder builder = HttpClientBuilder.create();
        if (isTrustSelfSignedCertificates) {
            ConnectorCommon.trustAllCertificates(builder);
        }
        // Set the timeout
        final RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeout)
                .setSocketTimeout(timeout).build();
        builder.setDefaultRequestConfig(requestConfig);
        return builder.build();
    }
}
