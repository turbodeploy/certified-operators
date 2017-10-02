package com.vmturbo.systest;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.xml.ws.http.HTTPException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * Class for making External API HTTP requests using either GET or POST.
 */
public class ExternalApiRequestor {
    private static final Logger logger = LogManager.getLogger();

    private String apiUrl;

    private RestTemplate restTemplate;

    /**
     * Create a requestor the external REST API. We create the stem of the
     * HTTP request url, including the hostname and port number of the Api Component
     * docker container.
     *
     * @param externalApiConfig the cluster config for the API component, to which the
     *                          external REST API calls should be sent
     */
    public ExternalApiRequestor(@Nonnull ComponentApiConnectionConfig externalApiConfig) {

        GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        restTemplate = new RestTemplate(Collections.singletonList(msgConverter));

        apiUrl = MessageFormat.format("http://{0}:{1,number,#}/vmturbo/api/v2",
                externalApiConfig.getHost(),
                externalApiConfig.getPort());
    }

    /**
     * Perform an HTTP GET request against the External API. Given the call-specific portion of
     * the HTTP request, prepends the common portion including the host name and port number.
     *
     * This call blocks until the request is received
     *
     * Note that the 'requestUrl' begins with a '/'.
     *
     * @param requestUrl the call-specific portion of the URL for this request; begins with a '/'
     * @param responseClass the class of the expected response for this request
     * @param <T> the type of the expected response for this request
     * @return the HTTP response body
     * @throws HTTPException for any non-200 HTTP response
     * @throws RuntimeException for other errors executing the request
     */
    <T> T externalApiGetRequest(@Nonnull String requestUrl, @Nonnull Class<T> responseClass) {

        String apiCallString = apiUrl + requestUrl;
        logger.trace("External API - GET request: {}", apiCallString);
        RequestEntity<?> request = RequestEntity.get(URI.create(apiCallString))
                .accept(MediaType.APPLICATION_JSON_UTF8, MediaType.TEXT_PLAIN, MediaType.ALL)
                .build();

        return waitForResponse(requestUrl, responseClass, request);
    }

    /**
     * Perform an HTTP POST request against the External API. Given the call-specific portion of
     * the HTTP request, prepends the common portion including the host name and port number.
     *
     * This call blocks until the request is received
     *
     * Note that the 'requestUrl' begins with a '/'.
     *
     * @param requestUrl the call-specific portion of the URL for this request; begins with a '/'
     * @param responseClass the class of the expected response for this request
     *                      @param requestData the data object to send with the POST request
     * @param <T> the type of the expected response for this request
     * @param <R> the type POST request data to be sent
     * @return the HTTP response body
     * @throws HTTPException for any non-200 HTTP response
     * @throws RuntimeException for other errors executing the request
     */
    <T, R> T externalApiPostRequest(@Nonnull String requestUrl,
                                    @Nonnull Class<T> responseClass,
                                    @Nonnull R requestData) {

        String apiCallString = apiUrl + requestUrl;
        logger.trace("External API - POST request: {}", apiCallString);
        RequestEntity<?> request = RequestEntity.post(URI.create(apiCallString))
                .accept(MediaType.APPLICATION_JSON_UTF8, MediaType.TEXT_PLAIN, MediaType.ALL)
                .body(requestData);

        return waitForResponse(requestUrl, responseClass, request);
    }

    /**
     * Perform the HTTP Request and wait for the response. Note that a runtime execption will
     * be thrown in case of RestClientException, null response, http status not OK, or
     * null response body.
     *
     * @param requestUrl the HTTP request URL to perform
     * @param responseClass the class of the expected response
     * @param request a request object to execute
     * @param <T> the type of the expected response
     * @return the body() of the response, of type T
     * @throws HTTPException for any non-200 HTTP response
     * @throws RuntimeException for other errors executing the request
     */
    private <T> T waitForResponse(@Nonnull String requestUrl,
                                  @Nonnull Class<T> responseClass,
                                  @Nonnull RequestEntity<?> request) {

        ResponseEntity<T> response;
        try {
            response = restTemplate.exchange(request, responseClass);
        } catch (RestClientException e) {
            throw new RuntimeException("Error executing API call " + requestUrl, e);
        }

        if (response == null) {
            throw new RuntimeException("Response to " + requestUrl +
                    " is null");
        }
        if (response.getStatusCode() != HttpStatus.OK) {
            throw new HTTPException(response.getStatusCodeValue());
        }
        if (response.getBody() == null) {
            throw new RuntimeException("Response body to " + requestUrl +
                    " is null");
        }
        return response.getBody();
    }
}