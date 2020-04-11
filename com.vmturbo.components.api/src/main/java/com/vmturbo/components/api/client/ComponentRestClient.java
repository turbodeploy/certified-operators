package com.vmturbo.components.api.client;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.ComponentRestTemplate;

/**
 * Parent class to execute REST operations on a remote endpoint,
 * with inner classes to manage requests and error-handling.
 */
public abstract class ComponentRestClient {

    protected final Logger logger = LogManager.getLogger();

    protected final String restUri;

    protected ComponentRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        this.restUri = connectionConfig.getUri();
    }

    /**
     * Object, holding request-specific information to perform the request.
     *
     * @param <T> type of response, returned by this class.
     * @param <E> type of exception thrown when an API error code comes back from the server.
     */
    protected abstract class RestClient<T, E extends ApiClientException> {

        /**
         * Error codes that the REST endpoint returns which should propagate
         * to the user as an {@link ApiClientException}.
         */
        private final Set<HttpStatus> expectedErrorCodes;

        private final RestTemplate restTemplate;

        private final Class<T> responseClass;

        public RestClient(Class<T> responseClass, HttpStatus... expectedErrorCodes) {
            this.expectedErrorCodes = ImmutableSet.copyOf(expectedErrorCodes);
            this.responseClass = responseClass;

            restTemplate = ComponentRestTemplate.create();
            restTemplate.setErrorHandler(new ResponseErrorHandler() {

                @Override
                public boolean hasError(ClientHttpResponse response) throws IOException {
                    return !response.getStatusCode().is2xxSuccessful()
                            && !RestClient.this.expectedErrorCodes
                            .contains(response.getStatusCode());
                }

                @Override
                public void handleError(ClientHttpResponse response) throws IOException {
                    throw new RestClientException("REST communicationerror occurred: "
                                    + response.getStatusCode().getReasonPhrase());
                }
            });
        }

        @Nonnull
        public T execute(RequestEntity<?> request)
                throws E, CommunicationException {
            final ResponseEntity<T> response;
            try {
                logger.trace("Executing request {}", request);
                response = restTemplate.exchange(request, responseClass);
            } catch (RestClientException e) {
                throw new CommunicationException(createCommunicationExceptionMessage(request), e);
            }
            if (response.getStatusCode() == HttpStatus.OK) {
                final T result = response.getBody();
                logger.trace("Successfully retrieved response {}", result);
                return result;
            }
            if (expectedErrorCodes.contains(response.getStatusCode())) {
                logger.debug("Status code is {}, which is allowed by API",
                        response.getStatusCode());
                throw createException(response.getBody());
            }
            // This should never happen
            throw new CommunicationException(
                    "Unexpected HTTP status returned: " + response.getStatusCode());
        }

        /**
         * Construct an error message based on the request.  This allows subclasses to sanitize the
         * message in cases where the request may contain sensitive information like passwords.
         *
         * @param request the request that led to the communication exception.
         * @return String giving the message to include in the exception that we throw.
         */
        protected String createCommunicationExceptionMessage(RequestEntity<?> request) {
            return "Error executing request " + request;
        }

        /**
         * Constructs {@link E} based on the returned object.
         *
         * @param body REST response entity from the server
         * @return exception to create.
         */
        protected abstract E createException(T body);
    }

    /**
     * A {@link RestClient} for REST calls that return no exceptions.
     *
     * @param <T> {@inheritDoc}
     */
    protected class NoExceptionsRestClient<T> extends RestClient<T, ApiClientException> {

        public NoExceptionsRestClient(Class<T> clazz) {
            super(clazz);
        }

        @Override
        protected ApiClientException createException(T body) {
            throw new RuntimeException(
                    "There should be no error status code for get-all-targets request");
        }

        @Nonnull
        @Override
        public T execute(@Nonnull final RequestEntity<?> request) throws CommunicationException {
            try {
                return super.execute(Objects.requireNonNull(request));
            } catch (ApiClientException e) {
                // Should never happen
                throw new RuntimeException("Error performing request " + request, e);
            }
        }
    }

    /**
     * Build the query suffix to use to get multiple objects via a REST GET call.
     * Not templatized because the only use cases right now are to get a set of
     * objects by ID.
     *
     * @param key The key to use (usually something like "id").
     * @param values The list of OID's to look for.
     * @return The query string, starting with "?".
     */
    @Nonnull
    public static String buildIdQuery(@Nonnull final String key,
                                      @Nonnull final Set<Long> values) {
        final StringBuilder queryBuilder = new StringBuilder().append("?");
        final Iterator<Long> idIterator = Objects.requireNonNull(values).iterator();
        while (idIterator.hasNext()) {
            queryBuilder.append(key);
            queryBuilder.append("=");
            queryBuilder.append(idIterator.next());
            if (idIterator.hasNext()) {
                queryBuilder.append("&");
            }
        }
        return queryBuilder.toString();
    }
}
