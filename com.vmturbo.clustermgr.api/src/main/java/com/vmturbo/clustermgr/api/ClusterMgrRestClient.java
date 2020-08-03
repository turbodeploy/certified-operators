package com.vmturbo.clustermgr.api;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.core.UriBuilder;

import com.google.common.io.ByteStreams;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

/**
 * Wrapper for the REST API for the ClusterMgr Component.
 **/
public class ClusterMgrRestClient extends ComponentRestClient {

    private static final String REST_API_PREFIX = "";

    // the URI to access the entire cluster state
    private static final String CLUSTER_CONFIG_URI = "";

    // the current state of the component instances
    private static final String COMPONENT_INSTANCES_STATE_URI = "/state";

    // fetch the diagnostics from all the running instances
    private static final String DIAGNOSTICS_URI = "/diagnostics";

    // configuration for the various components
    private static final String KNOWN_COMPONENTS_URI = "/components";
    private static final String TELEMETRY_INITIALIZED = "/proactive/initialized";
    private static final String TELEMETRY_ENABLED = "/proactive/enabled";
    private static final String COMPONENT_INSTANCE_EFFECTIVE_PROPERTIES_URI =
        "/components/{componentType}/instances/{instanceId}/effectiveProperties";
    private static final String COMPONENT_INSTANCE_PROPERTIES_URI =
        "/components/{componentType}/instances/{instanceId}/properties";
    private static final String COMPONENT_INSTANCE_PROPERTY_URI =
        "/components/{componentType}/instances/{instanceId}/properties/{propertyName}";
    private static final String COMPONENT_LOCAL_PROPERTIES_URI =
        "/components/{componentType}/localProperties";
    private static final String COMPONENT_LOCAL_PROPERTY_URI =
        "/components/{componentType}/localProperties/{propertyName}";
    private static final String COMPONENT_DEFAULT_PROPERTIES_URI =
        "/components/{componentType}/defaults";
    private static final String COMPONENT_DEFAULT_PROPERTY_URI =
        "/components/{componentType}/defaults/{propertyName}";
    private static final String COMPONENT_TYPE_INSTANCE_IDS_URI =
        "/components/{componentType}/instances";

    // the base URI to use for all accesses
    private String uriBase;

    // a RestTemplate to re-use in forwarding requests to ClusterMgr Component
    private RestTemplate restTemplate;
    // a RestTemplate to re-use for streaming REST requests, e.g. diagnostics
    private RestTemplate streamingRestTemplate;

    ClusterMgrRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);

        uriBase = restUri + REST_API_PREFIX;

        // set up the RestTemplate to re-use in forwarding requests to RepositoryComponent
        restTemplate = prepareRestTemplate();
        // set up a RestTemplate to re-use in streaming a large response; the response data will not be buffered locally.
        streamingRestTemplate = prepareRestTemplate();
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setBufferRequestBody(false);
        streamingRestTemplate.setRequestFactory(requestFactory);
    }

    /**
     * Create a RestTemplate by specifying the list converters explicitly.
     * This REST client uses the MappingJackson2HttpMessageConverter.
     * We are using this approach instead of calling ComponentRestTemplate.create() and handling
     * this REST client as a special case.
     *
     * @return The properly configured {@link RestTemplate} for {@link ClusterMgrRestClient}
     */
    @Nonnull
    private RestTemplate prepareRestTemplate() {
        final List<HttpMessageConverter<?>> converters = new ArrayList<>();
        converters.add(new ByteArrayHttpMessageConverter());
        converters.add(new StringHttpMessageConverter());
        converters.add(new MappingJackson2HttpMessageConverter());
        return new RestTemplate(converters);
    }

    // return the single restTemplate to be reused by all requests
    protected RestTemplate getRestTemplate() {
        return restTemplate;
    }

    // return the single restTemplate to be reused by all requests
    protected RestTemplate getStreamingRestTemplate() {
        return streamingRestTemplate;
    }

    /**
     * This method indicates whether or not we are running under XL.  In this case, we are, so always return 'true'.
     *
     * @return 'true' indicating that we are running under XL.
     */
    public boolean isXLEnabled() {
        return true;
    }

    /**
     * Indicates whether the telemetry is enabled.
     *
     * @return {@code true} iff telemetry is enabled.
     */
    public boolean isTelemetryInitialized() {
        return new RestGetRequestor<Boolean>(TELEMETRY_INITIALIZED, Boolean.class).invoke();
    }

    /**
     * Indicates whether the telemetry is enabled.
     *
     * @return {@code true} iff telemetry is enabled.
     */
    public boolean isTelemetryEnabled() {
        return new RestGetRequestor<Boolean>(TELEMETRY_ENABLED, Boolean.class).invoke();
    }

    /**
     * Sets the telemetry enabled flag.
     *
     * @param enabled The telemetry enabled flag.
     */
    public void setTelemetryEnabled(boolean enabled) {
        new RestPutRequestor<Void, Boolean>(TELEMETRY_ENABLED, Boolean.class).invokeVoid(enabled, enabled);
    }

    /**
     * Populate a ClusterConfiguration object with the current definition: all known component types,
     *  the defaults and current configuration properties for each component type, and all the
     *  component instances.
     *
     * @return an aggregate ClusterConfiguration populated with node/component/configuration and component/default-configuration
     * values.
     */
    @Nonnull
    public ClusterConfiguration getClusterConfiguration() {
        return new RestGetRequestor<ClusterConfiguration>(CLUSTER_CONFIG_URI, ClusterConfiguration.class)
                .invoke();
    }

    /**
     * Replace the entire Cluster Configuration.
     * This includes the <strong>default properties</strong> {@link ComponentProperties}
     * (component-type -> default properties) local properties {@link ComponentProperties}
     * (component-type -> local properties), and instances (component-type ->
     * (component-instance-id -> @link com.vmturbo.api.dto.cluster.ComponentInstanceDTO}
     *
     * @param newConfiguration an {@link ClusterConfiguration} to completely replace the current configuration.
     * @return the new configuration, read back from the Consul key/value store.
     */
    @Nonnull
    public ClusterConfiguration setClusterConfiguration(@Nonnull ClusterConfiguration newConfiguration) {
        return new RestPutRequestor<ClusterConfiguration, ClusterConfiguration>(CLUSTER_CONFIG_URI,
                    ClusterConfiguration.class)
                .invoke(newConfiguration);
    }

    /**
     * Fetch the set of Components known to VMTurbo from the Consul K/V store.
     * Components are "known" if there is a configuration key "{@code /vmturbo/components/{component-name}/}".
     *
     * <p>If no matching configuration keys are found, then the global key/value store is initialized from the
     * default Component list in application.yml.
     *
     * @return the set of all component names known to VMTurbo.
     */
    @Nonnull
    public Set<String> getKnownComponents() {
        return new RestGetRequestor<Set<String>>(KNOWN_COMPONENTS_URI, Set.class).invoke();
    }

    /**
     * Set the value of a given component property for the given component instance.
     *
     * <p>Set the value for the given property on the given component instance. Based on the semantics of Consul, any
     * previous value for this property will be overwritten, and if the property did not exist before then it is created.
     *
     * <p>The new value may be null, which effectively deletes this property. The "customer" will need to provide a suitable
     * default. This will help us implement a "search path" of configuration properties.
     *
     * <p>Returns the new value. Note that there is a write/read with no locking, and so the value returned may
     * differ if there has been an intervening write.
     *
     * @param componentType the type of the given component instance
     * @param instanceId the unique id of the given component instance
     * @param propertyName the name of the configuration property to set
     * @param propertyValue the new value for the given configuration property
     * @return the new value of the configuration property
     */
    public String setComponentInstanceProperty(String componentType,
                                               String instanceId,
                                               String propertyName,
                                               String propertyValue) {
        return new RestPutRequestor<String, String>(COMPONENT_INSTANCE_PROPERTY_URI, String.class)
            .invoke(propertyValue, componentType, instanceId, propertyName);
    }

    /**
     * Look up a set of currently configured Component Instance IDs belonging to a given Component Type.
     *
     * <p>Each Instance Id represents a VMT Component instance to be launched as part of the OpsMgr Cluster
     *
     * @param componentType the component type to which the answer Instance IDs belong
     * @return a set of Component Instance ID strings as configured in the current OpsMgr
     */
    public Set<String> getComponentInstanceIds(String componentType) {
        return new RestGetRequestor<Set<String>>(COMPONENT_TYPE_INSTANCE_IDS_URI, Set.class)
            .invoke(componentType);
    }

    /**
     * Set the local value for the given property on the given component instance.
     * Any previous value for this property will be overwritten, and if the property did not
     * exist before then it is created.
     *
     * <p>Returns the new value. Note that there is a write/read with no locking, and so the value returned may
     * differ if there has been an intervening write.
     *
     * @param componentType the type of the given component instance
     * @param propertyName the name of the configuration property to set
     * @param propertyValue the new value for the given configuration property
     * @return the new value of the configuration property
     */
    public String setComponentLocalProperty(String componentType,
                                            String propertyName,
                                            String propertyValue) {
        return new RestPutRequestor<String, String>(COMPONENT_LOCAL_PROPERTY_URI, String.class)
                .invoke(propertyValue, componentType, propertyName);
    }

    /**
     * Delete the local value for the given property for the given component type. Since there is
     * no longer a local override, then the default value for that property will be in effect.
     *
     * @param componentType the component type of the given component instance.
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return the default value of the configuration property for the given component
     */
    public String deleteComponentLocalProperty(String componentType,
                                              String propertyName) {
        return new RestDeleteRequestor<String>(COMPONENT_LOCAL_PROPERTY_URI, String.class)
                .invoke(componentType, propertyName);
    }

    /**
     * Gather the current state from each running VMT Component Instance.
     *
     * @return a map of component_id -> status
     */
    public Map<String, String> getComponentsState() {
        return new RestGetRequestor<Map<String, String>>(COMPONENT_INSTANCES_STATE_URI, Map.class)
                .invoke();
    }

    /**
     * This method streams the diagnostics .zip back to the original requestor.
     * The diagnostics .zip data may be very large, and so buffering it in local memory would be a bad idea.
     *
     * @param responseOutput the output stream onto which the diagnostics .zip,
     *                       as streamed from the request we generate here, should be written.
     */
    public void collectComponentDiagnostics(OutputStream responseOutput) {
        RequestCallback requestCallback = request -> request.getHeaders()
            .setAccept(Arrays.asList(
                new MediaType("application", "zip"),
                MediaType.APPLICATION_OCTET_STREAM,
                MediaType.ALL));
        ResponseExtractor<Object> copyRequestToResponseStream = request -> {
            try {
                // copy from the target component request to the collected response
                ByteStreams.copy(request.getBody(), responseOutput);
            } catch (EOFException e) {
                // if there's an  EOF on the response output, close the request stream
                request.getBody().close();
                logger.error("EOF on the diagnostics response output; diagnostics truncated.");
                throw new HttpClientErrorException(HttpStatus.PARTIAL_CONTENT);
            } catch (IOException e) {
                logger.error("IOException {} collecting component diagnostics.",
                    e.toString());
            }
            return null;
        };
        getStreamingRestTemplate().execute(uriBase + DIAGNOSTICS_URI,
            HttpMethod.GET,
            requestCallback,
            copyRequestToResponseStream);
    }


    /**
     * Export diagnostics file.
     *
     * @param httpProxyConfig the value object with http proxy settings
     * @return whether diagnostics has been successfully exported
     */
    public boolean exportComponentDiagnostics(@Nonnull HttpProxyConfig httpProxyConfig) {
        return new RestPostRequestor<Boolean, HttpProxyConfig>(DIAGNOSTICS_URI, Boolean.class)
            .invoke(httpProxyConfig);
    }

    /**
     * Return the default {@link ComponentProperties} for the given component type.
     *
     * @param componentType the component type for which to fetch the default ComponentProperties
     * @return a {@link ComponentProperties} object containing all default configuration properties for the given
     * component type.
     */
    @Nonnull
    public ComponentProperties getComponentDefaultProperties(String componentType) {
        return new RestGetRequestor<ComponentProperties>(COMPONENT_DEFAULT_PROPERTIES_URI,
            ComponentProperties.class).invoke(componentType);
    }

    /**
     * Return the local {@link ComponentProperties} for the given component type.
     * The local properties override the default properties for the corresponding component type.
     *
     * @param componentType the component type for which to fetch the local ComponentProperties
     * @return a {@link ComponentProperties} object containing all local configuration properties for the given
     * component type.
     */
    @Nonnull
    public ComponentProperties getComponentLocalProperties(String componentType) {
        return new RestGetRequestor<ComponentProperties>(COMPONENT_LOCAL_PROPERTIES_URI,
            ComponentProperties.class).invoke(componentType);
    }

    /**
     * Return the {@link ComponentProperties} for the given component instance / type. The result
     * map of properties is a merge of default values (came from component type) and specific values
     * (came from instance itself) with the priority of instance-level values.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentProperties getEffectiveInstanceProperties(String componentType,
                                                                 String componentInstanceId) {
        return new RestGetRequestor<ComponentProperties>(COMPONENT_INSTANCE_EFFECTIVE_PROPERTIES_URI,
            ComponentProperties.class)
            .invoke(componentType, componentInstanceId);
    }

    /**
     * Return the {@link ComponentProperties} for the given component instance / type. The result
     * map of properties is a merge of default values (came from component type) and specific values
     * (came from instance itself) with the priority of instance-level values.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentProperties getComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId) {
        return new RestGetRequestor<ComponentProperties>(COMPONENT_INSTANCE_PROPERTIES_URI, ComponentProperties.class)
                .invoke(componentType, componentInstanceId);
    }

    /**
     * Replace the {@link ComponentProperties} for the given component instance / type.
     * Return the updated {@link ComponentProperties}. The properties will replace current
     * set of properties for the instance. So, if empty map of properties is specified, this will
     * remove all the properties of this instance.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @param updatedProperties the new configuration property values to be saved.
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentProperties putComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId,
                                                                 ComponentProperties updatedProperties) {
        return new RestPutRequestor<ComponentProperties, ComponentProperties>(
                COMPONENT_INSTANCE_PROPERTIES_URI, ComponentProperties.class)
                .invoke(updatedProperties, componentType, componentInstanceId);
    }

    /**
     * Return the value for the given property for the given component type.
     * If there is no property by that name, then return null.
     *
     * @param componentType the type of component to query
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return value of the configureation property for the given component type
     */
    public String getComponentDefaultProperty(String componentType, String propertyName) {
        return new RestGetRequestor<String>(COMPONENT_DEFAULT_PROPERTY_URI, String.class)
                .invoke(componentType, propertyName);
    }

    /**
     * Return the local override value for the given property for the given component type.
     * If there is no override property by that name, then return null.
     *
     * @param componentType the type of component to fetch properties from
     * @param propertyName the current value of the named configuration property,
     *                     or null if there is none.
     * @return value of the current configuration property for the given component type
     */
    public String getComponentLocalProperty(String componentType, String propertyName) {
        return new RestGetRequestor<String>(COMPONENT_LOCAL_PROPERTY_URI, String.class)
                .invoke(componentType, propertyName);
    }

    /**
     * Return the value for the given property for the given component instance. If there is no property by that name,
     * then return null.
     *
     * @param componentType the component type of the given component instance.
     * @param componentInstanceId the unique id of the given component instance.
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return the updated value of the configureation property for the given component instance
     */
    public String getComponentInstanceProperty(String componentType,
                                               String componentInstanceId,
                                               String propertyName) {
        return new RestGetRequestor<String>(COMPONENT_INSTANCE_PROPERTY_URI, String.class)
                .invoke(componentType, componentInstanceId, propertyName);
    }

    /**
     * Set the default configuration properties for a single Component Type. The properties
     * given in this call replace all previous default configuration for the component.
     * If the component is not previously known, a new component configuration will be added.
     * <p/>
     * Note that this function is *not* required by the external REST API currently, and so
     * is not declared in the ClusterMgrRestClient interface.
     *
     * @param componentType the component type
     * @param updatedProperties the default configuration properties for this component type; the
     *                          previous configuration properties will all be replaced.
     * @return the newly updated default configuration properties for this component type
     */
    public ComponentProperties putComponentDefaultProperties(String componentType,
                                                                ComponentProperties updatedProperties) {
        return new RestPutRequestor<ComponentProperties, ComponentProperties>(
            COMPONENT_DEFAULT_PROPERTIES_URI, ComponentProperties.class)
                .invoke(updatedProperties, componentType);
    }

    /**
     * Replace the local {@link ComponentProperties} for the given component type.
     * Return the updated {@link ComponentProperties}. The properties will replace the local
     * set of properties for the instance. So, if empty map of properties is specified, this will
     * remove all the local properties of this instance.
     *
     * @param componentType type for the given component instance.
     * @param updatedProperties the new configuration property values to be saved.
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentProperties putLocalComponentProperties(String componentType,
                                                                    ComponentProperties updatedProperties) {
        return new RestPutRequestor<ComponentProperties, ComponentProperties>(
            COMPONENT_LOCAL_PROPERTIES_URI, ComponentProperties.class)
            .invoke(updatedProperties, componentType);
    }

    /**
     * Utility class to perform an HTTP GET. The constructor takes a URI string as a parameter, which
     * may include substitution parameters. The field "uriBase" is prepended to the given URI string.
     *
     * <p>The .invoke() call accepts parameters to be substituted into the URI.
     *
     * <p>Note: the status code from the http request is not checked.
     *
     * @param <T> the type of the return value
     */
    private class RestGetRequestor<T> {
        private final String uri;
        private final Class clz;

        RestGetRequestor(String uri, Class resultClass ) {
            this.uri = uriBase + uri;
            clz = resultClass;
            logger.debug("construct RestGetRequestor");
        }

        @Nonnull
        T invoke(@Nonnull String... parameters) {
            logger.debug("invoke GET requestor URI:{} parameters...{} resultClass: {}", this.uri, parameters, clz);
            final UriBuilder uriBuilder = UriBuilder.fromPath(this.uri);
            final URI uri = uriBuilder.build(parameters);
            final RestTemplate restTemplate = getRestTemplate();
            final ResponseEntity<T> response = restTemplate.exchange(uri,
                    HttpMethod.GET, null, clz);

        // todo - check the status code
            return response.getBody();
        }
    }

    /**
     * Utility class to perform an HTTP PUT. The constructor takes a URI string as a parameter, which
     * may include substitution parameters. The field "uriBase" is prepended to the given URI string.
     *
     * <p>The .invoke() call accepts a data object of type U to be "put", and parameters to be substituted into the URI.
     *
     * <p>Note: the status code from the http request is not checked.
     *
     * @param <T> the type of the return value
     * @param <U> the type of the object to be "PUT"
     */
    private class RestPutRequestor<T, U> {
        private final String uri;
        private final Class clz;

        RestPutRequestor(String uri, Class resultClass) {
            this.uri = uriBase + uri;
            this.clz = resultClass;
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        T invoke(@Nonnull U requestObject, @Nonnull Object... parameters) {
            logger.debug("invoke PUT requestor requestObject: {}   URI:{} parameters...{} resultClass: {}",
                    requestObject, this.uri, parameters, clz);
            final RestTemplate restTemplate = getRestTemplate();
            final ResponseEntity<T> response = restTemplate.exchange(UriBuilder.fromPath(uri).build(parameters),
                    HttpMethod.PUT, new HttpEntity<>(requestObject), clz);
            return response.getBody();
        }

        void invokeVoid(@Nonnull U requestObject, @Nonnull Object... parameters) {
            getRestTemplate().put(uri, requestObject, parameters);
        }
    }

    /**
     * Utility class to perform an HTTP POST. The constructor takes a URI string as a parameter,
     * which may include substitution parameters. The field "uriBase" is prepended to the
     * given URI string.
     *
     * <p>The .invoke() call accepts a data object of type U to be "POST", and parameters to be
     * substituted into the URI.
     *
     * <p>Note: the status code from the http request is not checked.
     *
     * @param <T> the type of the return value
     * @param <U> the type of the object to be "POST"
     */
    private class RestPostRequestor<T, U> {
        private final String uri;
        private final Class clz;

        RestPostRequestor(String uri, Class resultClass) {
            this.uri = uriBase + uri;
            this.clz = resultClass;
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        T invoke(@Nonnull U requestObject, @Nonnull Object... parameters) {
            logger.debug("invoke POST requestor requestObject: {}   URI:{} parameters...{} resultClass: {}",
                requestObject, this.uri, parameters, clz);
            final RestTemplate restTemplate = getRestTemplate();
            final ResponseEntity<T> response = restTemplate.exchange(UriBuilder.fromPath(uri).build(parameters),
                HttpMethod.POST, new HttpEntity<>(requestObject), clz);
            return response.getBody();
        }
    }

    /**
     * Utility class to perform an HTTP DELETE. The constructor takes a URI string as a parameter, which
     * may include substitution parameters. The field "uriBase" is prepended to the given URI string.
     *
     * <p>The .invoke() accepts parameters to be substituted into the URI.
     *
     * <p>Note: the status code from the http request is not checked.
     * @param <T> the type of the return value
     */
    private class RestDeleteRequestor<T> {
        private final String uri;
        private final Class clz;

        RestDeleteRequestor(String uri, Class resultClass) {
            this.uri = uriBase + uri;
            this.clz = resultClass;
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        T invoke(@Nonnull Object... parameters) {
            logger.trace("invoke DELETE requestor  URI:{} parameters...{} resultClass: {}",
                this.uri, parameters, clz);
            logger.trace(UriBuilder.fromPath(uri).build(parameters));
            final RestTemplate restTemplate = getRestTemplate();
            final ResponseEntity<T> response = restTemplate.exchange(
                UriBuilder.fromPath(uri).build(parameters),
                HttpMethod.DELETE, HttpEntity.EMPTY, clz);
            return response.getBody();
        }
    }

}
