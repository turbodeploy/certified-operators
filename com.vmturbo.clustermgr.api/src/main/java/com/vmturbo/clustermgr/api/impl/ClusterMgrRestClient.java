package com.vmturbo.clustermgr.api.impl;

import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.core.UriBuilder;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import com.google.common.io.ByteStreams;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

/**
 * Wrapper for the REST API for the ClusterMgr Component.
 **/
class ClusterMgrRestClient extends ComponentRestClient implements IClusterService {

    private static final String REST_API_PREFIX = "";

    // the URI to access the entire cluster state
    private static String CLUSTER_CONFIG_URI = "";

    // the current state of the component instances
    private static String COMPONENT_INSTANCES_STATE_URI = "/state";

    // fetch the diagnostics from all the running instances
    private static String DIAGNOSTICS_URI = "/diagnostics";

    // configuration for the various components
    private static String KNOWN_COMPONENTS_URI = "/components";
    private static String TELEMETRY_INITIALIZED = "/proactive/initialized";
    private static String TELEMETRY_ENABLED = "/proactive/enabled";
    private static String COMPONENT_INSTANCE_PROPERTIES_URI = "/components/{componentType}/instances/{instanceId}/properties";
    private static String COMPONENT_INSTANCE_PROPERTY_URI = "/components/{componentType}/instances/{instanceId}/properties/{propertyName}";
    private static String COMPONENT_TYPE_DEFAULT_PROPERTY_URI =  "/components/{componentType}/defaults/{propertyName}";
    private static String COMPONENT_TYPE_DEFAULTS_URI = "/components/{componentType}/defaults";
    private static String COMPONENT_TYPE_INSTANCE_IDS_URI = "/components/{componentType}/instances";
    private static String COMPONENT_INSTANCE_NODE_URI = "/components/{componentType}/instances/{instanceId}/node";

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
    @Override
    public boolean isXLEnabled() {
        return true;
    }

    @Override
    public boolean isTelemetryInitialized() {
        return new RestGetRequestor<Boolean>(TELEMETRY_INITIALIZED, Boolean.class).invoke();
    }

    /**
     * Indicates whether the telemetry is enabled.
     *
     * @return {@code true} iff telemetry is enabled.
     */
    @Override
    public boolean isTelemetryEnabled() {
        return new RestGetRequestor<Boolean>(TELEMETRY_ENABLED, Boolean.class).invoke();
    }

    /**
     * Sets the telemetry enabled flag.
     *
     * @param enabled The telemetry enabled flag.
     */
    @Override
    public void setTelemetryEnabled(boolean enabled) {
        new RestPutRequestor<Void, Boolean>(TELEMETRY_ENABLED, Boolean.class).invokeVoid(enabled, enabled);
    }

    @Override
    @Nonnull
    public ClusterConfigurationDTO getClusterConfiguration() {
        return new RestGetRequestor<ClusterConfigurationDTO>(CLUSTER_CONFIG_URI, ClusterConfigurationDTO.class)
                .invoke();
    }

    @Override
    @Nonnull
    public ClusterConfigurationDTO setClusterConfiguration(@Nonnull ClusterConfigurationDTO newConfiguration) {
        return new RestPutRequestor<ClusterConfigurationDTO, ClusterConfigurationDTO>(CLUSTER_CONFIG_URI,
                    ClusterConfigurationDTO.class)
                .invoke(newConfiguration);
    }

    @Override
    @Nonnull
    public Set<String> getKnownComponents() {
        return new RestGetRequestor<Set<String>>(KNOWN_COMPONENTS_URI, Set.class).invoke();
    }

    @Override
    public String setPropertyForComponentInstance(String componentType,
                                                  String instanceId,
                                                  String propertyName,
                                                  String propertyValue) {
        return new RestPutRequestor<String, String>(COMPONENT_INSTANCE_PROPERTY_URI, String.class)
                .invoke(propertyValue, componentType, instanceId, propertyName);
    }

    @Override
    public Set<String> getComponentInstanceIds(String componentType) {
        return new RestGetRequestor<Set<String>>(COMPONENT_TYPE_INSTANCE_IDS_URI, Set.class)
                .invoke(componentType);
    }

    @Override
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
    @Override
    public void collectComponentDiagnostics(OutputStream responseOutput) {
        RequestCallback requestCallback = request -> request.getHeaders()
                .setAccept(Arrays.asList(
                        new MediaType("application", "zip"),
                        MediaType.APPLICATION_OCTET_STREAM,
                        MediaType.ALL));
        ResponseExtractor copyResponseToOutputStream = response -> {

            ByteStreams.copy(response.getBody(), responseOutput);
            return null;
        };
        getStreamingRestTemplate().execute(uriBase + DIAGNOSTICS_URI,
                HttpMethod.GET,
                requestCallback,
                copyResponseToOutputStream);
    }


    @Override
    public String getNodeForComponentInstance(String componentType,
                                              String instanceId) {
        return new RestGetRequestor<String>(COMPONENT_INSTANCE_NODE_URI, String.class)
                .invoke(componentType, instanceId);
    }

    @Override
    public String setNodeForComponentInstance(String componentType, String instanceId,
                                              String nodeName) {
        return new RestPutRequestor<String, String>(COMPONENT_INSTANCE_NODE_URI, String.class)
                .invoke(nodeName, componentType, instanceId);
    }

    @Override
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(String componentType) {
        return new RestGetRequestor<ComponentPropertiesDTO>(COMPONENT_TYPE_DEFAULTS_URI, ComponentPropertiesDTO.class)
                .invoke(componentType);
    }

    @Override
    public ComponentPropertiesDTO getComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId) {
        return new RestGetRequestor<ComponentPropertiesDTO>(COMPONENT_INSTANCE_PROPERTIES_URI, ComponentPropertiesDTO.class)
                .invoke(componentType, componentInstanceId);
    }

    @Override
    public ComponentPropertiesDTO putComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId,
                                                                 ComponentPropertiesDTO updatedProperties) {
        return new RestPutRequestor<ComponentPropertiesDTO, ComponentPropertiesDTO>(
                COMPONENT_INSTANCE_PROPERTIES_URI, ComponentPropertiesDTO.class)
                .invoke(updatedProperties, componentType, componentInstanceId);
    }

    @Override
    public String getComponentTypeProperty(String componentType, String propertyName) {
        return new RestGetRequestor<String>(COMPONENT_TYPE_DEFAULT_PROPERTY_URI, String.class)
                .invoke(componentType, propertyName);
    }

    @Override
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
     * is not declared in the ICLusterService interface.
     *
     * @param componentType the component type
     * @param updatedProperties the default configuration properties for this component type; the
     *                          previous configuration properties will all be replaced.
     * @return the newly updated default configuration properties for this component type
     */
    public ComponentPropertiesDTO putComponentDefaultProperties(String componentType,
                                                                ComponentPropertiesDTO updatedProperties) {
        return new RestPutRequestor<ComponentPropertiesDTO, ComponentPropertiesDTO>(
                COMPONENT_TYPE_DEFAULTS_URI, ComponentPropertiesDTO.class)
                .invoke(updatedProperties, componentType);
    }

    /**
     * Utility class to perform an HTTP GET. The constructor takes a URI string as a parameter, which
     * may include substitution parameters. The field "uriBase" is prepended to the given URI string.
     * <p>
     * The .invoke() call accepts parameters to be substituted into the URI.
     * <p>
     * Note: the status code from the http request is not checked.
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
            final URI uri = uriBuilder
                    .build(parameters);
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
     * <p>
     * The .invoke() call accepts a data object of type U to be "put", and parameters to be substituted into the URI.
     * <p>
     * Note: the status code from the http request is not checked.
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

}
