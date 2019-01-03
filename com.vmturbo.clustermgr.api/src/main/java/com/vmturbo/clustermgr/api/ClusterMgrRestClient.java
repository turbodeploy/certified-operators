package com.vmturbo.clustermgr.api;

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
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

/**
 * Wrapper for the REST API for the ClusterMgr Component.
 **/
public class ClusterMgrRestClient extends ComponentRestClient {

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
     * Populate a ClusterConfiguration object with the current definition: all known component types, with the component
     * instances and configuration properties for each;
     * plus a list of all known component types with their default configuration property values.
     *
     * @return an aggregate ClusterConfiguration populated with node/component/configuration and component/default-configuration
     * values.
     */
    @Nonnull
    public ClusterConfigurationDTO getClusterConfiguration() {
        return new RestGetRequestor<ClusterConfigurationDTO>(CLUSTER_CONFIG_URI, ClusterConfigurationDTO.class)
                .invoke();
    }

    /**
     * Replace the entire Cluster Configuration.
     * This includes the <strong>default properties</strong> {@link ComponentPropertiesDTO}
     * (component-type -> default properties)
     * and the <strong>instance properties</strong> {@link ComponentPropertiesDTO}
     * (instance-id -> instance properties)
     *
     * @param newConfiguration an {@link ClusterConfigurationDTO} to completely replace the current configuration.
     * @return the new configuration, read back from the Consul key/value store.
     */
    @Nonnull
    public ClusterConfigurationDTO setClusterConfiguration(@Nonnull ClusterConfigurationDTO newConfiguration) {
        return new RestPutRequestor<ClusterConfigurationDTO, ClusterConfigurationDTO>(CLUSTER_CONFIG_URI,
                    ClusterConfigurationDTO.class)
                .invoke(newConfiguration);
    }

    /**
     * Fetch the set of Components known to VMTurbo from the Consul K/V store.
     * Components are "known" if there is a configuration key "{@code /vmturbo/components/{component-name}/}".
     * <p>
     * If no matching configuration keys are found, then the global key/value store is initialized from the
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
     * Set the value for the given property on the given component instance. Based on the semantics of Consul, any
     * previous value for this property will be overwritten, and if the property did not exist before then it is created.
     *
     * The new value may be null, which effectively deletes this property. The "customer" will need to provide a suitable
     * default. This will help us implement a "search path" of configuration properties.
     *
     * Returns the new value. Note that there is a write/read with no locking, and so the value returned may
     * differ if there has been an intervening write.
     *
     * See the {@literal COMPONENT_INSTANCE_PROPERTY_FORMAT} format for the full key to be used in Consul.
     *
     * @param componentType the type of the given component instance
     * @param instanceId the unique id of the given component instance
     * @param propertyName the name of the configuration property to set
     * @param propertyValue the new value for the given configuration property
     * @return the new value of the configuration property
     */
    public String setPropertyForComponentInstance(String componentType,
                                                  String instanceId,
                                                  String propertyName,
                                                  String propertyValue) {
        return new RestPutRequestor<String, String>(COMPONENT_INSTANCE_PROPERTY_URI, String.class)
                .invoke(propertyValue, componentType, instanceId, propertyName);
    }

    /**
     * Look up a set of currently configured Component Instance IDs belonging to a given Component Type.
     *
     * Each Instance Id represents a VMT Component instance to be launched as part of the OpsMgr Cluster
     *
     * @param componentType the component type to which the answer Instance IDs belong
     * @return a set of Component Instance ID strings as configured in the current OpsMgr
     */
    public Set<String> getComponentInstanceIds(String componentType) {
        return new RestGetRequestor<Set<String>>(COMPONENT_TYPE_INSTANCE_IDS_URI, Set.class)
                .invoke(componentType);
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
        ResponseExtractor copyResponseToOutputStream = response -> {

            ByteStreams.copy(response.getBody(), responseOutput);
            return null;
        };
        getStreamingRestTemplate().execute(uriBase + DIAGNOSTICS_URI,
                HttpMethod.GET,
                requestCallback,
                copyResponseToOutputStream);
    }


    /**
     * Return the current Cluster Node name assigned to this VMT Component Instance.
     *
     * @param componentType type of the component to which component is assigned
     * @param instanceId the id of the VMT Component Instance to look up.
     * @return the node name on which this component should be run.
     */
    public String getNodeForComponentInstance(String componentType,
                                              String instanceId) {
        return new RestGetRequestor<String>(COMPONENT_INSTANCE_NODE_URI, String.class)
                .invoke(componentType, instanceId);
    }

    /**
     * Store the name of the current cluster node for the given component instance / type.
     * The new cluster node name will be returned. Note that there is no locking for this write/read operation, so
     * if another request overlaps this one the value returned may not reflect the given cluster node.
     *
     * @param componentType the type of the given component
     * @param instanceId the unique id of the given component instance
     * @param nodeName the name of the cluster node on which this component should run
     * @return the cluster node name for this component instance
     */
    @Nonnull
    public String setNodeForComponentInstance(String componentType, String instanceId,
                                              String nodeName) {
        return new RestPutRequestor<String, String>(COMPONENT_INSTANCE_NODE_URI, String.class)
                .invoke(nodeName, componentType, instanceId);
    }

    /**
     * Return the default {@link ComponentPropertiesDTO} for the given component type.
     *
     * @param componentType the component type for which to fetch the default ComponentProperties
     * @return a {@link ComponentPropertiesDTO} object containing all default configuration properties for the given
     * component type.
     */
    @Nonnull
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(String componentType) {
        return new RestGetRequestor<ComponentPropertiesDTO>(COMPONENT_TYPE_DEFAULTS_URI, ComponentPropertiesDTO.class)
                .invoke(componentType);
    }

    /**
     * Return the {@link ComponentPropertiesDTO} for the given component instance / type. The result
     * map of properties is a merge of default values (came from component type) and specific values
     * (came from instance itself) with the priority of instance-level values.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentPropertiesDTO} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentPropertiesDTO getComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId) {
        return new RestGetRequestor<ComponentPropertiesDTO>(COMPONENT_INSTANCE_PROPERTIES_URI, ComponentPropertiesDTO.class)
                .invoke(componentType, componentInstanceId);
    }

    /**
     * Replace the {@link ComponentPropertiesDTO} for the given component instance / type.
     * Return the updated {@link ComponentPropertiesDTO}. The properties will replace current
     * set of properties for the instance. So, if empty map of properties is specified, this will
     * remove all the properties of this instance.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @param updatedProperties the new configuration property values to be saved.
     * @return a {@link ComponentPropertiesDTO} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentPropertiesDTO putComponentInstanceProperties(String componentType,
                                                                 String componentInstanceId,
                                                                 ComponentPropertiesDTO updatedProperties) {
        return new RestPutRequestor<ComponentPropertiesDTO, ComponentPropertiesDTO>(
                COMPONENT_INSTANCE_PROPERTIES_URI, ComponentPropertiesDTO.class)
                .invoke(updatedProperties, componentType, componentInstanceId);
    }

    /**
     * Return the value for the given property for the given component type. If there is no property by that name,
     * then return null.
     *
     * See the {@literal COMPONENT_INSTANCE_PROPERTY_FORMAT} format for the full key to be used in Consul.
     *
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return value of the configureation property for the given component type
     */
    public String getComponentTypeProperty(String componentType, String propertyName) {
        return new RestGetRequestor<String>(COMPONENT_TYPE_DEFAULT_PROPERTY_URI, String.class)
                .invoke(componentType, propertyName);
    }

    /**
     * Return the value for the given property for the given component instance. If there is no property by that name,
     * then return null.
     *
     * See the {@literal COMPONENT_INSTANCE_PROPERTY_FORMAT} format for the full key to be used in Consul.
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
