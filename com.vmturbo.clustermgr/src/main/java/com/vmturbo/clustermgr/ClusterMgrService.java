package com.vmturbo.clustermgr;

import static com.vmturbo.clustermgr.ClusterMgrConfig.TELEMETRY_ENABLED;
import static com.vmturbo.clustermgr.ClusterMgrConfig.TELEMETRY_LOCKED;
import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;
import static com.vmturbo.clustermgr.api.ClusterMgrClient.UNKNOWN_VERSION_STRING;
import static com.vmturbo.components.common.BaseVmtComponent.PROP_INSTANCE_IP;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.NotFoundException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.MediaType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.kv.Value;

import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.utils.Strings;

/**
 * Implement the ClusterMgr Services: component status, component configuration, node configuration.
 * This implementation uses the Consul key/value service API.
 * <p>
 * Consul key structure:
 * {@code
 * vmturbo/components/{component-type}/defaults/properties/{property-name} = default-property-value
 * vmturbo/components/{component-type}/instances/{instance_id}/properties/{property-name} = property-value
 * vmturbo/components/{component-type}/instances/{instance_id}/node = {node id where instance runs}
 * }
 * <p>
 * Consul values may be any byte string, with a size limit of 512kB.
 ***/
public class ClusterMgrService {
    // Consul key path component names to implement the Component/Node/Properties schema above
    private static final Character CONSUL_PATH_SEPARATOR = '/';
    private static final String VMTURBO_BASE_FORMAT = "vmturbo" + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENTS_BASE_FORMAT = VMTURBO_BASE_FORMAT + "components" + CONSUL_PATH_SEPARATOR;
    // %s = component type
    private static final String COMPONENT_FORMAT = COMPONENTS_BASE_FORMAT + "%s" + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENT_DEFAULTS_FORMAT = COMPONENT_FORMAT + "defaults" + CONSUL_PATH_SEPARATOR;
    // %s = property name
    private static final String COMPONENT_DEFAULTS_PROPERTY_FORMAT = COMPONENT_DEFAULTS_FORMAT + "%s";

    private static final String COMPONENT_INSTANCES_FORMAT = COMPONENT_FORMAT + "instances" + CONSUL_PATH_SEPARATOR;
    // %s = component instance id
    private static final String COMPONENT_INSTANCE_FORMAT = COMPONENT_FORMAT + "instances" + CONSUL_PATH_SEPARATOR + "%s"
            + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENT_INSTANCE_PROPERTIES_FORMAT = COMPONENT_INSTANCE_FORMAT + "properties"
            + CONSUL_PATH_SEPARATOR;
    // %s = property name
    private static final String COMPONENT_INSTANCE_PROPERTY_FORMAT = COMPONENT_INSTANCE_PROPERTIES_FORMAT + "%s";
    private static final String COMPONENT_INSTANCE_NODE_FORMAT = COMPONENT_INSTANCE_FORMAT + "node";
    private static final Character PROPERTY_KEY_SEPARATOR = '.';

    // split on the path separator for Consul keys: '/'
    private static final Splitter PATH_SEP_LIST_SPLITTER = Splitter.on(CONSUL_PATH_SEPARATOR)
            .omitEmptyStrings();

    // execution node name to use if none is specified.
    private static final String DEFAULT_NODE_NAME = "default";
    private static final String SERVICE_RESTART_REQUEST = "/service/restart";

    // constant values used for parsing consul health check results
    private static final String CONSUL_HEALTH_CHECK_PASSING_RESULT = "passing";
    private static final String CONSUL_HEALTH_CHECK_UNSUCCESSFUL_FRAGMENT = "no such host";
    private static final String GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH =
        "config/global_defaults.properties";
    public static final String HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP =
        "/home/turbonomic/data/turbonomic-diags-_%d.zip";
    public static final String COLLON = ":";
    public static final String AT = "@";


    /**
     * The socket timeout in milliseconds, which is the timeout for
     * waiting for data  or, put differently, a maximum period inactivity
     * between two consecutive data packets).
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    private static final int SOCKET_TIMEOUT = 10 * 60 * 1000;

    /**
     * Timeout in milliseconds until a connection is established.
     * A timeout value of zero is interpreted as an infinite timeout.
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    private static final int CONNECT_TIMEOUT = 10 * 1000;

    /**
     * Timeout in milliseconds used when requesting a connection
     * from the connection manager. A timeout value of zero is interpreted
     * as an infinite timeout.
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final String NEXT_LINE = "\n";

    private static final Predicate<String> IS_SERVICE_PREDICATE = name -> !name.startsWith("mediation");

    @VisibleForTesting
    static final String DIAGS_SUMMARY_SUCCESS_TXT = "DiagsSummary-Success.txt";

    @VisibleForTesting
    static final String DIAGS_SUMMARY_FAIL_TXT = "DiagsSummary-Fail.txt";

    // Custom diags collecting request configuration
    private final RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(CONNECT_TIMEOUT)
        .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
        .setSocketTimeout(SOCKET_TIMEOUT).build();

    // Default configuration properties global to all component types
    private final ComponentProperties globalDefaultProperties = new ComponentProperties();
    private final OsCommandProcessRunner osCommandProcessRunner;

    @VisibleForTesting
    final static String CURL_COMMAND = "curl";
    @VisibleForTesting
    final static String UPLOAD_VMTURBO_COM_URL =
        "http://upload.vmturbo.com/appliance/cgi-bin/vmtupload.cgi";

    private Logger log = LogManager.getLogger();

    /**
     * The {@link ConsulService} handles the details of the accessing the Key/Value store and
     * Service Registration function.
     */
    private final ConsulService consulService;

    private final AtomicBoolean kvInitialized = new AtomicBoolean(false);

    public ClusterMgrService(@Nonnull final ConsulService consulService,
                             @Nonnull final OsCommandProcessRunner osCommandProcessRunner) {
        this.consulService = Objects.requireNonNull(consulService);
        this.osCommandProcessRunner = osCommandProcessRunner;
        loadGlobalDefaultProperties();
    }

    /**
     * Read the Global Default Configuration properties from the .properties file.
     * These properties will be used for setting Default Configuration properties for
     * each component when they register. This method is called during a static {} initializer.
     */
    @VisibleForTesting
    void loadGlobalDefaultProperties() {
        Properties globalDefaultsFromFile = new Properties();
        log.info("Loading global default properties from file: {}",
                GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH);
        try (final InputStream configPropertiesStream = ClusterMgrService.class.getClassLoader()
                .getResourceAsStream(GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH)) {
            if (configPropertiesStream == null) {
                throw new RuntimeException("Cannot find default configuration properties file: " +
                        GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH, new FileNotFoundException());
            }
            globalDefaultsFromFile.load(configPropertiesStream);
        } catch (IOException e) {
            // if the component defaults cannot be found we still need to send an empty
            // default properties to ClusterMgr where the global defaults will be used.
            throw new RuntimeException("Cannot read global default configuration properties file:" +
                    GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH, e);
        }
        // clear any previous global defaults and then populate the defaults from the file
        synchronized (globalDefaultProperties) {
            globalDefaultProperties.clear();
            globalDefaultsFromFile.forEach((defaultKey, defaultValue) ->
                    globalDefaultProperties.put(defaultKey.toString(), defaultValue.toString()));
        }

        // Mark the KV store as initialized.
        kvInitialized.set(true);

        // log the result
        globalDefaultProperties.forEach((key, value) -> log.info("   {} = >{}<", key, value));
    }

    public boolean isClusterKvStoreInitialized() {
        return kvInitialized.get();
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
        String compositeKey = getComponentsBaseKey();
        return getComponentsWithPrefix(compositeKey);
    }

    /**
     * Instantiate a new instance of a given component type. This is accomplished simply by
     * <ol>
     * <li>creating a unique ID for this component instance. The current unique ID scheme is to
     * append a sequence number to the component type name, e.g. "mediation-hyperv-1".
     *
     * <p>
     * <strong>WARNING</strong> - the sequence number is ALWAYS "1". This means that we only support a single
     * instance of each component type. The salvation is - there are no VMT Components that are currently planning to
     * support multiple instances.
     * <p>
     * TODO: use Consul locking and k/v store to implement a cluster-wide unique ID, e.g. using a monotonically
     * increasing integer. For a discussion of Consul locking see:  https://www.consul.io/docs/guides/semaphore.html
     * </li>
     * <li>use the unique ID for the new component to create a key/value prefix for this component instance of the
     * form {@literal COMPONENT_INSTANCE_FORMAT} and put a "null" in the key/value store for this key.
     * The key/value prefix must be unique among all VMT Components of the given type.
     * </li>
     * </ol>
     *
     * @param componentType name of the VMT Component type of the new instance
     * @return the unique ID of the new instance or {@code null} if the instance already exist
     */
    private void addComponentInstance(String componentType, String componentInstanceId) {
        String newInstanceKey = getComponentInstanceKey(componentType, componentInstanceId);
        // check for previously existing component instance with this ID. TODO: this should be done with locking
        try {
            if (!consulService.keyExist(newInstanceKey)) {
                consulService.putValue(newInstanceKey);
            }
        } catch (NotFoundException e) {
            // Could return 404.
            log.info("The component instance " + newInstanceKey + " is not yet found.");
        }
    }

    /**
     * Set all the property values for a given component instance given the component type, instance id, and a
     * {@link ComponentProperties} object containing the property key/value pairs.
     * Note: this removes any previous default property values.
     *
     * @param componentType type of the given component instance
     * @param newInstanceId unique instance id of the given component instance
     * @param componentProperties a {@link ComponentProperties} map of key/value pairs to set.
     */
    private void setComponentInstanceProperties(String componentType, String newInstanceId, ComponentProperties componentProperties) {
        String valueKeyPrefix = getComponentInstancePropertiesKey(componentType, newInstanceId);
        try {
            removeAllSubKeys(valueKeyPrefix);
        } catch (NotFoundException e) {
            log.info("No subkeys are found for " + valueKeyPrefix);
        }
        setAllValues(valueKeyPrefix, componentProperties);
    }

    /**
     * Establish a new component type. This is accomplished simply by creating a key prefix in the consul
     * k/v store - see {@code COMPONENT_FORMAT} for the format of this key prefix.
     *
     * Note that there is no check for a previous component type by the same name
     *
     * @param componentType the new component type to be established.
     */
    private void addComponentType(String componentType) {
        String componentPrefix = getComponentKey(componentType);
        consulService.putValue(componentPrefix);
    }

    /**
     * Remove all child key/value pairs in the Consul Key/Value store below the given keyPrefix.
     * The keyPrefix value must end with the {@code CONSUL_PATH_SEPARATOR}.
     * @param keyPrefix the key string below which all key/value pairs will be removed.
     */
    private void removeAllSubKeys(String keyPrefix) {
        validateKeyPrefix(keyPrefix);
        consulService.deleteKey(keyPrefix);

    }

    /**
     * Store all key/value pairs in the Consul Key/Value store, where each is key prepended by the given
     * keyPrefix value. The keyPrefix value must end with the {@code CONSUL_PATH_SEPARATOR}.
     *
     * @param keyPrefix the string to be pre-pended to each key in the keyValuePairs to yield the consul key to be used.
     * @param keyValuePairs a {@link ComponentProperties} map of property name / value pairs to be stored.
     */
    private void setAllValues(String keyPrefix, ComponentProperties keyValuePairs) {
        validateKeyPrefix(keyPrefix);
        for (Map.Entry<String, String> keyValue :  keyValuePairs.entrySet()) {
            consulService.putValue(keyPrefix + keyValue.getKey(), keyValue.getValue());
        }
        for (String fullKeyName : consulService.getKeys(keyPrefix)) {
            final String propertyName = StringUtils.removeStart(fullKeyName, keyPrefix);
            if (!keyValuePairs.containsKey(propertyName)) {
                consulService.deleteKey(fullKeyName);
            }
        }
    }

    private void validateKeyPrefix(String keyPrefix) {
        if (keyPrefix.charAt(keyPrefix.length()-1) != CONSUL_PATH_SEPARATOR) {
            throw new RuntimeException("Value keyPrefix does not end with CONSUL_PATH_SEPARATOR: "
                    + CONSUL_PATH_SEPARATOR);
        }
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
    public String setPropertyForComponentInstance(
            String componentType,
            String instanceId,
            String propertyName,
            String propertyValue) {
        String instancePropertiesKey = getComponentInstancePropertyKey(componentType, instanceId, propertyName);
        setComponentKeyValue(instancePropertiesKey, propertyValue);
        return getComponentKeyValue(instancePropertiesKey);
    }

    /**
     * Look up Consul keys with a given prefix and exactly one additional component. Return an empty set if none found.
     * <p>
     * Note: the prefix MUST end in the CONSUL_PATH_SEPARATOR ('/').
     * Example: keyPrefix = "a/b/c/" matches: "a/b/c/d", "a/b/c/e" but not "a/b/c/x/y" and returns ["d", "e"].
     *
     * @param keyPrefix a string giving a prefix of keys to examine.
     * @return a set of the final path component for all matching keys, or an empty list if there is are non found
     */
    @Nonnull
    private Set<String> getComponentsWithPrefix(@Nonnull String keyPrefix) {
        if (!keyPrefix.endsWith(String.valueOf(CONSUL_PATH_SEPARATOR))) {
            throw new IllegalArgumentException("Consul key prefix MUST end with '" + CONSUL_PATH_SEPARATOR
                    + "', not: " + keyPrefix);
        }
        Set<String> answer = new HashSet<>();
        List<Value> valuesMatchinKeyPrefix = new ArrayList<>();
        try {
            valuesMatchinKeyPrefix = consulService.getValues(keyPrefix);
        } catch (NotFoundException e) {
            log.trace("empty result retrieving keys with prefix: " + keyPrefix);
        }
        // keep only the key values where the remainder is exactly a single path component
        for (Value v : valuesMatchinKeyPrefix) {
            String key = v.getKey();
            String remainder = StringUtils.removeStart(key, keyPrefix);
            List<String> parts = PATH_SEP_LIST_SPLITTER.splitToList(remainder);
            if (parts.size() > 0) {
                answer.add(parts.get(0));
            }
        }
        return answer;
    }

    /**
     * Look up a set of Consul key/value pairs where the key matches the given prefix.  The key returned will have the
     * prefix stripped off. For example, if the prefix is "a/b/", then the key/value {@code {"a/b/c": "foo"}} will return
     * the map with entry {@code {"c": "foo"}}
     *
     * If there are no keys with the given prefix, then an empty {@link ComponentProperties}.
     *
     * @param keyPrefix the initial prefix to be matched against keys in the key/value store
     * @return a map of the matching keys to their respective values, with the keyPrefix removed from the resulting key.
     */
    @Nonnull
    private ComponentProperties getComponentPropertiesWithPrefix(@Nonnull String keyPrefix) {
        ComponentProperties answer = new ComponentProperties();
        List<Value> allValues;
        try {
            allValues = consulService.getValues(keyPrefix);
        } catch (NotFoundException e) {
            log.trace("empty result retrieving keys with prefix: " + keyPrefix);
            return new ComponentProperties();
        }
        for (Value v : allValues) {
            String key = v.getKey();
            if (StringUtils.startsWith(key, keyPrefix)) {
                String remainder = StringUtils.removeStart(key, keyPrefix);
                if (remainder.length() > 0) {
                    answer.put(remainder, v.getValueAsString().or(""));
                }
            }
        }
        return answer;
    }

    /**
     * Look up a single Consul Key/Value. The value will be returned, or null if the given key is not present.
     *
     * Note that the property value may be null.
     *
     * @param propertyKey the key to look up in the Consul Key/Value database
     * @return the value for the given key, or null if the key is not present
     */
    @Nullable
    private String getComponentKeyValue(String propertyKey) {
        return consulService.getValueAsString(propertyKey).orNull();
    }

    /**
     * Set a single Consul Key/Value.
     *
     * The propertyValue may be null. Clients will need to provide a default value as appropriate.
     * As a general rule, we treat configuration properties with a <i>null</i> value as indistinguishable from a
     * property with <i>no</i> value.
     *
     * @param propertyKey the key to look up in the Consul Key/Value database
     */
    private void setComponentKeyValue(@Nonnull String propertyKey, @Nullable String propertyValue) {
        consulService.putValue(propertyKey, propertyValue);
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
        return getComponentsWithPrefix(getComponentInstancesKey(componentType));
    }

    /**
     * Gather the current state from each running VMT Component Instance. We will do this by
     * mapping the Consul health status to a turbo component state
     *
     *    consul health result     XL component status
     *    --------------------     ------------------
     *    passing                  RUNNING
     *    critical - w/output      UNHEALTHY (presence of output means our check is working)
     *    crticial - w/o output    UNKNOWN (no output means we haven't reported a check result yet)
     *
     * @return a map of component_id -> status
     */
    public Map<String, ComponentState> getComponentsState() {
        long startTime = System.currentTimeMillis();

        Map<String, HealthCheck> healthChecks = getAllComponentsHealth();

        // map the resulting state based on the health state.
        Map<String, ComponentState> answer = new HashMap<>();
        for (Entry<String, HealthCheck> entry : healthChecks.entrySet()) {
            final HealthCheck checkResult = entry.getValue();
            // if there is no output, check has not succeeded yet -- component status is unknown or down.
            // if the check is "passing" then the component is RUNNING
            // o/w the check is failing -- if there is output, report an UNHEALTHY status
            // o/w the check is failing and there is no output -- this is UNKNOWN
            final String healthOutput = checkResult.getOutput().or(ComponentState.UNKNOWN.name());
            final ComponentState result = checkResult.getStatus().equalsIgnoreCase(CONSUL_HEALTH_CHECK_PASSING_RESULT) ?
                    ComponentState.RUNNING :
                    // if the output ends with 'no such host', the health endpoint poll failed.
                    // we'll treat this as an unknown result. Note that this is a Consul-specific
                    // result, and will be revisited when we move to kubernetes.
                    (healthOutput.endsWith(CONSUL_HEALTH_CHECK_UNSUCCESSFUL_FRAGMENT) ?
                            ComponentState.UNKNOWN :
                            ComponentState.UNHEALTHY);
            answer.put(entry.getKey(), result);
        }
        log.debug("getComponentsState() took {} ms", System.currentTimeMillis() - startTime);
        return answer;
    }

    /**
     * Retrieves a map of all of the component health checks from Consul.
     *
     * @return a map of component id -> health check
     */
    public Map<String,HealthCheck> getAllComponentsHealth() {
        Set<String> discoveredComponents = getKnownComponents();
        Map<String, HealthCheck> retVal = new HashMap<>();
        for (String componentInstance : discoveredComponents) {
            for (HealthCheck check : consulService.getServiceHealth(componentInstance)) {
                retVal.put(check.getServiceId().or(componentInstance), check);
            }
        }
        return retVal;
    }

    /**
     * Gather diagnostics from all currently running VmtComponents. The running VmtComponents are discovered by:
     * (1) for each known component type, (2) from the Consul service registry, get all instances
     * for the given component type, (3) issue an HTTP request to that component instance to
     * GET /diagnostics, (4) zip the result from step 3 onto the given responseOutput as a zip file.
     * Note that the current implementation is sequential through all the known components. It would be
     * possible to issue all the component HTTP requests asynchronously, and as each request is
     * satisfied dump the response onto the output stream.  Remember that you may not interleave
     * partial responses from the different components; a complete response must be dumped onto the zip file
     * at a time.
     * <p>
     * Note that the responseOutput stream is not closed on exit.
     *
     * todo: consider moving this to its own class, e.g. ClusterDiagnosticsService
     * todo: look into the "offset" parameter to the diagnosticZip.write() method
     *
     * @param responseOutput the output stream onto which the zipfile is written.
     * @throws RuntimeException for errors creating URI, fetching data, and copying to output zip stream
     */
    public void collectComponentDiagnostics(OutputStream responseOutput) {
        ZipOutputStream diagnosticZip = new ZipOutputStream(responseOutput);
        String acceptTypes = MediaType.toString(Arrays.asList(
                MediaType.valueOf("application/zip"),
                MediaType.APPLICATION_OCTET_STREAM));
        final StringBuilder errorMessagesBuild = new StringBuilder();
        visitActiveComponents("/diagnostics", acceptTypes, (componentId, entity) -> {
            log.info(componentId + " --- Begin diagnostic collection");
            try (InputStream componentDiagnosticStream = entity.getContent()) {
                // create a new .zip file entry on the zip output stream
                String zipFileName = componentId + "-diags.zip";
                log.debug(componentId + " --- adding zip file named: " + zipFileName);
                diagnosticZip.putNextEntry(new ZipEntry(zipFileName));
                // copy the target .zip diagnostic file onto the .zip output stream
                IOUtils.copy(componentDiagnosticStream, diagnosticZip);
            } catch (IOException e) {
                // log the error and continue to the next service in the list of services
                log.error(componentId + " --- Error reading diagnostic stream", e);
                final String instanceName = componentId;
                errorMessagesBuild.append(instanceName + NEXT_LINE);
            } finally {
                log.debug(componentId + " --- closing zip entry");
                try {
                    diagnosticZip.closeEntry();
                } catch (IOException e) {
                    log.error("Error closing diagnostic .zip", e);
                }
            }
        }, errorMessagesBuild);
        getRsyslogDiags(diagnosticZip, acceptTypes, errorMessagesBuild);
        insertDiagsSummaryFile(diagnosticZip, errorMessagesBuild);

        // finished all instances of all known components - finish the aggregate output .zip file
        // stream
        try {
            log.debug("finishing diagnosticZip");
            // NB. finish() doesn't close the underlying stream. I.e. do NOT wrap in try
            // (resources){}
            diagnosticZip.finish();
        } catch (IOException e) {
            throw new RuntimeException("I/O error finishing diags zip stream", e);
        }
    }

    /**
     * Specialized request for the rsyslog to retrieve all the logs.
     * The reason for an independent method lies in fact that rsyslog is not a component
     * managed by Consul, so has a different configuration.
     *
     * Test: The test is very much an integration here.
     *
     * @param diagnosticZip The diagnostic zip stream.
     * @param acceptResponseTypes The HTTP header for accepted responses.
     */
    private void getRsyslogDiags(ZipOutputStream diagnosticZip, String acceptResponseTypes,
                                 @Nonnull final StringBuilder errorMessageBuilder) {
        // Handle the rsyslog
        // It is not part of the consul-managed set of components.
        String componentName = "rsyslog";
        URI requestUri = getComponentInstanceUri(componentName, "/diagnostics");
        HttpGet request = new HttpGet(requestUri);
        request.addHeader("Accept", acceptResponseTypes);
        try (CloseableHttpClient httpclient =  HttpClientBuilder.create().
            setDefaultRequestConfig(requestConfig).build()) {
            // execute the request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                log.debug(componentName + " --- response status: " + response.getStatusLine());
                // process the response entity
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try (InputStream componentDiagnosticStream = entity.getContent()) {
                        // create a new .zip file entry on the zip output stream
                        String zipFileName = componentName + "-diags.zip";
                        log.debug(componentName + " --- adding zip file named: " + zipFileName);
                        diagnosticZip.putNextEntry(new ZipEntry(zipFileName));
                        // copy the target .zip diagnostic file onto the .zip output stream
                        IOUtils.copy(componentDiagnosticStream, diagnosticZip);
                    } catch (IOException e) {
                        // log the error and continue to the next service in the list of services
                        log.error(componentName + " --- Error reading diagnostic stream", e);
                        errorMessageBuilder.append(componentName + NEXT_LINE);
                    } finally {
                        log.debug(componentName + " --- closing zip entry");
                        try {
                            diagnosticZip.closeEntry();
                        } catch (IOException e) {
                            log.error("Error closing diagnostic .zip", e);
                        }
                    }
                } else {
                    // log the error and continue to the next service in the list of services
                    log.error(componentName + " --- missing response entity");
                }
            }
        } catch (IOException e) {
            log.error(componentName.toString() + " --- Error fetching the information", e);
        }
    }

    // Insert summary file:
    // If all service component were assembled successfully, empty file with name "DiagsSummary-Success.txt".
    // Otherwise, file name is "DiagsSummary-Fail.txt" and it contains failed service component names.
    @VisibleForTesting
    void insertDiagsSummaryFile(@Nonnull final ZipOutputStream diagnosticZip,
                                @Nonnull final StringBuilder errorMessages) {
        final boolean isSuccessful = errorMessages.toString().isEmpty();
        final String summaryFileName = isSuccessful ? DIAGS_SUMMARY_SUCCESS_TXT : DIAGS_SUMMARY_FAIL_TXT;
        final ZipEntry zipEntry = new ZipEntry(summaryFileName);
        try {
            diagnosticZip.putNextEntry(zipEntry);
            if (!isSuccessful) {
                final byte[] data = errorMessages.toString().getBytes();
                diagnosticZip.write(data, 0, data.length);
            }
        } catch (IOException e) {
            log.error("Error adding diagnostic stream", e);
        } finally {
            log.debug(" closing zip entry");
            try {
                diagnosticZip.closeEntry();
            } catch (IOException e) {
                log.error("Error closing diagnostic .zip", e);
            }
        }
    }
    /**
     * Specialized request for the rsyslog to retrieve all the logs.
     * The reason for an independent method lies in fact that rsyslog is not a component
     * managed by Consul, so has a different configuration.
     *
     * Test: The test is very much an integration here.
     *
     * @param out The output stream.
     * @param acceptResponseTypes The HTTP header for accepted responses.
     */
    public void getRsyslogProactive(OutputStream out, String acceptResponseTypes) {
        // Handle the rsyslog
        // It is not part of the consul-managed set of components.
        String componentName = "rsyslog";
        URI requestUri = getComponentInstanceUri(componentName, "/proactive");
        HttpGet request = new HttpGet(requestUri);
        request.addHeader("Accept", acceptResponseTypes);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            // execute the request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                // process the response entity
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try (InputStream componentDiagnosticStream = entity.getContent()) {
                        IOUtils.copy(componentDiagnosticStream, out);
                    } catch (IOException e) {
                        // log the error and continue to the next service in the list of services
                        log.error(componentName + " --- Error reading diagnostic stream", e);
                    }
                } else {
                    // log the error and continue to the next service in the list of services
                    log.error(componentName + " --- missing response entity");
                }
            }
        } catch (IOException e) {
            log.error(componentName + " --- Error fetching the information", e);
        }
    }

    /**
     * Send an HTTP GET to each active VMT Component Instance and process the response.
     *
     * Note that this process is single-threaded and blocks for each request.
     *
     * TODO: consider parallel processing for the list of requests.
     * @param requestPath the URL to GET from each active component instance
     * @param acceptResponseTypes the http response types to accept from this request
     * @param responseEntityProcessor the processor for the response.getEntity() after the http request completes.
     */
    private void visitActiveComponents(String requestPath,
                                       String acceptResponseTypes,
                                       ResponseEntityProcessor responseEntityProcessor,
                                       @Nonnull final StringBuilder errorMessageBuilder) {
        for (String componentType : getKnownComponents()) {
            log.debug("getting " + requestPath + " for component type: " + componentType);
            Set<String> instanceIds = getComponentInstanceIds(componentType);
            for (String instanceId : instanceIds) {
                String instanceIp = getComponentInstanceProperty(componentType, instanceId, PROP_INSTANCE_IP);
                if (StringUtils.isNotBlank(instanceIp)) {
                    URI requestUri = getComponentInstanceUri(instanceIp, requestPath);
                    HttpGet request = new HttpGet(requestUri);
                    request.addHeader("Accept", acceptResponseTypes);
                    sendRequestToComponent(instanceId, request, responseEntityProcessor,
                            java.util.Optional.of(errorMessageBuilder));
                }
            }
        }
    }

    /**
     * Send an HTTP GET to a given VMT Component Instance and process the response.
     *
     * Note: we treat an http request as a non-fatal error; most likely cause is the target
     * component is not currently running.
     *
     * @param componentName the VMT Component service to send the request to
     * @param request the HttpUriRequest to execute
     * @param responseEntityProcessor the processor for the response.getEntity() after the http request completes.
     */
    private void sendRequestToComponent(String componentName, HttpUriRequest request,
                                        ResponseEntityProcessor responseEntityProcessor,
                                        @Nonnull final java.util.Optional<StringBuilder> errorMessagBuilder) {
        // open an HttpClient link
        try (CloseableHttpClient httpclient = HttpClientBuilder.create().
            setDefaultRequestConfig(requestConfig).build()) {
            // execute the request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                log.debug(componentName + " --- response status: " + response.getStatusLine());
                // process the response entity
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    responseEntityProcessor.process(componentName, entity);
                } else {
                    appendFailedServiceName(componentName, errorMessagBuilder);
                    // log the error and continue to the next service in the list of services
                    log.error(componentName + " --- missing response entity");
                }
            }
        } catch (IOException e) {
            appendFailedServiceName(componentName, errorMessagBuilder);
            // log the error and continue to the next service
            log.error("{} --- Error executing http request {}: {}",
                    componentName,
                    request.getURI().toString(),
                    e.getMessage());
        }
    }

    @VisibleForTesting
    void appendFailedServiceName(@Nonnull final String componentName,
                                 @Nonnull final java.util.Optional<StringBuilder> errorMessageBuilder) {
        errorMessageBuilder.ifPresent(builder -> builder.append(componentName + NEXT_LINE));
    }

    /**
     * Returns the component instance URI.
     *
     * @param componentInstance The dnsname or address of the component.
     * @param requestPath The request path.
     * @return The URI.
     */
    private URI getComponentInstanceUri(String componentInstance, String requestPath) {
        // create a request from the given path and the target component instance properties
        URI requestUri;
        try {
            requestUri = new URIBuilder()
                    .setHost(componentInstance)
                    .setPort(8080)
                    .setScheme("http")
                    .setPath(requestPath)
                    .build();
            log.debug(" --- : " + requestUri);
        } catch (URISyntaxException e) {
            // log the error and continue to the next service in the list of services
            throw new RuntimeException(" --- Error creating diagnostic URI to query component", e);
        }
        return requestUri;
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
    public ClusterConfiguration getClusterConfiguration() {
        ClusterConfiguration answer = new ClusterConfiguration();
        // add the node component/property configurations
        for (String componentType : getKnownComponents()) {
            ComponentProperties defaultProperties = getComponentDefaultProperties(componentType);
            answer.addComponentType(componentType, defaultProperties);
            // copy the version number string into the instance properties...TODO: revisit if/when we
            // remove the instance properties for components
            String version = defaultProperties.get(COMPONENT_VERSION_KEY);
            Set<String> instanceIds = getComponentInstanceIds(componentType);
            for (String instanceId : instanceIds) {
                String nodeId = getNodeForComponentInstance(componentType, instanceId);
                String instancePropertiesKey = getComponentInstancePropertiesKey(componentType, instanceId);
                ComponentProperties instanceProperties = getComponentPropertiesWithPrefix(instancePropertiesKey);
                answer.addComponentInstance(instanceId, componentType, version, nodeId, instanceProperties);
            }
        }
        return answer;
    }

    /**
     * Replace the entire Cluster Configuration.
     * This includes the <strong>default properties</strong> {@link ComponentProperties}
     * (component-type -> default properties)
     * and the <strong>instance properties</strong> {@link ComponentProperties}
     * (instance-id -> instance properties)
     *
     * @param newConfiguration an {@link ClusterConfiguration} to completely replace the current configuration.
     * @return the new configuration, read back from the Consul key/value store.
     */
    @Nonnull
    public ClusterConfiguration setClusterConfiguration(ClusterConfiguration newConfiguration) {

        // clear the current configuration (gulp!)
        consulService.deleteKey(getVMTurboBaseKey());

        // Process the defaults for the given component types
        for (Map.Entry<String, ComponentProperties> defaultEntry : newConfiguration.getDefaults().getComponents().entrySet()) {
            String componentType = defaultEntry.getKey();
            String defaultComponentKey = getComponentDefaultsKey(componentType);
            setAllValues(defaultComponentKey, defaultEntry.getValue());
        }
        // process the properties for each component instance
        for (Map.Entry<String, ComponentInstanceInfo> instanceEntry : newConfiguration.getInstances().entrySet()) {
            String instanceId = instanceEntry.getKey();
            ComponentInstanceInfo instanceInfo = instanceEntry.getValue();
            String componentType = instanceInfo.getComponentType();
            String instancePropertiesKey = getComponentInstancePropertiesKey(componentType, instanceId);
            setAllValues(instancePropertiesKey, instanceInfo.getProperties());
            String componentNode = instanceInfo.getNode();
            String instanceNodeKey = getInstanceNodeKey(componentType, instanceId);
            consulService.putValue(instanceNodeKey, componentNode);

        }
        return getClusterConfiguration();
    }

    /**
     * Return the current Cluster Node name assigned to this VMT Component Instance.
     *
     * @param componentType type of the component to which component is assigned
     * @param instanceId the id of the VMT Component Instance to look up.
     * @return the node name on which this component should be run.
     */
    public String getNodeForComponentInstance(String componentType, String instanceId) {
        String instanceNodeKey = getInstanceNodeKey(componentType, instanceId);
        return consulService.getValueAsString(instanceNodeKey, DEFAULT_NODE_NAME);
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
    public String setNodeForComponentInstance(String componentType, String instanceId, String nodeName) {
        checkValidNodeName(nodeName);
        String instanceNodeKey = getInstanceNodeKey(componentType, instanceId);
        consulService.putValue(instanceNodeKey, nodeName);
        return getNodeForComponentInstance(componentType, instanceId);
    }

    public void addInstancePropertiesNode(@Nonnull final String componentType,
                                          @Nonnull final String instanceId) {
        final String propertiesNodeKey =
                getComponentInstancePropertiesKey(componentType, instanceId);
        consulService.putValue(propertiesNodeKey);
    }

    /**
     * Return the default {@link ComponentProperties} for the given component type.
     *
     * @param componentType the component type for which to fetch the default ComponentProperties
     * @return a {@link ComponentProperties} object containing all default configuration properties for the given
     * component type.
     */
    public ComponentProperties getDefaultPropertiesForComponentType(String componentType) {
        return getComponentDefaultProperties(componentType);
    }

    /**
     * Replace the default {@link ComponentProperties} for the given component type. Begin with
     * the Global Default properties as a base, and then overlay the new Default properties on
     * top of that.
     *
     * @param componentType the component type for which to store the default ComponentProperties
     * @param newComponentDefaultProperties the new values for the default {@link ComponentProperties} for
     *                             the given component type
     * @return a {@link ComponentProperties} object containing all default configuration properties
     * for the given component type.
     */
    public ComponentProperties putDefaultPropertiesForComponentType(
            String componentType,
            ComponentProperties newComponentDefaultProperties) {

        // begin with the 'global default properties'
        ComponentProperties combinedDefaultProperties = new ComponentProperties();
        combinedDefaultProperties.putAll(globalDefaultProperties);

        // add the new default properties
        combinedDefaultProperties.putAll(newComponentDefaultProperties);

        // ensure that the component type exists; if already exists, this is a no-op
        addComponentType(componentType);

        // store those as the new component defaults
        final String componentDefaultsKeyPrefix = getComponentDefaultsKey(componentType);
        setAllValues(componentDefaultsKeyPrefix, combinedDefaultProperties);

        // return the updated component default properties
        return getComponentDefaultProperties(componentType);
    }

    /**
     * Return the {@link ComponentProperties} for the given component instance / type.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    public ComponentProperties getComponentInstanceProperties(String componentType, String componentInstanceId) {
        final String defaultPropertiesKey = getComponentDefaultsKey(componentType);
        final ComponentProperties componentProperties =
                getComponentPropertiesWithPrefix(defaultPropertiesKey);

        final String instancePropertiesKey =
                getComponentInstancePropertiesKey(componentType, componentInstanceId);
        final ComponentProperties instanceSpecificProperties =
                getComponentPropertiesWithPrefix(instancePropertiesKey);
        componentProperties.putAll(instanceSpecificProperties);
        return componentProperties;
    }

    /**
     * Replace the {@link ComponentProperties} for the given component instance / type.
     * Return the updated {@link ComponentProperties}.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @param updatedProperties the new configuration property values to be saved.
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */

    public ComponentProperties putComponentInstanceProperties(String componentType, String componentInstanceId, ComponentProperties updatedProperties) {
        log.debug("updating default properties for: " + componentType);
        setComponentInstanceProperties(componentType, componentInstanceId, updatedProperties);
        return getComponentInstanceProperties(componentType, componentInstanceId);

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
    public String getComponentTypeProperty(
            String componentType,
            String propertyName) {
        String defaultPropertyKey = getComponentDefaultPropertyKey(componentType, propertyName);
        return getComponentKeyValue(defaultPropertyKey);
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
    public String getComponentInstanceProperty(
            String componentType,
            String componentInstanceId,
            String propertyName) {
        String instancePropertiesKey = getComponentInstancePropertyKey(componentType, componentInstanceId, propertyName);
        return getComponentKeyValue(instancePropertiesKey);
    }

    /**
     * Checks whether the telemetry has been locked out.
     * The site administrators should mbe able to completely lock out the
     * telemetry in the docker-compose.yml.
     * The actual code is in the {@link ClusterMgrConfig}.
     *
     * @return {@code true} iff the telemetry has been locked out.
     */
    private boolean isTelemetryPermitted() {
        return Boolean.parseBoolean(consulService.getValueAsString(TELEMETRY_LOCKED, "false"));
    }

    /**
     * Returns enabled flag.
     *
     * @return The enabled flag.
     */
    Boolean isTelemetryEnabled() {
        if (isTelemetryPermitted()) {
            return Boolean.FALSE;
        }
        return Boolean.parseBoolean(consulService.getValueAsString(TELEMETRY_ENABLED, "true"));
    }

    /**
     * Sets enabled flag.
     * Will throw an exception in case the telemetry has been locked out by the site administrator.
     *
     * @param enabled The enabled flag.
     */
    void setTelemetryEnabled(final Boolean enabled) {
        if (isTelemetryPermitted()) {
            throw new IllegalStateException("The Telemetry has been locked " +
                                            "out by the site administrator");
        }
        boolean flag = (enabled == null) ? false : enabled;
        consulService.putValue(TELEMETRY_ENABLED, String.valueOf(flag));
    }

    /**
     * Return the default {@link ComponentProperties} for the given component instance / type.
     *
     * Add the version for the component type to the default properties.
     *
     * @param componentType the component type for the requested defaults
     * @return the default {@link ComponentProperties} for the given component type
     */
    @Nonnull
    private ComponentProperties getComponentDefaultProperties(String componentType) {
        String defaultPropertiesKey = getComponentDefaultsKey(componentType);
        final ComponentProperties defaultProperties = getComponentPropertiesWithPrefix(defaultPropertiesKey);

        // The Component version is added to the defaults for each component.  This assumes
        // that version of all instancese of a component are the same.
        String version = getComponentVersionString(componentType);
        defaultProperties.put(COMPONENT_VERSION_KEY, version);

        return defaultProperties;
    }

    /**
     * Fetch the version string for this component from the the k/v store.
     * The key for the version string is, for now, "${component_type}-1". This is a hold-over
     * from the naming for the component key/value store naming.
     *
     * @param componentType the component-type to look up
     * @return the version string for this component type, or "<unknown>" if not found.
     */
    @Nonnull
    private String getComponentVersionString(@Nonnull final String componentType) {
        com.google.common.base.Optional<String> componentVersion = consulService.getValueAsString(
            componentType + "-1/" + COMPONENT_VERSION_KEY);
        return componentVersion.isPresent() ? componentVersion.get() : UNKNOWN_VERSION_STRING;
    }

    /**
     * Base key for all VMT Key/Value information
     */
    private static String getVMTurboBaseKey() {
        return VMTURBO_BASE_FORMAT;
    }

    /**
     * Base key for all VMT Components information.
     */
    private static String getComponentsBaseKey() {
        return COMPONENTS_BASE_FORMAT;
    }

    /**
     * Key for accessing a given component type
     * @param componentType type of component to access
     */
    private static String getComponentKey(String componentType) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_FORMAT, componentType);
    }

    /**
     * Key for accessing defaults of a given component type.
     *
     * @param componentType type of component to access
     */
    private static String getComponentDefaultsKey(String componentType) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_DEFAULTS_FORMAT, componentType);
    }

    /**
     * Key for accessing defaults of a given component type.
     *
     * @param componentType type of component to access
     */
    private static String getComponentDefaultPropertyKey(String componentType, String propertyName) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_DEFAULTS_PROPERTY_FORMAT, componentType, propertyName);
    }

    /**
     * Key for accessing instances of a given component type.
     *
     * @param componentType type of component to access
     */
    private static String getComponentInstancesKey(String componentType) {
        return String.format(COMPONENT_INSTANCES_FORMAT, componentType);
    }

    /**
     * Key for accessing instances of a given component type.
     *
     * @param componentType type of component to access
     */
    private static String getComponentInstanceKey(String componentType, String componentInstanceId) {
        return String.format(COMPONENT_INSTANCE_FORMAT, componentType, componentInstanceId);
    }

    /**
     * Key for accessing properties of a given instance of a given component type.
     *
     * @param componentType type of component to access
     * @param componentInstanceId instance id of the specific component to access
     */
    private static String getComponentInstancePropertiesKey(String componentType, String componentInstanceId) {
        return String.format(COMPONENT_INSTANCE_PROPERTIES_FORMAT, componentType, componentInstanceId);
    }

    /**
     * Key for accessing a given property of a given instance of a given component type.
     *
     * @param componentType type of component to access
     * @param componentInstanceId instance id of the specific component to access
     * @param propertyName name of the specific property
     */
    private static String getComponentInstancePropertyKey(
            String componentType,
            String componentInstanceId,
            String propertyName) {
        return String.format(COMPONENT_INSTANCE_PROPERTY_FORMAT, componentType, componentInstanceId, propertyName);
    }

    /**
     * Key for accessing the execution node name for a given instance of a given component type.
     *
     * @param componentType type of component to access
     * @param componentInstanceId instance id of the specific component to access
     */
    private static String getInstanceNodeKey(String componentType, String componentInstanceId) {
        return String.format(COMPONENT_INSTANCE_NODE_FORMAT, componentType, componentInstanceId);
    }


    /**
     * A Node Name may be a hostname, IP address, or empty string ("") which corresponds to the "default" node.
     *
     * @param nodeName the name of the node to check for illegal characters.
     * @throws RuntimeException if the node name is incompatible.
     */
    private void checkValidNodeName(@Nonnull String nodeName) {
        if (!nodeName.isEmpty() && !InetAddresses.isInetAddress(nodeName) && !InternetDomainName.isValid(nodeName)) {
            throw new RuntimeException(("invalid nodename: " + nodeName));
        }
    }

    /**
     * Verify that a Component type name is legal. Component names may not contain the Consul path separator character,
     * a period ('.'), may not be empty, and must also be one of the "known components".
     *
     * @param componentType the name of the node to check for illegal characters.
     */
    private static void checkComponentTypeValid(@Nonnull String componentType) {
        if (!isValidKeyComponent(componentType)) {
            throw new RuntimeException("invalid component name: " + componentType);
        }
    }

    /**
     * Check that a given string is a legal Consul k/v path component for a Consul key/value key. A Consul key path
     * may not be empty, may not contain a period ('.'), and may not contain the CONSUL_PATH_SEPARATOR ('/').
     *
     * @param keyComponent the component to check for validity
     */
    private static boolean isValidKeyComponent(@Nonnull String keyComponent) {
        return (StringUtils.isNotBlank(keyComponent) &&
                !StringUtils.containsAny(keyComponent, CONSUL_PATH_SEPARATOR, PROPERTY_KEY_SEPARATOR));
    }

    /**
     * Export diagnostics to upload.vmturbo.com using Curl. Because:
     * 1. Both classic and metron are using curl to upload files.
     * 2. The vmtupload.cgi in upload.vmturbo.com doesn't support stream upload!!!
     * Note: this method is synchronized because they are resources intensive operations, e.g.
     * the diagnostics file could be very big.
     * TODO: integration test
     * @param httpProxyDTO value object with proxy settings
     * @return true if export is successful
     */
    public synchronized boolean exportDiagnostics(final HttpProxyDTO httpProxyDTO) {
        // export the diags file to /home/turbonomic/data/turbonomic-diags-_xxx.zip
        // xxx is current system time epoch.
        final String fileName = String.format(HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP,
            System.currentTimeMillis());
        final File diagsFile = new File(fileName);

        // 1. write to file
        if (writeDiagnosticsFileToDisk(diagsFile)) {
            // 2. invoke curl command to upload, e.g.:
            // curl -x 10.10.172.84:3128 -F ufile=@test.log  http://10.10.168.175/cgi-bin/vmturboupload.cgi
            try {
                // this runner blocks until the process completes
                osCommandProcessRunner.runOsCommandProcess(CURL_COMMAND, getCurlArgs(fileName, httpProxyDTO));
                // delete the file
                if (diagsFile.delete()) {
                    log.debug("Successfully deleted diagnostic file: " + diagsFile.getName());
                } else {
                    log.error("failed to deleted diagnostic file:" + diagsFile.getName());
                }
            } finally {
                log.info("Successfully upload diagnostics file: " + fileName);
            }
            return true;
        }
        return false;
    }

    // write diagnostics file to disk
    private boolean writeDiagnosticsFileToDisk(@Nonnull final File diagsFile) {
        FileOutputStream diagsFileOutputStream = null;
        try {
            diagsFileOutputStream = new FileOutputStream(diagsFile);
            collectComponentDiagnostics(diagsFileOutputStream);
            diagsFileOutputStream.flush();
            diagsFileOutputStream.close();
        } catch (FileNotFoundException e) {
            log.error("Failed to write diagnostics file to {}. Error message: {}",
                HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP, e.getMessage() );
            return false;
        } catch (IOException e) {
            log.error("Failed to write diagnostics file to {}. Error message: {}",
                HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP, e.getMessage() );
            return false;
        } finally {
            try {
                if (diagsFileOutputStream != null) {
                    diagsFileOutputStream.close();
                }
            } catch (IOException e) {
                log.error("Failed to close diagnostic file output stream. Error message: {}",
                    e.getMessage() );
                // it doesn't affect writing to disk.
            }
        }
        return true;
    }

    // get curl command based on if proxy is available or not
    @VisibleForTesting
    String[] getCurlArgs(@Nonnull final String fileName,
                         @Nonnull final HttpProxyDTO httpProxyDTO) {
        final List <String> argsList = Lists.newArrayList("-F", "ufile=@" + fileName,
            UPLOAD_VMTURBO_COM_URL);
        // it's mainly not writing proxy password to log file.
        final List <String> argsListDebug = Lists.newArrayList(argsList);
        if (httpProxyDTO.getIsProxyEnabled()) {
            String hostPortTuple = httpProxyDTO.getProxyHostAndPortTuple();
            if (StringUtils.isNotBlank(hostPortTuple)) {
                // using proxy
                argsList.add("-x");
                argsListDebug.add("-x");

                String userNamePasswordTuple = httpProxyDTO.getUserNameAndPasswordTuple();
                if (StringUtils.isNotBlank(userNamePasswordTuple)) {
                    argsList.add(Strings.concat(userNamePasswordTuple, AT, hostPortTuple));
                    argsListDebug.add(Strings.concat(httpProxyDTO.getUserNameAndHiddenPasswordTuple(), AT, hostPortTuple));
                } else {
                    argsList.add(hostPortTuple);
                    argsListDebug.add(hostPortTuple);
                }
            }
        }
        log.info("curl arguments: " +argsListDebug);
        return argsList.toArray(new String[0]);
    }

    private interface ResponseEntityProcessor {
        void process(String componentId, HttpEntity entity);
    }
}
