package com.vmturbo.clustermgr;

import static com.vmturbo.clustermgr.ClusterMgrConfig.TELEMETRY_ENABLED;
import static com.vmturbo.clustermgr.ClusterMgrConfig.TELEMETRY_LOCKED;
import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;
import static com.vmturbo.clustermgr.api.ClusterMgrClient.UNKNOWN_VERSION_STRING;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.NotFoundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.kv.Value;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;

import com.vmturbo.clustermgr.api.ClusterConfiguration;
import com.vmturbo.clustermgr.api.ComponentInstanceInfo;
import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.clustermgr.api.HttpProxyConfig;
import com.vmturbo.clustermgr.management.ComponentHealth;
import com.vmturbo.clustermgr.management.ComponentRegistry;
import com.vmturbo.clustermgr.management.RegisteredComponent;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.utils.Strings;

/**
 * Implement the ClusterMgr Services: component status, component configuration, node configuration.
 * This implementation uses the Consul key/value service API.
 *
 * <p>Consul key structure:
 * {@code
 * vmturbo/components/{component-type}/defaults/properties/{property-name} = default-property-value
 * vmturbo/components/{component-type}/instances/{instance_id}/properties/{property-name} = property-value
 * vmturbo/components/{component-type}/instances/{instance_id}/node = {node id where instance runs}
 * }
 *
 * <p>Consul values may be any byte string, with a size limit of 512kB.
 ***/
public class ClusterMgrService {
    // Consul key path component names to implement the Component/Node/Properties schema above
    private static final Character CONSUL_PATH_SEPARATOR = '/';
    private static final String VMTURBO_BASE_FORMAT = "vmturbo" + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENTS_BASE_FORMAT = VMTURBO_BASE_FORMAT + "components"
        + CONSUL_PATH_SEPARATOR;
    // %s = component type
    private static final String COMPONENT_FORMAT = COMPONENTS_BASE_FORMAT + "%s"
        + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENT_DEFAULTS_FORMAT = COMPONENT_FORMAT + "defaults"
        + CONSUL_PATH_SEPARATOR;
    // %s = property name
    private static final String COMPONENT_DEFAULTS_PROPERTY_FORMAT = COMPONENT_DEFAULTS_FORMAT + "%s";

    private static final String COMPONENT_LOCAL_PROPERTIES_FORMAT = COMPONENT_FORMAT
        + "local" + CONSUL_PATH_SEPARATOR;
    // %s = property name
    private static final String COMPONENT_LOCAL_PROPERTY_FORMAT =
        COMPONENT_LOCAL_PROPERTIES_FORMAT + "%s";

    private static final String COMPONENT_INSTANCES_FORMAT = COMPONENT_FORMAT + "instances"
        + CONSUL_PATH_SEPARATOR;
    // %s = component instance id
    private static final String COMPONENT_INSTANCE_FORMAT = COMPONENT_FORMAT + "instances"
        + CONSUL_PATH_SEPARATOR + "%s" + CONSUL_PATH_SEPARATOR;
    private static final String COMPONENT_INSTANCE_PROPERTIES_FORMAT = COMPONENT_INSTANCE_FORMAT
        + "properties" + CONSUL_PATH_SEPARATOR;
    // %s = property name
    private static final String COMPONENT_INSTANCE_PROPERTY_FORMAT =
        COMPONENT_INSTANCE_PROPERTIES_FORMAT + "%s";
    private static final Character PROPERTY_KEY_SEPARATOR = '.';

    // split on the path separator for Consul keys: '/'
    private static final Splitter PATH_SEP_LIST_SPLITTER = Splitter.on(CONSUL_PATH_SEPARATOR)
            .omitEmptyStrings();

    // constant values used for parsing consul health check results
    private static final String CONSUL_HEALTH_CHECK_PASSING_RESULT = "passing";
    private static final String CONSUL_HEALTH_CHECK_UNSUCCESSFUL_FRAGMENT = "no such host";
    private static final String GLOBAL_DEFAULT_COMPONENT_CONFIG_PROPERTIES_PATH =
        "config/global_defaults.properties";

    /**
     * The most recent diagnostics file uploaded will be stored in the /tmp folder.
     * When a new diagnostic is about to be uploaded, any prior diagnostics files will
     * be deleted.
     */
    private static final String DIAGNOTICS_OUTPUT_FILE_PATH = "/tmp/diagnostics/";

    /**
     * Used in formatting the diagnostic upload "curl" command".
     */
    private static final String AT = "@";
    private static final String COLON = ":";

    /**
     * The socket timeout in milliseconds, which is the timeout for
     * waiting for data  or, put differently, a maximum period inactivity
     * between two consecutive data packets).
     *
     * <p>A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     *
     * <p>Default: {@code -1}
     */
    private static final int SOCKET_TIMEOUT = 10 * 60 * 1000;

    /**
     * Timeout in milliseconds until a connection is established.
     * A timeout value of zero is interpreted as an infinite timeout.
     *
     * <p>A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     *
     * <p>Default: {@code -1}
     */
    private static final int CONNECT_TIMEOUT = 10 * 1000;

    /**
     * Timeout in milliseconds used when requesting a connection
     * from the connection manager. A timeout value of zero is interpreted
     * as an infinite timeout.
     *
     * <p>A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     *
     * <p>Default: {@code -1}
     */
    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final String NEXT_LINE = "\n";

    @VisibleForTesting
    static final String DIAGS_SUMMARY_SUCCESS_TXT = "DiagsSummary-Success.txt";

    @VisibleForTesting
    static final String DIAGS_SUMMARY_FAIL_TXT = "DiagsSummary-Fail.txt";

    private static final String ENVIRONMENT_SUMMARY_TXT = "EnvironmentSummary.txt";
    private static final String DIAGNOSTIC_OPERATION_BUSY =
        "There is currently a diagnostic operation running.";

    // Custom diags collecting request configuration
    private final RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(CONNECT_TIMEOUT)
        .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
        .setSocketTimeout(SOCKET_TIMEOUT).build();

    // Default configuration properties global to all component types
    private final ComponentProperties globalDefaultProperties = new ComponentProperties();
    private final OsCommandProcessRunner osCommandProcessRunner;

    private static final String CURL_COMMAND = "curl";
    @VisibleForTesting
    static final String UPLOAD_VMTURBO_COM_URL =
        "https://upload.vmturbo.com/appliance/cgi-bin/vmtupload.cgi";

    private static Logger log = LogManager.getLogger();

    /**
     * The {@link ConsulService} handles the details of the accessing the Key/Value store and
     * Service Registration function.
     */
    private final ConsulService consulService;

    private final DiagEnvironmentSummary diagEnvironmentSummary;

    private final ComponentRegistry componentRegistry;

    private final AtomicBoolean kvInitialized = new AtomicBoolean(false);

    /**
     * Lock to provide for single-use access to the ClusterMgr /diagnostics upload and
     * download. Allowing more than one upload/download at a time will significantly impact
     * system resources and increase the time for each upload/download to complete - resulting
     * in a negative user experience. Instead, the system service will reject any additional
     * diagnostics requests immediately, indicicating "system busy".
     *
     * <p>Note: in the future, in order to support horizontal scaling for ClusterMgr, this lock
     * will need to be implemented using a persistent locking facility such as is provided
     * Consul.
     */
    private final Lock diagnosticsLock = new ReentrantLock();

    /**
     * Create a new ClusterMgrService instance.
     *
     * @param consulService the client API for calls to Consul
     * @param osCommandProcessRunner a utility object for running operating system commands
     * @param diagEnvironmentSummary Utility to get summary information for diag collection.
     * @param componentRegistry Component registry to get information about registered components.
     */
    public ClusterMgrService(@Nonnull final ConsulService consulService,
                             @Nonnull final OsCommandProcessRunner osCommandProcessRunner,
                             @Nonnull final DiagEnvironmentSummary diagEnvironmentSummary,
                             @Nonnull final ComponentRegistry componentRegistry) {
        this.consulService = Objects.requireNonNull(consulService);
        this.osCommandProcessRunner = osCommandProcessRunner;
        this.diagEnvironmentSummary = Objects.requireNonNull(diagEnvironmentSummary);
        this.componentRegistry = componentRegistry;
    }

    public boolean isClusterKvStoreInitialized() {
        return kvInitialized.get();
    }

    /**
     * Set kvInitialized to true if default configuration properties are loaded from configMap so
     * that clustermgr can be marked as healthy.
     *
     * @param clusterKvStoreInitialized True if default configuration properties are loaded from
     *                                  configMap.
     */
    public void setClusterKvStoreInitialized(boolean clusterKvStoreInitialized) {
        kvInitialized.set(clusterKvStoreInitialized);
    }

    /**
     * Fetch the set of Components known to VMTurbo from the Consul K/V store.
     * Components are "known" if there is a configuration key "{@code /<component-type>-1}/}", e.g.: "api-1".
     *
     * @return the set of all known component names.
     */
    @Nonnull
    public Set<String> getKnownComponents() {
        return consulService.getAllServiceInstances().keySet();
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
     * <p>Note that there is no check for a previous component type by the same name
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
        if (keyPrefix.charAt(keyPrefix.length() - 1) != CONSUL_PATH_SEPARATOR) {
            throw new RuntimeException("Value keyPrefix does not end with CONSUL_PATH_SEPARATOR: "
                    + CONSUL_PATH_SEPARATOR);
        }
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
     * <p>See the {@literal COMPONENT_INSTANCE_PROPERTY_FORMAT} format for the full key to be used in Consul.
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
     *
     * <p>Note: the prefix MUST end in the CONSUL_PATH_SEPARATOR ('/').
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
     * <p>If there are no keys with the given prefix, then an empty {@link ComponentProperties}.
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
                    answer.put(remainder, v.getValueAsString().orElse(""));
                }
            }
        }
        return answer;
    }

    /**
     * Look up a single Consul Key/Value. The value will be returned, or null if the given key is not present.
     *
     * <p>Note that the property value may be null.
     *
     * @param propertyKey the key to look up in the Consul Key/Value database
     * @return the value for the given key, or null if the key is not present
     */
    @Nullable
    private String getComponentKeyValue(String propertyKey) {
        return consulService.getValueAsString(propertyKey).orElse(null);
    }

    /**
     * Set a single Consul Key/Value.
     *
     * <p>The propertyValue may be null. Clients will need to provide a default value as appropriate.
     * As a general rule, we treat configuration properties with a <i>null</i> value as indistinguishable from a
     * property with <i>no</i> value.
     *
     * @param propertyKey the key to look up in the Consul Key/Value database
     * @param propertyValue the new value to store in the Consul Key/value database
     */
    private void setComponentKeyValue(@Nonnull String propertyKey, @Nullable String propertyValue) {
        consulService.putValue(propertyKey, propertyValue);
    }

    /**
     * Delete a single Consul Key/Value.
     *
     * @param propertyKey the key to delete in the Consul Key/Value database
     */
    private void deleteComponentKeyValue(@Nonnull String propertyKey) {
        consulService.deleteKey(propertyKey);
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
        return getComponentsWithPrefix(getComponentInstancesKey(componentType));
    }

    /**
     * Gather the current state from each running VMT Component Instance. We will do this by
     * mapping the Consul health status to a turbo component state
     *<pre>
     *    consul health result     XL component status
     *    --------------------     ------------------
     *    passing                  RUNNING
     *    critical - w/output      UNHEALTHY (presence of output means our check is working)
     *    crticial - w/o output    UNKNOWN (no output means we haven't reported a check result yet)
     *</pre>
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
            final String healthOutput = checkResult.getOutput().orElse(ComponentState.UNKNOWN.name());
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
    public Map<String, HealthCheck> getAllComponentsHealth() {
        Set<String> discoveredComponents = getKnownComponents();
        Map<String, HealthCheck> retVal = new HashMap<>();
        for (String componentInstance : discoveredComponents) {
            for (HealthCheck check : consulService.getServiceHealth(componentInstance)) {
                retVal.put(check.getServiceId().orElse(componentInstance), check);
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
     *
     * <p>Since diagnostics collection is resource intensive, only allow one such operation
     * at a time within Clustermgr.
     * TODO: When we move to horizontal scaling of clustermgr, this should be implemented
     * using a shared, persistent lock (such as provided by Consul).
     *
     * <p>Note that the responseOutput stream is not closed on exit.
     *
     * <p>TODO: consider moving this to its own class, e.g. ClusterDiagnosticsService
     * TODO: look into the "offset" parameter to the diagnosticZip.write() method
     *
     * @param responseOutput the output stream onto which the zipfile is written.
     * @throws HttpClientErrorException if there is already a diagnostics operation in progress
     * @throws RuntimeException for errors creating URI or fetching data
     * @throws IOException if there is an IO error dealing with the zip files on the oupput stream
     */
    public void collectComponentDiagnostics(OutputStream responseOutput) throws IOException {
        log.debug("locking diagnosticLock");
        if (!diagnosticsLock.tryLock()) {
            log.debug(DIAGNOSTIC_OPERATION_BUSY);
            throw new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS,
                DIAGNOSTIC_OPERATION_BUSY);
        }
        try {
            ZipOutputStream diagnosticZip = new ZipOutputStream(responseOutput);
            String acceptTypes = MediaType.toString(Arrays.asList(
                MediaType.parseMediaType("application/zip"),
                MediaType.APPLICATION_OCTET_STREAM));
            final StringBuilder errorMessagesBuild = new StringBuilder();
            visitActiveComponents("/diagnostics", acceptTypes, (componentId, entity) -> {
                log.info(" --- Begin diagnostic collection for {}", componentId);
                try (InputStream componentDiagnosticStream = entity.getContent()) {
                    // create a new .zip file entry on the zip output stream
                    String zipFileName = componentId + "-diags.zip";
                    log.info(" --- adding zip file named: " + zipFileName);
                    diagnosticZip.putNextEntry(new ZipEntry(zipFileName));
                    // copy the target .zip diagnostic file onto the .zip output stream
                    IOUtils.copy(componentDiagnosticStream, diagnosticZip);
                } catch (EOFException e) {
                    log.error(" --- EOF copying diags for " + componentId);
                    // throw unchecked exception to break out of the visitActiveComponents lambda
                    throw new CollectDiagsEofException("EOF copying diagnostics for " +
                        componentId);
                } catch (IOException e) {
                    // log the error and continue to the next service in the list of services
                    log.error(" --- Error reading diagnostic stream", e);
                    errorMessagesBuild
                        .append(componentId)
                        .append(NEXT_LINE);
                } finally {
                    log.debug(" --- closing zip entry");
                    try {
                        diagnosticZip.closeEntry();
                    } catch (EOFException e) {
                        log.error(" --- EOF closing diagnostics zipfor " + componentId);
                        // throw unchecked exception to break out of the visitActiveComponents lambda
                        throw new CollectDiagsEofException("EOF copying diagnostics for " +
                            componentId);
                    } catch (IOException e) {
                        log.error(" --- Error closing diagnostic .zip for " + componentId, e);
                    }
                }
            }, errorMessagesBuild);
            getRsyslogDiags(diagnosticZip, acceptTypes, errorMessagesBuild);
            insertDiagEnvironmentSummaryFile(diagnosticZip, errorMessagesBuild);
            insertDiagsSummaryFile(diagnosticZip, errorMessagesBuild);

            // finished all instances of all known components - finish the aggregate output .zip file
            // stream
            log.info("finishing diagnosticZip");
            // NB. finish() doesn't close the underlying stream. I.e. do NOT wrap in try
            // (resources){}
            diagnosticZip.finish();
        } catch (EOFException | CollectDiagsEofException e) {
            // EOF means that the output stream was closed; just exit
            log.error("Diags EOF exception: " + e.getLocalizedMessage());
        } finally {
            log.debug("unlocking diagnosticLock");
            diagnosticsLock.unlock();
        }
    }

    private String getConsulNamespace() {
        String consulNamespacePrefix = consulService.getConsulNamespacePrefix();
        // If consulNamespacePrefix is not empty, then consulNamespace is enabled for multi tenant
        // Consul.
        if (!StringUtils.isEmpty(consulService.getConsulNamespacePrefix())) {
            // If not empty, consulNamespacePrefix is constructed by consulNamespace + "/".
            return consulNamespacePrefix.substring(0, consulNamespacePrefix.length() - 1);
        } else {
            return "";
        }
    }

    /**
     * Checked exception used to leave the collectComponentDiagnostics visitActiveComponents loop.
     */
    private static class CollectDiagsEofException extends RuntimeException {
        CollectDiagsEofException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * Specialized request for the rsyslog to retrieve all the logs.
     * The reason for an independent method lies in fact that rsyslog is not a component
     * managed by Consul, so has a different configuration.
     *
     * <p>Test: The test is very much an integration here.
     *
     * @param diagnosticZip The diagnostic zip stream.
     * @param acceptResponseTypes The HTTP header for accepted responses.
     * @param errorMessageBuilder an accumulator for multiple error messages
     * @throws EOFException from IOUtils.copy(),
     */
    private void getRsyslogDiags(ZipOutputStream diagnosticZip, String acceptResponseTypes,
                                 @Nonnull final StringBuilder errorMessageBuilder) throws EOFException {
        // Handle the rsyslog
        // It is not part of the consul-managed set of components.
        String componentName = "rsyslog";
        URI requestUri = getComponentInstanceUri(componentName, 8080, "/diagnostics");
        HttpGet request = new HttpGet(requestUri);
        request.addHeader("Accept", acceptResponseTypes);
        log.info(" - fetching rsyslog diagnostics");
        try (CloseableHttpClient httpclient =  HttpClientBuilder.create()
            .setDefaultRequestConfig(requestConfig).build()) {
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
                    } catch (EOFException e) {
                        log.error("Output Stream EOF copying rsyslog diags zip.");
                        throw new ZipOutputEofExeption(e);
                    } catch (IOException e) {
                        // log the error and continue to the next service in the list of services
                        log.error(componentName + " --- Error reading diagnostic stream", e);
                        errorMessageBuilder
                            .append(componentName)
                            .append(NEXT_LINE);
                    } finally {
                        log.debug(componentName + " --- closing zip entry");
                        try {
                            diagnosticZip.closeEntry();
                        } catch (EOFException e) {
                            log.error("Output Stream EOF closing rsyslog diags zip.");
                            throw new ZipOutputEofExeption(e);
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
            final String message = "Exception connecting to rsyslog. Diags will not have logs. Error: "
                + e.getMessage();
            log.error(message);
            errorMessageBuilder.append(message);
        }

    }

    /**
     * Insert an environment summary file into the diagnostics.
     * For details about the contents of the file see: {@link DiagEnvironmentSummary}.
     *
     * @param diagnosticZip The output stream.
     * @param errorMessagesBuild Builder for error messages encountered during the file insertion.
     */
    private void insertDiagEnvironmentSummaryFile(@Nonnull final ZipOutputStream diagnosticZip,
                                          @Nonnull final StringBuilder errorMessagesBuild) {
        final ZipEntry zipEntry = new ZipEntry(ENVIRONMENT_SUMMARY_TXT);
        log.info(" - recording diagnostics environment summary");
        try {
            diagnosticZip.putNextEntry(zipEntry);
            final byte[] data = diagEnvironmentSummary.getDiagSummary().getBytes();
            diagnosticZip.write(data, 0, data.length);
        } catch (EOFException e) {
            log.error("Output Stream EOF inserting diag environment summary.");
            throw new ZipOutputEofExeption(e);
        } catch (IOException e) {
            log.error("Error adding environment summary zip entry", e);
            errorMessagesBuild.append("Failed to add environment summary due to error: ")
                .append(e.getMessage())
                .append("\n");
        } finally {
            try {
                diagnosticZip.closeEntry();
            } catch (EOFException e) {
                log.error("Output Stream EOF closing diag environment summary.");
                throw new ZipOutputEofExeption(e);
            } catch (IOException e) {
                errorMessagesBuild.append("Failed to close environment summary entry to error: ")
                    .append(e.getMessage())
                    .append("\n");
                log.error("Error closing environment summary zip entry", e);
            }
        }
    }

    // Insert summary file:
    // If all service component were assembled successfully, empty file with name "DiagsSummary-Success.txt".
    // Otherwise, file name is "DiagsSummary-Fail.txt" and it contains failed service component names.
    @VisibleForTesting
    void insertDiagsSummaryFile(@Nonnull final ZipOutputStream diagnosticZip,
                                @Nonnull final StringBuilder errorMessages) {
        log.info(" - inserting diagnostics summary");
        final boolean isSuccessful = errorMessages.toString().isEmpty();
        final String summaryFileName = isSuccessful ? DIAGS_SUMMARY_SUCCESS_TXT : DIAGS_SUMMARY_FAIL_TXT;
        final ZipEntry zipEntry = new ZipEntry(summaryFileName);
        try {
            diagnosticZip.putNextEntry(zipEntry);
            if (!isSuccessful) {
                final byte[] data = errorMessages.toString().getBytes();
                diagnosticZip.write(data, 0, data.length);
            }
        } catch (EOFException e) {
            log.error("Output Stream EOF inserting diags summary.");
            throw new ZipOutputEofExeption(e);
        } catch (IOException e) {
            log.error("Error adding diagnostic stream", e);
        } finally {
            log.debug(" closing zip entry");
            try {
                diagnosticZip.closeEntry();
            } catch (EOFException e) {
                log.error("Output Stream EOF closing diags summary.");
                throw new ZipOutputEofExeption(e);
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
     * <p>Test: The test is very much an integration here.
     *
     * @param out The output stream.
     * @param acceptResponseTypes The HTTP header for accepted responses.
     */
    public void getRsyslogProactive(OutputStream out, String acceptResponseTypes) {
        // Handle the rsyslog
        // It is not part of the consul-managed set of components.
        String componentName = "rsyslog";
        URI requestUri = getComponentInstanceUri(componentName, 8080, "/proactive");
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
     * <p>Note that this process is single-threaded and blocks for each request.
     *
     * <p>TODO: consider parallel processing for the list of requests.
     * @param requestPath the URL to GET from each active component instance
     * @param acceptResponseTypes the http response types to accept from this request
     * @param responseEntityProcessor the processor for the response.getEntity() after the http request completes.
     * @param errorMessageBuilder an accumulator for multiple error messages
     */
    private void visitActiveComponents(String requestPath,
                                       String acceptResponseTypes,
                                       ResponseEntityProcessor responseEntityProcessor,
                                       @Nonnull final StringBuilder errorMessageBuilder) {
        Table<String, String, RegisteredComponent> instancesByTypeAndId = componentRegistry.getRegisteredComponents();
        final MutableInt curComponentCounter = new MutableInt(1);
        instancesByTypeAndId.rowMap().forEach((componentType, instances) -> {
            log.info("applying REST REQUEST {} to component type: {} [{} of {}]", requestPath,
                componentType, curComponentCounter.getAndIncrement(), instancesByTypeAndId.size());
            final MutableInt curInstanceCounter = new MutableInt(1);
            instances.forEach((instanceId, instanceInfo) -> {
                log.info("- instance {} [{} of {}]", instanceId, curInstanceCounter.getAndIncrement(), instances.size());
                if (instanceInfo.getComponentHealth() == ComponentHealth.CRITICAL) {
                    log.warn("Skipping instance {} because component status is critical.", instanceId);
                } else {
                    try {
                        URI requestUri = instanceInfo.getUri(requestPath);
                        HttpGet request = new HttpGet(requestUri);
                        request.addHeader("Accept", acceptResponseTypes);
                        sendRequestToComponent(instanceId, request, responseEntityProcessor,
                            Optional.of(errorMessageBuilder));
                    } catch (URISyntaxException e) {
                        log.error("Failed to construct URI for instance {}. Error: {}", instanceId, e.getMessage());
                    }
                }
            });
        });
    }

    /**
     * Send an HTTP GET to a given VMT Component Instance and process the response.
     *
     * <p>Note: we treat an http request as a non-fatal error; most likely cause is the target
     * component is not currently running.
     *
     * @param componentName the VMT Component service to send the request to
     * @param request the HttpUriRequest to execute
     * @param responseEntityProcessor the processor for the response.getEntity() after the http request completes.
     * @param errorMessagBuilder an accumulator for errors in case multiple are found
     */
    private void sendRequestToComponent(String componentName,
                                        HttpUriRequest request,
                                        ResponseEntityProcessor responseEntityProcessor,
                                        @Nonnull final Optional<StringBuilder> errorMessagBuilder) {
        final Instant start = Instant.now();
        // open an HttpClient link
        try (CloseableHttpClient httpclient = HttpClientBuilder.create()
            .setDefaultRequestConfig(requestConfig).build()) {
            // execute the request
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                log.debug(" --- response status: {}", response.getStatusLine());
                // process the response entity
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    responseEntityProcessor.process(componentName, entity);
                } else {
                    appendFailedServiceName(componentName, errorMessagBuilder);
                    // log the error and continue to the next service in the list of services
                    log.error(" --- missing response entity");
                }
            }
            long timeElapsed = Duration.between(start, Instant.now()).toMillis();
            log.info(" --- {} elapsed time: {}", componentName, timeElapsed / 1000.0);
        } catch (IOException e) {
            appendFailedServiceName(componentName, errorMessagBuilder);
            // log the error and continue to the next service
            log.error(" --- Error executing http request {}: {}",
                    request.getURI().toString(),
                    e.getMessage());
        }
        // TODO: Log upload times as prometheus metrics - OM-51340
        long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        log.info(" --- {} elapsed time: {}", componentName,
            timeElapsed / 1000.0);
    }

    @VisibleForTesting
    private void appendFailedServiceName(@Nonnull final String componentName,
                                         @Nonnull final Optional<StringBuilder> errorMessageBuilder) {
        errorMessageBuilder.ifPresent(builder -> builder
            .append(componentName)
            .append(NEXT_LINE));
    }

    /**
     * Returns the component instance URI.
     *
     * @param componentInstance The dnsname or address of the component.
     * @param port The port to access the component on.
     * @param requestPath The request path.
     * @return The URI.
     */
    @Nonnull
    private URI getComponentInstanceUri(@Nonnull String componentInstance,
                                        final int port,
                                        @Nonnull final String requestPath) {
        // create a request from the given path and the target component instance properties
        URI requestUri;
        try {
            requestUri = new URIBuilder()
                    .setHost(componentInstance)
                    .setPort(port)
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
                String instancePropertiesKey = getComponentInstancePropertiesKey(componentType, instanceId);
                ComponentProperties instanceProperties = getComponentPropertiesWithPrefix(instancePropertiesKey);
                answer.addComponentInstance(instanceId, componentType, version, instanceProperties);
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
     * @param newConfiguration an {@link ClusterConfiguration} to completely replace the current
     *                         configuration.
     * @return the new configuration, read back from the Consul key/value store.
     */
    @Nonnull
    public ClusterConfiguration setClusterConfiguration(
        @Nonnull ClusterConfiguration newConfiguration) {

        // clear the current configuration (gulp!)
        consulService.deleteKey(getVMTurboBaseKey());

        // Process the defaults for the given component types
        for (Map.Entry<String, ComponentProperties> defaultEntry :
            newConfiguration.getDefaults().getComponents().entrySet()) {
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
        }
        return getClusterConfiguration();
    }

    /**
     * Return the default {@link ComponentProperties} for the given component type.
     *
     * @param componentType the component type for which to fetch the default ComponentProperties
     * @return a {@link ComponentProperties} object containing all default configuration properties for the given
     * component type.
     */
    @Nonnull
    public ComponentProperties getDefaultPropertiesForComponentType(@Nonnull String componentType) {
        return getComponentDefaultProperties(componentType);
    }

    /**
     * Return the current {@link ComponentProperties} for the given component type.
     *
     * @param componentType the component type for which to fetch the default ComponentProperties
     * @return a {@link ComponentProperties} object containing all default configuration properties for the given
     * component type.
     */
    @Nonnull
    public ComponentProperties getLocalPropertiesForComponentType(@Nonnull String componentType) {
        return getComponentLocalProperties(componentType);
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
    @Nonnull
    public ComponentProperties putDefaultPropertiesForComponentType(
            @Nonnull String componentType,
            @Nonnull ComponentProperties newComponentDefaultProperties) {

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
     * Replace the local {@link ComponentProperties} for the given component type. Begin with
     * the Global Default properties as a base, and then overlay the new Default properties on
     * top of that.
     *
     * @param componentType the component type for which to store the local ComponentProperties
     * @param newComponentDefaultProperties the new values for the local {@link ComponentProperties}
     *                                      for the given component type
     * @return a {@link ComponentProperties} object containing all default configuration properties
     * for the given component type.
     */
    @Nonnull
    public ComponentProperties putLocalPropertiesForComponentType(
        @Nonnull String componentType,
        @Nonnull ComponentProperties newComponentDefaultProperties) {

        // ensure that the component type exists; if already exists, this is a no-op
        addComponentType(componentType);

        // store those as the new component local properties
        final String componentLocalPropertiesKeyPrefix =
            getComponentLocalPropertiesKey(componentType);
        setAllValues(componentLocalPropertiesKeyPrefix, newComponentDefaultProperties);

        // return the updated component default properties
        return getComponentLocalProperties(componentType);
    }

    /**
     * Return the Effective {@link ComponentProperties} for the given component instance / type.
     * The effective properties combine the default properties for the component type (lowest
     * priority), local properties for the component type, and instance properties for the
     * component type and instance.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentProperties} object containing the merged set of
     * configuration properties for the given component instance.
     */
    @Nonnull
    public ComponentProperties getEffectiveInstanceProperties(@Nonnull String componentType,
                                                              @Nonnull String componentInstanceId) {
        final ComponentProperties componentProperties =
            getDefaultPropertiesForComponentType(componentType);
        componentProperties.putAll(getComponentLocalProperties(componentType));
        componentProperties.putAll(getComponentInstanceProperties(componentType,
            componentInstanceId));
        return componentProperties;
    }

    /**
     * Return the {@link ComponentProperties} for the given component instance / type.
     *
     * @param componentType type for the given component instance.
     * @param componentInstanceId unique id for the given component instance
     * @return a {@link ComponentProperties} object containing all of the configuration properties for the given
     * component instance.
     */
    @Nonnull
    public ComponentProperties getComponentInstanceProperties(@Nonnull String componentType,
                                                              @Nonnull String componentInstanceId) {

        final String instancePropertiesKey =
                getComponentInstancePropertiesKey(componentType, componentInstanceId);
        return getComponentPropertiesWithPrefix(instancePropertiesKey);
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
     * Return the default value for the given property for the given component type.
     * If there is no property by that name, then return null.
     *
     * @param componentType the component type of to access
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return value of the configureation property for the given component type
     */
    public String getDefaultComponentProperty(
            String componentType,
            String propertyName) {
        String defaultPropertyKey = getComponentDefaultPropertyKey(componentType, propertyName);
        return getComponentKeyValue(defaultPropertyKey);
    }

    /**
     * Return the local value for the given property for the given component type.
     * If there is no property by that name, then return null.
     *
     * @param componentType the component type of to access
     * @param propertyName the value of the named configuration property, or null if there is none.
     * @return value of the configureation property for the given component type
     */
    public String getLocalComponentProperty(
            String componentType,
            String propertyName) {
        String defaultPropertyKey = getComponentDefaultPropertyKey(componentType, propertyName);
        return getComponentKeyValue(defaultPropertyKey);
    }

    /**
     * Return the local value for the given property for the given component type.
     * If there is no property by that name, then return null.
     *
     * @param componentType the component type of to access
     * @param propertyName the name of the property to set
     * @param newValue the value to set as local property
     * @return value of the configuration property for the given component type
     */
    public String putComponentLocalProperty(
            String componentType,
            String propertyName,
            String newValue) {
        String localPropertyKey = getComponentLocalPropertyKey(componentType, propertyName);
        setComponentKeyValue(localPropertyKey, newValue);
        return getLocalComponentProperty(componentType, localPropertyKey);
    }

    /**
     * Delete the local value for the given property for the given component type.
     * Returns the newly-uncovered "default" value.
     *
     * @param componentType the component type of to access
     * @param propertyName the configuration property to delete
     * @return the default value of the configureation property for the given component type
     */
    public String deleteComponentLocalProperty(
            String componentType,
            String propertyName) {
        String localPropertyKey = getComponentLocalPropertyKey(componentType, propertyName);
        deleteComponentKeyValue(localPropertyKey);
        return getDefaultComponentProperty(componentType, propertyName);
    }

    /**
     * Return the value for the given property for the given component instance. If there is no property by that name,
     * then return null.
     *
     * <p>See the {@literal COMPONENT_INSTANCE_PROPERTY_FORMAT} format for the full key to be used in Consul.
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
     * <p>Add the version for the component type to the default properties.
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
     * Return the current {@link ComponentProperties} for the given component type.
     *
     * <p>Add the version for the component type to the default properties.
     *
     * @param componentType the component type for the requested defaults
     * @return the default {@link ComponentProperties} for the given component type
     */
    @Nonnull
    private ComponentProperties getComponentLocalProperties(String componentType) {
        String localPropertiesKey = getComponentLocalPropertiesKey(componentType);
        return getComponentPropertiesWithPrefix(localPropertiesKey);
    }

    /**
     * Fetch the version string for this component from the the k/v store.
     * The key for the version string is, for now, "${component_type}-1". This is a hold-over
     * from the naming for the component key/value store naming.
     *
     * @param componentType the component-type to look up
     * @return the version string for this component type, or "unknown" if not found
     */
    @Nonnull
    private String getComponentVersionString(@Nonnull final String componentType) {
        Optional<String> componentVersion = consulService.getValueAsString(
            componentType + "-1/" + COMPONENT_VERSION_KEY);
        return componentVersion.isPresent() ? componentVersion.get() : UNKNOWN_VERSION_STRING;
    }

    /**
     * Base key for all VMT Key/Value information.
     *
     * @return the base (root) key for the configuration key/value segment
     */
    private static String getVMTurboBaseKey() {
        return VMTURBO_BASE_FORMAT;
    }

    /**
     * Base key for all VMT Components information.
     *
     * @return the path element for "components"
     */
    private static String getComponentsBaseKey() {
        return COMPONENTS_BASE_FORMAT;
    }

    /**
     * Key for accessing a given component type.
     *
     * @param componentType type of component to access
     * @return the key for accessing a component type properties
     */
    private static String getComponentKey(String componentType) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_FORMAT, componentType);
    }

    /**
     * Key for accessing defaults of a given component type.
     *
     * @param componentType type of component to access
     * @return the composite key to access the default properties of a component type
     */
    private static String getComponentDefaultsKey(String componentType) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_DEFAULTS_FORMAT, componentType);
    }

    /**
     * Key for accessing local properties of a given component type.
     *
     * @param componentType type of component to access
     * @return the composite key for the local properties for the given component type
     */
    private static String getComponentLocalPropertiesKey(String componentType) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_LOCAL_PROPERTIES_FORMAT, componentType);
    }

    /**
     * Key for accessing defaults of a given component type.
     *
     * @param componentType type of component to access
     * @param propertyName the name of the property to access
     * @return the key for accessing a default property with the given componentType and propertyName
     */
    private static String getComponentDefaultPropertyKey(String componentType, String propertyName) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_DEFAULTS_PROPERTY_FORMAT, componentType, propertyName);
    }

    /**
     * Key for accessing local property value of a given component type.
     *
     * @param componentType type of component to access
     * @param propertyName name of property value to access
     * @return the composite key for the local property value given the component type and
     * property name
     */
    private static String getComponentLocalPropertyKey(String componentType, String propertyName) {
        checkComponentTypeValid(componentType);
        return String.format(COMPONENT_LOCAL_PROPERTY_FORMAT, componentType, propertyName);
    }

    /**
     * Key for accessing instances of a given component type.
     *
     * @param componentType type of component to access
     * @return the key for the root of the component instances config properties
     */
    private static String getComponentInstancesKey(String componentType) {
        return String.format(COMPONENT_INSTANCES_FORMAT, componentType);
    }

    /**
     * Key for accessing properties of a given instance of a given component type.
     *
     * @param componentType type of component to access
     * @param componentInstanceId instance id of the specific component to access
     * @return the key for accessing all the configuration properties of a component instance
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
     * @return the composite key for accessing a particular configuration property of the given
     * component instance and type
     */
    private static String getComponentInstancePropertyKey(
            String componentType,
            String componentInstanceId,
            String propertyName) {
        return String.format(COMPONENT_INSTANCE_PROPERTY_FORMAT, componentType, componentInstanceId, propertyName);
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
     * @return whether the key component is valid or not
     */
    private static boolean isValidKeyComponent(@Nonnull String keyComponent) {
        return (StringUtils.isNotBlank(keyComponent) &&
                !StringUtils.containsAny(keyComponent, CONSUL_PATH_SEPARATOR, PROPERTY_KEY_SEPARATOR));
    }

    /**
     * Export diagnostics to to {@link #DIAGNOTICS_OUTPUT_FILE_PATH} and then to upload.vmturbo.com
     * using Curl. Write to a file because:
     * <ul>
     *     <li>Both classic and metron are using curl to upload files.
     *     <li>The vmtupload.cgi in upload.vmturbo.com doesn't support stream upload!!!
     * </ul>
     * TODO: integration test
     *
     * @param httpProxy value object with proxy settings
     * @return true if export is successful
     */
    public boolean exportDiagnostics(final HttpProxyConfig httpProxy) {
        try {
            prepareDiagnosticFilesDirectory();
        } catch (IOException e) {
            log.error("Error preparing diagnostics directory.", e);
            return false;
        }
        String consulNamespace = getConsulNamespace();
        if (!StringUtils.isEmpty(consulNamespace)) {
            log.info("Exporting diagnostics for namespace '{}'.", consulNamespace);
        }
        final String fileName = DIAGNOTICS_OUTPUT_FILE_PATH + diagEnvironmentSummary.getDiagFileName();
        final File diagsFile = new File(fileName);

        // 1. write to file
        final Instant start = Instant.now();
        if (writeDiagnosticsFileToDisk(diagsFile)) {
            String[] curlArgs = getCurlArgs(fileName, httpProxy);

            // 2. invoke curl command to upload, se.g.:
            // curl -x 10.10.172.84:3128 -F ufile=@test.log \
            //                            http://10.10.168.175/cgi-bin/vmturboupload.cgi
            // this runner blocks until the process completes

            log.info("Running CURL command to upload diagnostics.");
            int rc = osCommandProcessRunner.runOsCommandProcess(CURL_COMMAND, curlArgs);
            boolean success = rc == 0;

            if (!success) {
                log.error("Failure in uploading diagnostic file: {}", fileName);
            } else {
                long timeElapsed = Duration.between(start, Instant.now()).toMillis();
                log.info("Successfully upload diagnostics file: {}, time (secs): {}", fileName,
                        timeElapsed / 1000.0);
            }
            return success;
        }
        log.error("Write to disk failed.");
        return false;
    }

    /**
     * Prepare the directory for diagnostic files: DIAGNOTICS_OUTPUT_FILE_PATH.
     * If the directory exists, then clear it. If the directory does not exist, create it.
     *
     * @throws IOException if there is an error in listing the files or deleting a prior
     * diagnostics file
     */
    private void prepareDiagnosticFilesDirectory() throws IOException {
        Path diagsPath = Paths.get(DIAGNOTICS_OUTPUT_FILE_PATH);
        if (Files.exists(diagsPath)) {
            try (DirectoryStream<Path> filesStream = Files.newDirectoryStream(diagsPath)) {
                for (Path filePath: filesStream) {
                    log.debug("Deleting prior diagnostic file: {}", filePath.toString());
                    Files.delete(filePath);
                }
            }
        } else {
            Files.createDirectory(diagsPath);
        }
    }

    /**
     * Stream the component diagnostics to a disk file. This allows the file
     * to be the subject of an external "curl" command.
     *
     * @param diagsFile the file onto which the
     * @return true if written successfully; false if there's an IO error
     */
    private boolean writeDiagnosticsFileToDisk(@Nonnull final File diagsFile) {
        Instant start = Instant.now();
        try (FileOutputStream diagsFileOutputStream = new FileOutputStream(diagsFile)) {
            log.info("Diagnostics file: {}", diagsFile.getAbsolutePath());
            collectComponentDiagnostics(diagsFileOutputStream);
            diagsFileOutputStream.flush();
        } catch (IOException e) {
            log.error("Failed to write diagnostics file to {}. Error message: {}",
                diagsFile.getPath(), e.getMessage() );
            return false;
        }
        long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        log.info("done writing diag file to disk. time in secs: {}", timeElapsed / 1000.0);
        return true;
    }

    // get curl command based on if proxy is available or not
    @VisibleForTesting
    String[] getCurlArgs(@Nonnull final String fileName,
                         @Nonnull final HttpProxyConfig httpProxy) {
        final List<String> argsList = Lists.newArrayList("-F", "ufile=@" + fileName,
            UPLOAD_VMTURBO_COM_URL);
        // it's mainly not writing proxy password to log file.
        final List<String> argsListDebug = Lists.newArrayList(argsList);
        if (httpProxy.getIsProxyEnabled()) {
            String hostPortTuple = httpProxy.getProxyHostAndPortTuple();
            if (StringUtils.isNotBlank(hostPortTuple)) {
                // using proxy
                argsList.add("-x");
                argsListDebug.add("-x");

                String userNamePasswordTuple = httpProxy.getUserNameAndPasswordTuple();
                if (StringUtils.isNotBlank(userNamePasswordTuple)) {
                    argsList.add(Strings.concat(userNamePasswordTuple, AT, hostPortTuple));
                    argsListDebug.add(Strings.concat(httpProxy.getUserNameAndHiddenPasswordTuple(), AT, hostPortTuple));
                } else {
                    argsList.add(hostPortTuple);
                    argsListDebug.add(hostPortTuple);
                }
            }
        }
        log.debug("curl arguments: " + argsListDebug);
        return argsList.toArray(new String[0]);
    }

    /**
     * Process the response (an {@link HttpEntity}), to an Http query for a given
     * component instance.
     */
    private interface ResponseEntityProcessor {
        void process(String componentId, HttpEntity entity);
    }

    /**
     * This unchecked exception is used to break out of the zip filestream processing lambda on
     * output zip stream EOF.
     */
    private static class ZipOutputEofExeption extends RuntimeException {
        ZipOutputEofExeption(Exception e) {
            super(e);
        }
    }
}
