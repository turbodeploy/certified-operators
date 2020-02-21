package com.vmturbo.clustermgr;

import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.CatalogClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.kv.Value;

import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriUtils;

import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.common.health.ConsulHealthcheckRegistration;

/**
 * {@link ConsulService} provides utility methods that facility:
 * <ul>
 *     <li>access to the key/value store</li>
 *     <li></li>
 * </ul>
 */
@Component
public class ConsulService {

    private final String consulHost;
    private final int consulPort;
    private String consulNamespacePrefix;

    @org.springframework.beans.factory.annotation.Value("${consulMaxRetrySecs:60}")
    private int consulMaxRetrySecs;

    private final Logger logger = LogManager.getLogger();

    // lazy-fetched handle for the Consul client API
    private Consul consulClientApi;

    /**
     * Create a new {@link ConsulService}.
     *
     * @param consulHost            Consul host.
     * @param consulPort            Consul port.
     * @param consulNamespacePrefix Given consul namespace prefix to be prepended to keys.
     */
    public ConsulService(@Nonnull final String consulHost, final int consulPort,
                         @Nonnull final String consulNamespacePrefix) {
        this.consulHost = Objects.requireNonNull(consulHost);
        this.consulPort = consulPort;
        this.consulNamespacePrefix = consulNamespacePrefix;
    }

    /**
     * Fetch a list of Consul Key/Value keys that begin with the given stem.
     * If there are none, an empty list will be returned.
     *
     * @param keyStem the key stem to match against the key/value store
     * @return a list of keys defined in the key/value store that begin with
     *          the given stem, or an empty list if there are none.
     */
    public @Nonnull List<String> getKeys(@Nonnull String keyStem) {
        try {
            return getConsulKeyValueClient().getKeys(fullKey(keyStem));
        } catch (ConsulException ce) {
            if (ce.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw ce;
            }
            // no keys found - return an empty list
            return new ArrayList<>();
        }
    }

    public void putValue(String keyToPut) {
        getConsulKeyValueClient().putValue(fullKey(keyToPut));
    }

    public void putValue(String key, String value) {
        getConsulKeyValueClient().putValue(fullKey(key), value);
    }

    /**
     * Get the Consul key/value api client handle.
     *
     * @return the Consul key/value API Client Handle
     */
    private synchronized KeyValueClient getConsulKeyValueClient() {
        return getConsulApi().keyValueClient();
    }

    /**
     * Get the Consul catalog api client handle for dealing with registration / lookup of services.
     *
     * @return the Consul catalog API Client Handle
     */
    private synchronized CatalogClient getConsulCatalogClient() {
        return getConsulApi().catalogClient();
    }

    /**
     * Lazy-fetch a handle for the Consul client API.
     *
     * @return a Consul API client.
     */
    private Consul getConsulApi() {
        if (consulClientApi == null) {
            HostAndPort hostAndPort = HostAndPort.fromParts(consulHost, consulPort);
            try {
                consulClientApi = RetriableOperation.newOperation(() -> Consul.builder()
                                .withHostAndPort(hostAndPort)
                                .build())
                        .retryOnException(e -> {
                            if (e instanceof ConsulException) {
                                Throwable cause = e.getCause();
                                if (cause instanceof SocketTimeoutException
                                        || cause instanceof ConnectException
                                        || cause instanceof ConnectTimeoutException
                                        || cause instanceof UnknownHostException) {
                                    return true;
                                }
                            }
                            return false;
                        })
                        .run(consulMaxRetrySecs, TimeUnit.SECONDS);
            } catch (RetriableOperationFailedException | TimeoutException e) {
                // rethrow
                throw new RuntimeException(e);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        return consulClientApi;
    }

    public List<Value> getValues(String keyStem) {
        return getConsulKeyValueClient().getValues(fullKey(keyStem));
    }

    /**
     * Information about a single instance of a component, containing utility methods to help access
     * the component over HTTP.
     */
    public static class ComponentInstance {
        private final String id;
        private final String address;
        private final int port;
        private final String routePrefix;

        @VisibleForTesting
        ComponentInstance(final CatalogService catalogService) {
            this.id = catalogService.getServiceId();
            this.address = catalogService.getServiceAddress();
            this.port = catalogService.getServicePort();
            this.routePrefix = catalogService.getServiceTags() == null ? "" :
                catalogService.getServiceTags().stream()
                    .map(ConsulHealthcheckRegistration::decodeInstanceRoute)
                    .filter(java.util.Optional::isPresent)
                    .map(java.util.Optional::get)
                    .findFirst()
                    .orElse("");
        }

        /**
         * Get the ID of this particular instance.
         *
         * @return The instance ID.
         */
        @Nonnull
        public String getId() {
            return id;
        }

        /**
         * Get a URI to access a particular HTTP endpoint of the instance.
         *
         * @param requestPath The path within the component (e.g. /health).
         * @return The {@link URI} to use to make HTTP requests to that path.
         */
        @Nonnull
        public URI getUri(@Nonnull final String requestPath) {
            // create a request from the given path and the target component instance properties
            URI requestUri;
            try {
                requestUri = new URIBuilder()
                    .setHost(address)
                    .setPort(port)
                    .setScheme("http")
                    .setPath(routePrefix + requestPath)
                    .build();
            } catch (URISyntaxException e) {
                // log the error and continue to the next service in the list of services
                throw new RuntimeException(" --- Error creating diagnostic URI to query component", e);
            }
            return requestUri;
        }

        @Override
        public String toString() {
            return "instance " + id + " (ip: " + address + " and port: " + port + ")";
        }
    }

    /**
     * Get information about all instances of al services.
     *
     * @return A map from (component type) -> ({@link ComponentInstance} for every instance of the component).
     */
    @Nonnull
    public Map<String, List<ComponentInstance>> getAllServiceInstances() {
        final CatalogClient catalogClient = getConsulCatalogClient();
        final boolean isConsulNamespacePrefixEmpty = Strings.isEmpty(consulNamespacePrefix);
        // TODO Find out a way to query all services by tag when calling getServices.
        // Currently Consul doesn't support listing services by tags:
        // https://github.com/hashicorp/consul/issues/4811
        // https://www.consul.io/api/catalog.html#list-services
        // QueryOptions with tag specification only works for getting a specific service but not
        // working for getting a list of services.
        final Set<String> registeredComponents = catalogClient.getServices().getResponse().entrySet().stream()
            // Include only services that have the turbonomic component tag. This will exclude
            // services like "consul" itself.
            // If consulNamespacePrefix is not empty, include only services whose names contain
            // consulNamespacePrefix
            .filter(entry -> (isConsulNamespacePrefixEmpty ||
                entry.getKey().contains(consulNamespacePrefix)) &&
                entry.getValue().contains(ConsulHealthcheckRegistration.COMPONENT_TAG))
            .map(Entry::getKey)
            .collect(Collectors.toSet());
        final Map<String, List<ComponentInstance>> retMap = new HashMap<>(registeredComponents.size());
        registeredComponents.forEach(service -> {
            try {
                ConsulResponse<List<CatalogService>> instances = catalogClient.getService(service);
                if (instances.getResponse() != null) {
                    // Extract component type from service name without the consulNamespacePrefix.
                    String componentType = service.substring(consulNamespacePrefix.length());
                    retMap.put(componentType, instances.getResponse().stream()
                        .map(ComponentInstance::new)
                        .collect(Collectors.toList()));
                }
            } catch (ConsulException e) {
                logger.error("Failed to get nodes for service " + service, e);
            }
        });
        return retMap;
    }

    public List<HealthCheck> getServiceHealth(String prefix) {
        HealthClient healthClient = getConsulApi().healthClient();
        List<HealthCheck> healthResults = healthClient.getServiceChecks(prefix).getResponse();
        return healthResults;
    }

    /**
     * Deletes the key with its subkeys recursively. No effect if key does not exist.
     *
     * @param key key to remove
     */
    public void deleteKey(String key) {
        getConsulKeyValueClient().deleteKey(fullKey(key));
        try {
            for (String subKey : getConsulKeyValueClient().getKeys(fullKey(key))) {
                getConsulKeyValueClient().deleteKey(fullKey(subKey));
            }
        } catch (ConsulException ce) {
            //skip NotFoundErrors
            if (ce.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw ce;
            }
        }
    }

    public Optional<String> getValueAsString(String instanceNodeKey) {
        return getConsulKeyValueClient().getValueAsString(fullKey(instanceNodeKey));
    }

    public String getValueAsString(String key, String defaultValue) {
        return getConsulKeyValueClient().getValueAsString(fullKey(key)).or(defaultValue);
    }

    public boolean keyExist(String key) {
        try {
            return !getConsulKeyValueClient().getKeys(fullKey(key)).isEmpty();
        } catch (ConsulException e) {
            return false;
        }
    }

    /**
     * Get consulNamespacePrefix.
     *
     * @return constructed consulNamespacePrefix.
     */
    public String getConsulNamespacePrefix() {
        return consulNamespacePrefix;
    }

    @Nonnull
    private String fullKey(@Nonnull final String key) {
        final String fullKey = consulNamespacePrefix + key;
        try {
            return UriUtils.encodeFragment(fullKey, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e); // Should never happen unless UTF-8 encoding support is somehow dropped
            return "";
        }
    }
}
