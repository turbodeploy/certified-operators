package com.vmturbo.clustermgr;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.CatalogClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.kv.Value;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link ConsulService} provides utility methods that facility:
 * <ul>
 *     <li>access to the key/value store</li>
 *     <li></li>
 * </ul>
 */
@Component
public class ConsulService {

    @org.springframework.beans.factory.annotation.Value("${clustermgr.consul.host:consul}")
    private final String consulHost;
    @org.springframework.beans.factory.annotation.Value("${clustermgr.consul.port:8500}")
    private final int consulPort;

    private final Logger logger = LogManager.getLogger();

    // lazy-fetched handle for the Consul client API
    private Consul consulClientApi;

    public ConsulService(@Nonnull final String consulHost, final int consulPort) {
        this.consulHost = Objects.requireNonNull(consulHost);
        this.consulPort = consulPort;
    }

    /**
     * Fetch a list of Consul Key/Value keys that begin with the given stem.
     * If there are none, an empty list will be returned.
     * If there are no keys, then an HTTP NotFoundExcecption will be thrown.
     *
     * @param keyStem the key stem to match against the key/value store
     * @return a list of keys defined in the key/value store that begin with the given stem, or an empty list if there
     * are none.
     */
    public @Nonnull List<String> getKeys(@Nonnull String keyStem) {
        try {
            return getConsulKeyValueClient().getKeys(keyStem);
        } catch (NotFoundException e) {
            // no keys found - return an empty list
            return new ArrayList<>();
        }
    }

    public void putValue(String keyToPut) {
        getConsulKeyValueClient().putValue(keyToPut);
    }

    public void putValue(String key, String value) {
        getConsulKeyValueClient().putValue(key, value);
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
            consulClientApi = Consul.builder()
                    .withHostAndPort(hostAndPort)
                    .build();
        }
        return consulClientApi;
    }

    public List<Value> getValues(String keyStem) {
        return getConsulKeyValueClient().getValues(keyStem);
    }

    /**
     * Return the instance of the given component type with the given instance id, or null if not found.
     * Note that there's no current Consul service to call to retrieve a service by type and id directly, so we
     * need to iterate here.
     *
     * @param componentType type of component to which this instance belongs
     * @param serviceId unique id of this component instance
     * @return either the {@link CatalogService} for the given instance, or null if not found.
     */
    @Nullable
    public CatalogService getServiceById(@Nonnull String componentType, @Nonnull String serviceId) {
        return getConsulCatalogClient().getService(componentType).getResponse().stream()
                .filter(svc -> svc.getServiceId().equals(serviceId))
                .findFirst()
                .orElse(null);
    }

    public List<CatalogService> getService(String componentName){
        return getConsulCatalogClient().getService(componentName).getResponse();
    }

    /**
     * Deletes the key with its subkeys recursively. No effect if key does not exist.
     *
     * @param key key to remove
     */
    public void deleteKey(String key) {
        getConsulKeyValueClient().deleteKey(key);
        try {
            for (String subKey : getConsulKeyValueClient().getKeys(key)) {
                getConsulKeyValueClient().deleteKey(subKey);
            }
        } catch (NotFoundException e) {
            // Nothing to do. It's no problem, if the item does not exist.
        }
    }

    public Optional<String> getValueAsString(String instanceNodeKey) {
        return getConsulKeyValueClient().getValueAsString(instanceNodeKey);
    }

    public String getValueAsString(String key, String defaultValue) {
        return getConsulKeyValueClient().getValueAsString(key).or(defaultValue);
    }

    public boolean keyExist(String key) {
        try {
            return !getConsulKeyValueClient().getKeys(key).isEmpty();
        } catch (ConsulException e) {
            return false;
        }
    }
}