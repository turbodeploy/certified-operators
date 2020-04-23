package com.vmturbo.securekvstore;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.vault.VaultException;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponseSupport;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.util.UriUtils;

import com.vmturbo.kvstore.KeyValueStore;

/**
 * A key value store to interact with HashiCorp vault using Spring-Vault library.
 * The path is: namespace/componentType/key, e.g: kv/topology-processor/user.
 * Note:
 * <p><ul>
 * <li> The vault must be unsealed.
 * <li> The namespace must be configured with secrets engine version 1.
 * E.g: "vault secrets enable -version=1 kv"
 * </ul>
 */
public class VaultKeyValueStore implements KeyValueStore {

    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String GET = "GET";
    private static final String GET_PREFIX = "GET PREFIX";
    private static final String SLASH = "/";
    private static final String DISALLOWED_PATH_CHARACTERS = "\\/";
    private static final CharMatcher DISALLOWED_PATH_MATCHER =
            CharMatcher.anyOf(DISALLOWED_PATH_CHARACTERS);
    private final Logger logger = LogManager.getLogger();

    /**
     * The prefix to use for all keys stored with vault.
     */
    private final String namespace;

    /**
     * Interface to Vault.
     */
    private final VaultTemplate vaultTemplate;

    /**
     * The component type, e.g. api. It will be part of the path.
     */
    private final String componentType;

    /**
     * The length of time to wait between attempts to perform key-value
     * store operations when a previous attempt fails.
     */
    private final long retryInterval;

    /**
     * The time unit to apply to the retry interval.
     */
    private final TimeUnit retryTimeUnit;

    /**
     * Constructor for the vault backed key value store.
     *
     * @param vaultTemplate The interface to Vault.
     * @param namespace The path configured with secrets engine version.
     *         It has to be non-null and non-empty, so as to not pollute the global namespace.
     *         Cannot contain "/" or "\".
     * @param componentType The component type, e.g.: api.
     * @param retryInterval The length of time to wait between attempts to perform
     *         operations when a previous attempt fails.
     * @param retryTimeUnit The time unit to apply to the retry interval.
     */
    public VaultKeyValueStore(@Nonnull VaultTemplate vaultTemplate, @Nonnull String namespace,
            @Nonnull String componentType, long retryInterval, @Nonnull TimeUnit retryTimeUnit) {
        validatePath(namespace);
        validatePath(componentType);
        Preconditions.checkArgument(
                TimeUnit.MILLISECONDS.convert(retryInterval, retryTimeUnit) > 500,
                "Retry interval cannot be less than 500 milliseconds");

        this.vaultTemplate = Objects.requireNonNull(vaultTemplate);
        this.namespace = Objects.requireNonNull(namespace);
        this.componentType = Objects.requireNonNull(componentType);
        this.retryInterval = retryInterval;
        this.retryTimeUnit = retryTimeUnit;
    }

    /**
     * Helper to validate characters in path.
     */
    private static void validatePath(@Nonnull final String path) {
        Objects.requireNonNull(path);
        final String trimmedNamespace = path.trim();
        if (trimmedNamespace.isEmpty()) {
            throw new IllegalArgumentException("Path must be a non-empty string.");
        }
        if (DISALLOWED_PATH_MATCHER.matchesAnyOf(path)) {
            throw new IllegalArgumentException(
                    "Path " + path + " contains one of these illegal characters " +
                            DISALLOWED_PATH_CHARACTERS);
        }
    }

    /**
     * A helper to build sub key path.
     */
    @Nonnull
    private static String buildKeyPath(@Nonnull String prefix, @Nonnull String subkey) {
        return prefix + SLASH + subkey;
    }

    @Override
    public void put(@Nonnull final String key, @Nonnull final String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        performKeyValueOperation(() -> Optional.ofNullable(
                vaultTemplate.write(fullComponentKey(key), new ObjectWrapper(key, value))), PUT,
                key);
    }

    @Override
    public void removeKeysWithPrefix(@Nonnull final String prefix) {
        Objects.requireNonNull(prefix);
        this.getByPrefix(prefix).keySet().forEach(subkey -> performKeyValueOperation(() -> {
            vaultTemplate.delete(fullComponentKey(buildKeyPath(prefix, subkey)));
            return Optional.empty();
        }, DELETE, prefix));
        this.removeKey(prefix);
    }

    @Override
    public void removeKey(@Nonnull final String key) {
        Objects.requireNonNull(key);
        performKeyValueOperation(() -> {
            vaultTemplate.delete(fullComponentKey(key));
            return Optional.empty();
        }, DELETE, key);
    }

    @Override
    @Nonnull
    public Optional<String> get(@Nonnull final String key) {
        Objects.requireNonNull(key);
        return performKeyValueOperation(() -> {
            final VaultResponseSupport<ObjectWrapper> responseSupport =
                    vaultTemplate.read(fullComponentKey(key), ObjectWrapper.class);
            return Optional.ofNullable(responseSupport)
                    .map(VaultResponseSupport::getData)
                    .map(ObjectWrapper::getValue);
        }, GET, key);
    }

    @Override
    @Nonnull
    public String get(@Nonnull final String key, @Nonnull final String defaultValue) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(defaultValue);
        return performKeyValueOperation(() -> {
            final VaultResponseSupport<ObjectWrapper> responseSupport =
                    vaultTemplate.read(fullComponentKey(key), ObjectWrapper.class);
            return Optional.ofNullable(responseSupport)
                    .map(VaultResponseSupport::getData)
                    .map(ObjectWrapper::getValue);
        }, GET, key).orElse(defaultValue);
    }

    @Override
    @Nonnull
    public Map<String, String> getByPrefix(@Nonnull final String keyPrefix) {
        Objects.requireNonNull(keyPrefix);
        return performKeyValueOperation(() -> {
            final ImmutableMap.Builder<String, String> mapBuilder = new ImmutableMap.Builder<>();
            final Collection<String> values = vaultTemplate.list(fullComponentKey(keyPrefix));
            values.forEach(string -> {
                final String path = fullComponentKey(keyPrefix) + SLASH + string;
                final VaultResponseSupport<ObjectWrapper> support =
                        vaultTemplate.read(path, ObjectWrapper.class);
                final String value = support.getData().getValue();
                mapBuilder.put(string, value);
            });
            return Optional.of(mapBuilder.build());
        }, GET_PREFIX, keyPrefix).orElse(ImmutableMap.of());
    }

    @Override
    public boolean containsKey(@Nonnull final String key) {
        return this.get(key).isPresent();
    }

    @VisibleForTesting
    @Nonnull
    String fullComponentKey(@Nonnull final String key) {
        final String fullKey = new StringBuilder().append(namespace)
                .append("/")
                .append(componentType)
                .append("/")
                .append(key)
                .toString();

        return UriUtils.encodeFragment(fullKey, "UTF-8");
    }

    private <T> Optional<T> performKeyValueOperation(@Nonnull VaultOperation<T> operation,
            @Nonnull String operationName, @Nonnull String key) {
        Objects.requireNonNull(operation);

        while (true) {
            try {
                return operation.perform();
            } catch (VaultException | ResourceAccessException e) {
                final String message = String.format(
                        "Error while attempting key value operation " + "\"%s\" and key \"%s\".",
                        operationName, key);
                logger.error(message, e);

                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.convert(retryInterval, retryTimeUnit));
                } catch (InterruptedException ie) {
                    logger.info("Key value operation interrupted ", ie);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return Optional.empty();
    }

    /**
     * A vault operation.
     */
    @FunctionalInterface
    private interface VaultOperation<T> {
        Optional<T> perform() throws VaultException, ResourceAccessException;
    }
}