package com.vmturbo.kvstore;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.util.UriUtils;

import com.ecwid.consul.ConsulException;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;

/**
 * An interface to interact with consul's distributed key value store.
 */
public class ConsulKeyValueStore implements KeyValueStore {

    private static final String DISALLOWED_NAMESPACE_CHARACTERS = "\\/";
    private static final CharMatcher DISALLOWED_NAMESPACE_MATCHER =
            CharMatcher.anyOf(DISALLOWED_NAMESPACE_CHARACTERS);

    private final Logger logger = LogManager.getLogger();

    /**
     * The prefix to use for all keys stored with this key-value-store.
     */
    private final String namespace;

    /**
     * Interface to consul.
     */
    private final KeyValueClient consul;

    /**
     * The length of time to wait between attempts to perform key-value
     * store operations when a previous time fails.
     */
    private final long retryInterval;

    /**
     * The time unit to apply to the retry interval.
     */
    private final TimeUnit retryTimeUnit;

    /**
     * A consul operation that can be performed.
     */
    @FunctionalInterface
    private interface ReturningConsulOperation<T> {
        T perform() throws ConsulException;
    }

    /**
     * A consul operation that can be performed.
     */
    @FunctionalInterface
    private interface VoidConsulOperation {
        void perform() throws ConsulException;
    }

    /**
     * Constructor to allow consul dependency injection for tests.
     *
     * @param consul Consul interface to use.
     * @param namespace See: {@link #ConsulKeyValueStore(String, String, String, long, TimeUnit)}.
     * @param retryInterval The length of time to wait between attempts to perform key-value
     *                      store operations when a previous time fails.
     * @param retryTimeUnit The time unit to apply to the retry interval.
     * @throws IllegalArgumentException If the retryInterval or namespace is invalid
     */
    ConsulKeyValueStore(@Nonnull final KeyValueClient consul, @Nonnull final String namespace,
                        final long retryInterval, final TimeUnit retryTimeUnit) {
        validateNamespace(namespace);
        if (retryInterval <= 0) {
            throw new IllegalArgumentException("Illegal retry interval: " + retryInterval);
        }

        this.namespace = namespace;
        this.consul = consul;
        this.retryInterval = retryInterval;
        this.retryTimeUnit = retryTimeUnit;
    }

    /**
     * Create a new ConsulKeyValueStore. It has to be initialized before usage.
     *
     * @param namespace The namespace to use for all keys stored with this
     *                  key-value-store instance. It has to be non-null
     *                  and non-empty, so as to not pollute the global namespace.
     *                  Cannot contain "/" or "\".
     * @param consulHost Hostname of the consul service.
     * @param consulPort Port to use to connect to consul.
     * @param retryInterval The length of time to wait between attempts to perform key-value
     *                      store operations when a previous time fails.
     * @param retryTimeUnit The time unit to apply to the retry interval.
     * @throws IllegalArgumentException If the retryInterval or namespace is invalid
     */
    public ConsulKeyValueStore(@Nonnull final String namespace,
                               @Nonnull final String consulHost,
                               @Nonnull final String consulPort,
                               final long retryInterval,
                               final TimeUnit retryTimeUnit) {
        validateNamespace(namespace);
        if (retryInterval <= 0) {
            throw new IllegalArgumentException("Illegal retry interval: " + retryInterval);
        }

        this.namespace = namespace;
        this.consul = new ConsulClient(consulHost, Integer.valueOf(consulPort));
        this.retryInterval = retryInterval;
        this.retryTimeUnit = retryTimeUnit;
    }

    /** {@inheritDoc}
     */
    @Override
    public void put(@Nonnull final String key, @Nonnull final String value) {
        performKeyValueOperation(() -> consul.setKVValue(fullKey(key), value));
    }

    /** {@inheritDoc}
     */
    @Override
    public void remove(@Nonnull final String key) {
        // KeyValueClient::deleteKVValues allows deleting values or directories
        performKeyValueOperation(() -> consul.deleteKVValues(fullKey(key)));
    }

    /** {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<String> get(@Nonnull final String key) {
        return performKeyValueOperation(
            () -> Optional.ofNullable(consul.getKVValue(fullKey(key)).getValue())
                .map(GetValue::getValue)
                .map(ConsulKeyValueStore::decodeBase64),
            Optional.empty()
        );
    }

    /** {@inheritDoc}
     */
    @Override
    @Nonnull
    public String get(@Nonnull final String key, @Nonnull final String defaultValue) {
        return performKeyValueOperation(
            () -> {
                final GetValue value = consul.getKVValue(fullKey(key)).getValue();
                return (value == null) ? defaultValue : decodeBase64(value.getValue());
            }, ""
        );
    }

    /** {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<String, String> getByPrefix(@Nonnull final String keyPrefix) {
        return performKeyValueOperation(() -> {
            ImmutableMap.Builder<String, String> retBuilder = new ImmutableMap.Builder<>();
            final Response<List<GetValue>> values = consul.getKVValues(fullKey(keyPrefix));
            if (values.getValue() != null) {
                values.getValue().stream()
                        .filter(retValue -> retValue.getValue() != null)
                        .forEach(retValue -> {
                            String key = retValue.getKey().replaceFirst(namespace + "/", "");
                            String decodedVal = decodeBase64(retValue.getValue());
                            retBuilder.put(key, decodedVal);
                        });
            }
            return retBuilder.build();
        }, Collections.emptyMap());
    }

    /** {@inheritDoc}
     */
    @Override
    @Nonnull
    public boolean containsKey(@Nonnull final String key) {
        return performKeyValueOperation(() -> {
            List<String> keys = consul.getKVKeysOnly(fullKey(key)).getValue();
            return keys != null && keys.size() > 0;
        }, false);
    }

    /**
     * Decode a base64-encoded string to the default encoding.
     * Consul values are base64 encoded, and the ecwid library doesn't decode them on our behalf
     * so we have to do it manually.
     *
     * @param base64EncodedString A base-64 encoded string.
     * @return The decoded string.
     */
    @Nonnull
    static String decodeBase64(@Nonnull final String base64EncodedString) {
        return new String(Base64.getDecoder().decode(base64EncodedString));
    }

    @Nonnull
    private String fullKey(@Nonnull final String key) {
        final String fullKey =  new StringBuilder()
                .append(namespace)
                .append("/")
                .append(key)
                .toString();
        try {
            return UriUtils.encodeFragment(fullKey, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e); // Should never happen unless UTF-8 encoding support is somehow dropped
            return "";
        }
    }

    private <T> T performKeyValueOperation(@Nonnull ReturningConsulOperation<T> operation,
                                           @Nonnull T defaultOnInterrupt) {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(defaultOnInterrupt);

        while (true) {
            try {
                return operation.perform();
            } catch (ConsulException e) {
                logger.error("Error while attempting key value operation: ", e);

                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.convert(retryInterval, retryTimeUnit));
                } catch (InterruptedException ie) {
                    logger.info("Key value operation interrupted: ", ie);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return defaultOnInterrupt;
    }

    private void performKeyValueOperation(@Nonnull VoidConsulOperation operation) {
        Objects.requireNonNull(operation);

        while (true) {
            try {
                operation.perform();
                return;
            } catch (ConsulException e) {
                logger.error("Error while attempting key value operation: ", e);

                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.convert(retryInterval, retryTimeUnit));
                } catch (InterruptedException ie) {
                    logger.info("Key value operation interrupted: ", ie);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void validateNamespace(@Nonnull final String namespace) {
        Objects.requireNonNull(namespace);
        final String trimmedNamespace = namespace.trim();
        if (trimmedNamespace.length() == 0) {
            throw new IllegalArgumentException("Namespace must be a non-empty string!");
        }
        if (DISALLOWED_NAMESPACE_MATCHER.matchesAnyOf(namespace)) {
            throw new IllegalArgumentException("Namespace " +
                    namespace +
                    " contains one of these illegal characters: " +
                    DISALLOWED_NAMESPACE_CHARACTERS);
        }
    }
}
