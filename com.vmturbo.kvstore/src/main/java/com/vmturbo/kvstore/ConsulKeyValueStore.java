package com.vmturbo.kvstore;

import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;

import com.ecwid.consul.ConsulException;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;

import org.apache.http.conn.ConnectTimeoutException;

import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.Operation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;

/**
 * An interface to interact with consul's distributed key value store. All consul operations are wrapped
 * with {@link RetriableOperation} to retry. After retry timeout, {@link KeyValueStoreOperationException}
 * will be thrown, it's caller's responsibility to catch the exception to ensure operations' consistency.
 */
public class ConsulKeyValueStore implements KeyValueStore {

    private static final String DISALLOWED_NAMESPACE_CHARACTERS = "\\/";
    private static final CharMatcher DISALLOWED_NAMESPACE_MATCHER =
            CharMatcher.anyOf(DISALLOWED_NAMESPACE_CHARACTERS);
    private static final Predicate<Exception> EXCEPTION_PREDICATE = e -> {
        if (e instanceof ConsulException) {
            Throwable cause = e.getCause();
            return cause instanceof SocketTimeoutException || cause instanceof ConnectException ||
                    cause instanceof ConnectTimeoutException || cause instanceof UnknownHostException;
        }
        return false;
    };
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
     * Time period before stop retrying.
     */
    private final long timeout;
    /**
     * The time unit to apply to the retry interval.
     */
    private final TimeUnit timeoutTimeUnit;
    @VisibleForTesting
    static final Response emptyResponse = new Response(null, 0L, true, 0L);

    /**
     * Constructor to allow consul dependency injection for tests.
     *
     * @param consul      Consul interface to use.
     * @param namespace   See: {@link #ConsulKeyValueStore(String, String, String, String, long,
     *                    TimeUnit)}.
     * @param timeout     Time period before stop retrying.
     * @param timeoutTimeUnit Time unit for timeout.
     * @throws IllegalArgumentException If the retryInterval or namespace is invalid
     */
    @VisibleForTesting
    ConsulKeyValueStore(@Nonnull final KeyValueClient consul, @Nonnull final String namespace,
            final long timeout, final TimeUnit timeoutTimeUnit) {
        validateNamespace(namespace);
        if (timeout <= 0) {
            throw new IllegalArgumentException("Illegal timeout: " + timeout);
        }

        this.namespace = namespace;
        this.consul = consul;
        this.timeout = timeout;
        this.timeoutTimeUnit = timeoutTimeUnit;
    }

    /**
     * Create a new ConsulKeyValueStore. It has to be initialized before usage.
     *
     * @param namespacePrefix Namespace prefix to be prepended to namespace of all keys.
     * @param namespace       The namespace to use for all keys stored with this key-value-store
     *                        instance. It has to be non-null and non-empty, so as to not pollute the
     *                        global namespace. Cannot contain "/" or "\".
     * @param consulHost      Hostname of the consul service.
     * @param consulPort      Port to use to connect to consul.
     * @param timeout         Time period before stop retrying.
     * @param timeoutTimeUnit Time unit for timeout.
     * @throws IllegalArgumentException If the timeout or namespace is invalid.
     */
    public ConsulKeyValueStore(@Nonnull final String namespacePrefix,
                               @Nonnull final String namespace,
                               @Nonnull final String consulHost,
                               @Nonnull final String consulPort,
                               final long timeout,
                               final TimeUnit timeoutTimeUnit) {
        validateNamespace(namespace);
        if (timeout <= 0) {
            throw new IllegalArgumentException("Illegal timeout: " + timeout);
        }

        this.namespace = namespacePrefix + namespace;
        this.consul = new ConsulClient(consulHost, Integer.valueOf(consulPort));
        this.timeout = timeout;
        this.timeoutTimeUnit = timeoutTimeUnit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(@Nonnull final String key, @Nonnull final String value) {
        execute(() -> consul.setKVValue(fullKey(key), value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeKeysWithPrefix(@Nonnull final String prefix) {
        execute(() -> consul.deleteKVValues(fullKey(prefix)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeKey(@Nonnull final String key) {
        execute(() -> consul.deleteKVValue(fullKey(key)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<String> get(@Nonnull final String key) {
      return  Optional.ofNullable(execute(() -> consul.getKVValue(fullKey(key))).getValue())
                .map(GetValue::getValue)
                .map(ConsulKeyValueStore::decodeBase64);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String get(@Nonnull final String key, @Nonnull final String defaultValue) {
        final GetValue value = execute(() -> consul.getKVValue(fullKey(key))).getValue();
        return value == null ? defaultValue : decodeBase64(value.getValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<String, String> getByPrefix(@Nonnull final String keyPrefix) {
        final Response<List<GetValue>> values =
                execute(() -> consul.getKVValues(fullKey(keyPrefix)));
        if (values.getValue() != null) {
            ImmutableMap.Builder<String, String> retBuilder = new ImmutableMap.Builder<>();
            values.getValue()
                    .stream()
                    .filter(retValue -> retValue.getValue() != null)
                    .forEach(retValue -> {
                        String key = retValue.getKey().replaceFirst(namespace + "/", "");
                        String decodedVal = decodeBase64(retValue.getValue());
                        retBuilder.put(key, decodedVal);
                    });
            return retBuilder.build();
        }
        return Collections.emptyMap();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public boolean containsKey(@Nonnull final String key) {
        List<String> keys = execute(() -> consul.getKVKeysOnly(fullKey(key))).getValue();
        return keys != null && keys.size() > 0;
    }

    /**
     * Construct consul namespace prefix to be prepended to consul key of each k/v store and service
     * name.
     *
     * @param consulNamespace       Given consulNamespace.
     * @param enableConsulNamespace True if consulNamespace is enabled.
     * @return Constructed consul namespace prefix. If enableConsulNamespace is true and consulNamespace
     * is not empty, return the constructed constructNamespacePrefix, otherwise return empty string.
     */
    @Nonnull
    public static String constructNamespacePrefix(String consulNamespace, boolean enableConsulNamespace) {
        if (!enableConsulNamespace) {
            return "";
        } else if (StringUtils.isEmpty(consulNamespace)) {
            return "";
        } else {
            return consulNamespace + "/";
        }
    }

    @Nonnull
    private String fullKey(@Nonnull final String key) {
        final String fullKey =
                new StringBuilder().append(namespace).append("/").append(key).toString();
        return UriUtils.encodeFragment(fullKey, "UTF-8");
    }

    private void validateNamespace(@Nonnull final String namespace) {
        Objects.requireNonNull(namespace);
        final String trimmedNamespace = namespace.trim();
        if (trimmedNamespace.length() == 0) {
            throw new IllegalArgumentException("Namespace must be a non-empty string!");
        }
        if (DISALLOWED_NAMESPACE_MATCHER.matchesAnyOf(namespace)) {
            throw new IllegalArgumentException(
                    "Namespace " + namespace + " contains one of these illegal characters: " +
                            DISALLOWED_NAMESPACE_CHARACTERS);
        }
    }

    /**
     * Decode a base64-encoded string to the default encoding. Consul values are base64 encoded, and
     * the ecwid library doesn't decode them on our behalf so we have to do it manually.
     *
     * @param base64EncodedString A base-64 encoded string.
     * @return The decoded string.
     */
    @Nonnull
    @VisibleForTesting
    static String decodeBase64(@Nonnull final String base64EncodedString) {
        return new String(Base64.getDecoder().decode(base64EncodedString.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    /**
     * Execute Consul K/V operation using {@link RetriableOperation}.
     *
     * @param operation Consul operation to be execute.
     * @param <T>       response type.
     * @return Consul response.
     */
    private <T> Response<T> execute(@Nonnull final Operation<Response<T>> operation) {
        try {
            return RetriableOperation.newOperation(operation)
                    .retryOnException(EXCEPTION_PREDICATE)
                    .run(timeout, timeoutTimeUnit);
        } catch (RetriableOperationFailedException | TimeoutException e) {
            // rethrow
            throw new KeyValueStoreOperationException("Failed to execute KeyValue store operation.", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return emptyResponse;
    }
}
