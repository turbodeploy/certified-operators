package com.vmturbo.ml.datastore.influx;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist.ActionType;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Whitelist of commodities and metric types whose metrics should be stored.
 *
 * Persistent storage for whitelist settings are accomplished through a key-value store (consul).
 *
 * The whitelist is thread-safe.
 */
@ThreadSafe
public class MetricsStoreWhitelist {

    private static final String WHITELISTS_PREFIX = "whitelists/";
    public static final String COMMODITY_TYPES_KEY = "commodity_types";
    public static final String METRIC_TYPES_KEY = "metric_types";
    public static final String ACTION_TYPES_KEY = "action_types";
    public static final String ACTION_STATE_KEY = "action_states";
    public static final String CLUSTER_SUPPORT_KEY = "cluster_support";

    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;

    /**
     * Lock for write operations on the persistent storage.
     */
    private final Object storeLock = new Object();

    /**
     * Used to serialize the whitelist data to JSON strings.
     */
    private final Gson gson = new Gson();

    public static class WhitelistType<T extends Enum<T>> {
        private final BiFunction<String, Gson, Set<T>> fromJsonConverter;

        private final String keyName;

        public WhitelistType(@Nonnull final BiFunction<String, Gson, Set<T>> fromJsonConverter,
                             @Nonnull final String keyName) {
            this.fromJsonConverter = Objects.requireNonNull(fromJsonConverter);
            this.keyName = Objects.requireNonNull(keyName);
        }

        public Set<T> convert(@Nonnull final String whitelistJsonString,
                              @Nonnull final Gson gson) {
            return fromJsonConverter.apply(whitelistJsonString, gson);
        }

        public String getKeyName() {
            return keyName;
        }
    }


    public static final WhitelistType<CommodityType> COMMODITY_TYPE
            = new WhitelistType<>(
                (json, gson) -> gson.<Set<CommodityType>>fromJson(json,
                    new TypeToken<Set<CommodityType>>() { }.getType()), COMMODITY_TYPES_KEY);

    public static final WhitelistType<MetricType> METRIC_TYPE
            = new WhitelistType<>(
                (json, gson) -> gson.<Set<MetricType>>fromJson(json,
                    new TypeToken<Set<MetricType>>() { }.getType()), METRIC_TYPES_KEY);

    public static final WhitelistType<ActionType> ACTION_TYPE
            = new WhitelistType<>(
                (json, gson) -> gson.<Set<ActionType>>fromJson(json,
                    new TypeToken<Set<ActionType>>() { }.getType()), ACTION_TYPES_KEY);

    public static final WhitelistType<ActionState> ACTION_STATE
            = new WhitelistType<>(
                (json, gson) -> gson.<Set<ActionState>>fromJson(json,
                    new TypeToken<Set<ActionState>>() { }.getType()), ACTION_STATE_KEY);

    private static final Set<WhitelistType<?>> WHITELIST_TYPES
            = ImmutableSet.of(COMMODITY_TYPE, METRIC_TYPE, ACTION_TYPE, ACTION_STATE);

    private Map<WhitelistType<?>, Set<? extends Enum<?>>> whiteListHolder = new HashMap<>();

    /**
     * Whether we write cluster membership information.
     */
    private boolean clustersSupported;

    /**
     * Create a new commodity type whitelist.
     *
     * If commodity/metric type overrides have been set, those will take precedence over the
     * defaults.
     *
     * @param defaultWhitelists mapping of WhiteListType to corresponding whitelist.
     * @param clustersSupported Whether we should write cluster membership information.
     * @param keyValueStore The kvStore to use for persisting overrides and looking up any existing
     *                      overrides.
     */
    public MetricsStoreWhitelist(Map<WhitelistType<?>, Set<? extends Enum<?>>> defaultWhitelists,
                                        boolean clustersSupported,
                                        @Nonnull final KeyValueStore keyValueStore) {
        this.clustersSupported = clustersSupported;
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        defaultWhitelists.forEach(whiteListHolder::put);

        // If metric and/or commodity type overrides have been set, load those.
        WHITELIST_TYPES.forEach(type -> {
            keyValueStore.get(keyPath(type.getKeyName()))
                    .map(whitelistTypesJson -> type.convert(whitelistTypesJson, gson))
                    .ifPresent(overrideWhitelistTypes -> whiteListHolder.put(type, overrideWhitelistTypes));
        });

        keyValueStore.get(keyPath(CLUSTER_SUPPORT_KEY))
                .map(clusterSupportJson -> gson.<Boolean>fromJson(clusterSupportJson,
                        new TypeToken<Boolean>() { }.getType()))
                .ifPresent(overrideClusterSupport -> this.clustersSupported = overrideClusterSupport);
    }

    /**
     * Set the whitelist of a commodities/actions. Metrics for whitelisted commodities/action will be written to the database.
     * Commodities in a topology/actions received by the ML datastore in the whitelist will have their metrics
     * written to influx. Commodities/actions NOT appearing in whitelist do NOT have their metrics written to influx.
     *
     * Note that {@code MetricType.CAPACITY} is only available on sold commodities and
     * not on bought commodities.
     *
     * Setting the whitelist metrics COMPLETELY REPLACES the previously set whitelist.
     * Setting an empty whitelist will prevent any statistics at all from being written.
     *
     * This whitelist is persisted across restarts.
     *
     * @param whitelist The attribute of a particular type to allow when writing metrics to influx.
     * @param type type of attribute being saved
     */
    public <T extends Enum<T>> void storeWhitelist(Set<T> whitelist, WhitelistType<T> type) {
        synchronized (storeLock) {
            keyValueStore.put(keyPath(type.getKeyName()), gson.toJson(whitelist));
            whiteListHolder.put(type, whitelist);
        }
    }

    /**
     * Get the whitelist of commodity/actions to write to influx.
     *
     * If the whitelist has been explicitly set (via gRPC service), returns the set list.
     * Otherwise, returns the default list (set via configuration injection from clustermgr).
     *
     * @return The set of commodity types whose metrics should be written to influx.
     */
    @SuppressWarnings("unchecked")
    public <T extends Enum<T>> Set<T> getWhitelist(WhitelistType<T> type) {
        synchronized (storeLock) {
            return (Set<T>)this.whiteListHolder.get(type);
        }
    }

    /**
     * Get the whitelist of commodity types to write to influx.
     *
     * If the whitelist has been explicitly set (via gRPC service), returns the set list.
     * Otherwise, returns the default list (set via configuration injection from clustermgr).
     *
     * @return The set of commodity types whose metrics should be written to influx.
     */
    @Nullable
    public Set<Integer> getWhitelistAsInteger(@Nonnull final WhitelistType<?> type) {
        synchronized (storeLock) {
            if (type == COMMODITY_TYPE) {
                final Set<? extends Enum<?>> set = whiteListHolder.get(type);
                return set == null ? null : asIntegerSet(set.stream().map(CommodityType.class::cast).collect(Collectors.toSet()));
            } else {
                // logger.error("cannot retrieve whitelist as integers for anything that is not of CommodityType");
                return null;
            }
        }
    }

    /**
     * Set whether we should write cluster membership information to influx.
     *
     * @param clustersSupported whether we should write cluster membership information to influx.
     */
    public void setClusterSupport(final boolean clustersSupported) {
        synchronized (storeLock) {
            keyValueStore.put(keyPath(CLUSTER_SUPPORT_KEY), gson.toJson(clustersSupported));
            this.clustersSupported = clustersSupported;
        }
    }

    /**
     * Get whether we should write cluster membership information to influx.
     *
     * @return whether we should write cluster membership information to influx.
     */
    public boolean getClusterSupport() {
        synchronized (storeLock) {
            return clustersSupported;
        }
    }

    /**
     * Convert a collection of {@link CommodityType}s to a set of their number integers.
     *
     * @param commodityTypes The commodity types to convert.
     * @return The set of integers corresponding to the commodity types.
     */
    private Set<Integer> asIntegerSet(@Nonnull final Collection<CommodityType> commodityTypes) {
        return commodityTypes.stream()
                .map(CommodityType::getNumber)
                .collect(Collectors.toSet());
    }

    /**
     * Get the key path for a particular key-value store key where a whitelist configuration
     * is stored.
     *
     * @param keyName the name of the key for a particular whitelist.
     * @return The key path for a key-value store key where a whitelist configuration is stored.
     */
    static String keyPath(@Nonnull final String keyName) {
        return WHITELISTS_PREFIX + keyName;
    }
}
