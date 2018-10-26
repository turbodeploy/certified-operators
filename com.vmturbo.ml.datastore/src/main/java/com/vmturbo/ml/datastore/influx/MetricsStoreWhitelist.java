package com.vmturbo.ml.datastore.influx;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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

    /**
     * The set of commodity types to use in the commodity type whitelist.
     */
    private Set<Integer> commodityTypeWhitelist;

    /**
     * The set of metric types to use in the metric type whitelist.
     */
    private Set<MetricType> metricTypeWhtielist;

    /**
     * Create a new commodity type whitelist.
     *
     * If commodity/metric type overrides have been set, those will take precedence over the
     * defaults.
     *
     * @param defaultCommodityTypeWhitelist The default commodity types.
     * @param defaultMetricTypeWhitelist The default metric types.
     * @param keyValueStore The kvStore to use for persisting overrides and looking up any existing
     *                      overrides.
     */
    public MetricsStoreWhitelist(@Nonnull final Set<CommodityType> defaultCommodityTypeWhitelist,
                                 @Nonnull final Set<MetricType> defaultMetricTypeWhitelist,
                                 @Nonnull final KeyValueStore keyValueStore) {
        this.commodityTypeWhitelist = Objects.requireNonNull(asIntegerSet(defaultCommodityTypeWhitelist));
        this.metricTypeWhtielist = Objects.requireNonNull(defaultMetricTypeWhitelist);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);

        // If metric and/or commodity type overrides have been set, load those.
        keyValueStore.get(keyPath(COMMODITY_TYPES_KEY))
            .map(commodityTypesJson -> asIntegerSet(gson.fromJson(commodityTypesJson,
                new TypeToken<List<CommodityType>>() { }.getType())))
            .ifPresent(overrideCommodityTypes -> this.commodityTypeWhitelist = overrideCommodityTypes);
        keyValueStore.get(keyPath(METRIC_TYPES_KEY))
            .map(metricTypesJson -> gson.<Set<MetricType>>fromJson(metricTypesJson,
                    new TypeToken<Set<MetricType>>() { }.getType()))
            .ifPresent(overrideMetricTypes -> this.metricTypeWhtielist = overrideMetricTypes);
    }

    /**
     * Set the whitelist commodities. Metrics for whitelisted commodities will be written to the database.
     * Commodities in a topology received by the ML datastore in the whitelist will have their metrics
     * written to influx. Commodities NOT appearing in whitelist do NOT have their metrics written to influx.
     *
     * Setting the whitelist commodities COMPLETELY REPLACES the previously set whitelist.
     * Setting an empty whitelist will prevent any statistics at all from being written.
     *
     * This whitelist is persisted across restarts.
     *
     * @param whitelistCommodityTypes The commodities whitelist to use when writing metrics to influx.
     */
    public void setWhitelistCommodityTypes(@Nonnull final Set<CommodityType> whitelistCommodityTypes) {
        synchronized (storeLock) {
            final Set<Integer> integerCommodities = asIntegerSet(whitelistCommodityTypes);
            keyValueStore.put(keyPath(COMMODITY_TYPES_KEY), gson.toJson(whitelistCommodityTypes));
            this.commodityTypeWhitelist = integerCommodities;
        }
    }

    /**
     * Set the whitelist commodities. Metrics for whitelisted commodities will be written to the database.
     * Commodities in a topology received by the ML datastore in the whitelist will have their metrics
     * written to influx. Commodities NOT appearing in whitelist do NOT have their metrics written to influx.
     *
     * Note that {@code MetricType.CAPACITY} is only available on sold commodities and
     * not on bought commodities.
     *
     * Setting the whitelist metrics COMPLETELY REPLACES the previously set whitelist.
     * Setting an empty whitelist will prevent any statistics at all from being written.
     *
     * This whitelist is persisted across restarts.
     *
     * @param whitelistMetricTypes The metrics types whitelist to use when writing metrics to influx.
     */
    public void setWhitelistMetricTypes(@Nonnull final Set<MetricType> whitelistMetricTypes) {
        synchronized (storeLock) {
            keyValueStore.put(keyPath(METRIC_TYPES_KEY), gson.toJson(whitelistMetricTypes));
            this.metricTypeWhtielist = whitelistMetricTypes;
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
    public Set<Integer> getWhitelistCommodityTypeNumbers() {
        synchronized (storeLock) {
            return this.commodityTypeWhitelist;
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
    public Set<CommodityType> getWhitelistCommodityTypes() {
        synchronized (storeLock) {
            return commodityTypeWhitelist.stream()
                .map(CommodityType::forNumber)
                .collect(Collectors.toSet());
        }
    }

    /**
     * Get the whitelist of metric types to write to influx.
     *
     * If the whitelist has been explicitly set (via gRPC service), returns the set list.
     * Otherwise, returns the default list (set via configuration injection from clustermgr).
     *
     * @return The set of metric types whose metrics should be written to influx.
     */
    public Set<MetricType> getWhitelistMetricTypes() {
        synchronized (storeLock) {
            return metricTypeWhtielist;
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
