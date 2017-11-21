package com.vmturbo.group.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Enumeration for all the settings, available in Group component.
 */
public enum SettingPolicySetting {
    Move("move"),
    Suspend("suspend"),
    Provision("provision"),
    Resize("resize"),
    CpuUtilization("cpuUtilization"),
    MemoryUtilization("memoryUtilization"),
    IoThroughput("ioThroughput"),
    NetThroughput("netThroughput"),
    SwappingUtilization("swappingUtilization"),
    ReadyQueueUtilization("readyQueueUtilization"),
    StorageAmmountUtilization("storageAmountUtilization"),
    IopsUtilization("iopsUtilization"),
    LatencyUtilization("latencyUtilization"),
    CpuOverprovisionedPercentage("cpuOverprovisionedPercentage"),
    MemoryOverprovisionedPercentage("memoryOverprovisionedPercentage");

    /**
     * Setting name to setting enumeration value map for fast access.
     */
    private static final Map<String, SettingPolicySetting> SETTING_MAP;

    private final String settingName;

    static {
        final SettingPolicySetting[] settings = SettingPolicySetting.values();
        final Map<String, SettingPolicySetting> result = new HashMap<>(settings.length);
        for (SettingPolicySetting setting : settings) {
            result.put(setting.getSettingName(), setting);
        }
        SETTING_MAP = Collections.unmodifiableMap(result);
    }

    SettingPolicySetting(@Nonnull String settingName) {
        this.settingName = Objects.requireNonNull(settingName);
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getSettingName() {
        return settingName;
    }

    /**
     * Finds a setting (enumeration value) by setting name.
     *
     * @param settingName setting name
     * @return setting enumeration value or empty optional, if not setting found by the name
     * @throws NullPointerException if {@code settingName} is null
     */
    @Nonnull
    public static Optional<SettingPolicySetting> getSettingByName(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Optional.ofNullable(SETTING_MAP.get(settingName));
    }
}
