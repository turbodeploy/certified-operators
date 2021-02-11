package com.vmturbo.topology.processor.probeproperties;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.group.api.SettingMessages.SettingNotification;
import com.vmturbo.mediation.common.PropertyKeyName;
import com.vmturbo.mediation.common.PropertyKeyName.PropertyKeyType;

/**
 * Global settings that have to be passed to the probes as properties.
 * Contains most recent stringized values.
 * - to avoid startup-time TP->Group dependency, values are retrieved from the group
 *   client in background, until success (group is available),
 *   settings defaults may be used for few starting discoveries.
 * - values are updated through SettingsListener when they are changed using group API
 */
public class GlobalProbePropertiesSettingsLoader {
    private static final Logger logger = LogManager.getLogger();
    /**
     * List of global policies that have to be delivered to the probes as probe properties.
     */
    private static final Set<GlobalSettingSpecs> GLOBAL_PROBE_RELATED_SETTINGS =
                    ImmutableSet.of(GlobalSettingSpecs.OnPremVolumeAnalysis);

    private final SettingServiceBlockingStub settingsServiceClient;
    private final ScheduledExecutorService settingsLoadingThreadpool;
    private final long retryIntervalSec;
    private final long timeoutSec;
    private volatile boolean loaded;

    /**
     * Relevant to probes setting spec -> value (mediation property value can be only string).
     */
    private final Map<GlobalSettingSpecs, String> setting2value =
                    new ConcurrentHashMap<GlobalSettingSpecs, String>();

    /**
     * Construct an instance.
     *
     * @param settingsServiceClient provides sync remote access to settings
     * @param settingsLoadingThreadpool thread pool for loading settings
     * @param retryIntervalSec interval between failed settings load retries
     * @param timeoutSec timeout for a single try
     */
    public GlobalProbePropertiesSettingsLoader(SettingServiceBlockingStub settingsServiceClient,
                    ScheduledExecutorService settingsLoadingThreadpool, long retryIntervalSec,
                    long timeoutSec) {
        this.settingsServiceClient = settingsServiceClient;
        this.retryIntervalSec = retryIntervalSec;
        this.timeoutSec = timeoutSec;
        this.settingsLoadingThreadpool = settingsLoadingThreadpool;
        submit(0L);
    }

    /**
     * Return mediation representation of probe property name to value.
     *
     * @return e.g. ["global..onPremVolumeAnalysis" -> "true"]
     */
    public Map<String, String> getProbeProperties() {
        // only expose to probes already loaded values
        // for the rest they will assume default
        return setting2value.entrySet().stream()
                        .collect(Collectors.toMap(
                                        spec2value -> (new PropertyKeyName(PropertyKeyType.GLOBAL,
                                                        null, spec2value.getKey().getSettingName()))
                                                                        .toString(),
                                        Map.Entry::getValue));
    }

    /**
     * Process the change of global policies.
     *
     * @param settingChange change setting message
     * @return true if any of relevant probe-specific policies have changed value
     */
    public boolean updateProbeProperties(SettingNotification settingChange) {
        if (settingChange.hasGlobal() && settingChange.getGlobal().hasSetting()) {
            return storeRelevantSettingValue(settingChange.getGlobal().getSetting(), true);
        }
        return false;
    }

    public boolean isLoaded() {
        return loaded;
    }

    private void load() {
        // load unset yet values
        Set<String> specNamesToLoad = Sets.difference(GLOBAL_PROBE_RELATED_SETTINGS, setting2value.keySet())
                        .stream().map(GlobalSettingSpecs::getSettingName)
                        .collect(Collectors.toSet());
        if (!specNamesToLoad.isEmpty()) {
            try {
                settingsServiceClient.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                                .getMultipleGlobalSettings(GetMultipleGlobalSettingsRequest
                                                .newBuilder().addAllSettingSpecName(specNamesToLoad)
                                                .build())
                                .forEachRemaining(setting -> storeRelevantSettingValue(setting, false));
                logger.info("Loaded probe property-specific settings " + specNamesToLoad);
                loaded = true;
            } catch (StatusRuntimeException e) {
                logger.warn("Failed to load probe property-specific global settings "
                                + specNamesToLoad + ", will retry, error: " + e.getMessage());
                // repeatedly reload until success
                submit(retryIntervalSec);
            }
        } else {
            loaded = true;
        }
    }

    private void submit(long afterTimeoutSec) {
        // not considering the future, mr eastwood?
        settingsLoadingThreadpool.schedule(new Runnable() {
            @Override
            public void run() {
                load();
            }
        }, afterTimeoutSec, TimeUnit.SECONDS);
    }

    /**
     * Store the setting value received from either direct request or subscription.
     *
     * @param setting setting containing value
     * @param updated true if value has come from subscription
     * @return true if setting is relevant and value has changed
     */
    private boolean storeRelevantSettingValue(Setting setting, boolean updated) {
        String settingName = setting.getSettingSpecName();
        GlobalSettingSpecs spec = GLOBAL_PROBE_RELATED_SETTINGS.stream()
                        .filter(settingSpec -> settingName.equals(settingSpec.getSettingName()))
                        .findAny().orElse(null);
        if (spec == null) {
            return false;
        }
        Object newValueObj = spec.getValue(setting, Object.class);
        if (newValueObj == null) {
            return false;
        }
        String oldValue = setting2value.get(spec);
        String newValue = newValueObj.toString();
        boolean changed = !Objects.equals(oldValue, newValue);
        if (updated && changed) {
            logger.info("Global probe-related setting " + settingName + " changed value to "
                            + newValue);
        }
        // for explicit requests - do not store if we already have
        // a possibly more recent value from subscription
        // for subscription updates - always override
        if (updated) {
            setting2value.put(spec, newValue);
        } else {
            setting2value.putIfAbsent(spec, newValue);
        }
        return changed;
    }
}
