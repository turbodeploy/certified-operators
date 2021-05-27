package com.vmturbo.cost.component.savings;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Manages action retention configuration values.
 */
public class EntitySavingsRetentionConfig {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    private final SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient;
    private Long actionRetentionMs;
    private Long volumeDeleteRetentionMs;
    private Long previousActionRetentionMs;
    private Long previousVolumeDeleteRetentionMs;

    /**
     * Constructor.
     *
     * @param settingServiceClient settings service client.
     */
    public EntitySavingsRetentionConfig(SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient) {
        this.settingServiceClient = settingServiceClient;
        this.actionRetentionMs = getDefaultSetting(GlobalSettingSpecs.CloudSavingsActionRetention);
        this.volumeDeleteRetentionMs =
                getDefaultSetting(GlobalSettingSpecs.CloudSavingsDeleteVolumeRetention);
        previousActionRetentionMs = null;
        previousVolumeDeleteRetentionMs = null;
    }

    /**
     * Query the settings manager for a global setting.  The global setting in the UI is expressed
     * in months, but internally, milliseconds are used, so this returns the configured value in
     * milliseconds.  A month is assumed to have a duration of 730 hours.
     *
     * @param spec the {@link GlobalSettingSpecs} to query.
     * @return the global setting value expressed in milliseconds.  If the configuration value
     * isn't explicitly set, the previously configured value will be returned instead.
     */
    private long getSetting(GlobalSettingSpecs spec) {
        String settingName = spec.getSettingName();

        final SettingProto.GetSingleGlobalSettingRequest request =
                SettingProto.GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(settingName)
                        .build();
        SettingProto.Setting setting;
        try {
            setting = settingServiceClient.getGlobalSetting(request).getSetting();
        } catch (io.grpc.StatusRuntimeException e) {
            // The service isn't available, so use the default value below
            setting = null;
        }
        if (setting != null
                && setting.hasNumericSettingValue()
                && setting.getNumericSettingValue().hasValue()) {
            float durationInMonths = setting.getNumericSettingValue().getValue();
            return TimeUnit.HOURS.toMillis((long)(durationInMonths * 730f));
        } else {
            long durationInMs = settingName
                    .equals(GlobalSettingSpecs.CloudSavingsActionRetention.getSettingName())
                ? actionRetentionMs
                : volumeDeleteRetentionMs;
            logger.warn("Error retrieving {} setting - using previously set value of {} months",
                    spec.getDisplayName(), TimeUnit.MILLISECONDS.toHours(durationInMs) / 730f);
            return durationInMs;
        }
    }

    /**
     * Get the default value of an action expiration setting, in milliseconds.
     *
     * @param spec the GlobalSettingSpecs to get the default for
     * @return the default value for the setting, in milliseconds.
     */
    private long getDefaultSetting(GlobalSettingSpecs spec) {
        long durationInMonths;
        SettingProto.SettingSpec settingSpec = spec.createSettingSpec();
        durationInMonths = (long)settingSpec.getNumericSettingValueType().getDefault();
        return TimeUnit.HOURS.toMillis((long)(durationInMonths * 730f));
    }

    /**
     * Query the global settings manager to update the configured values.  This should be done once
     * at the beginning of periodic processing to avoid using stale configuration values.
     */
    public void updateValues() {
        this.actionRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsActionRetention);
        this.volumeDeleteRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsDeleteVolumeRetention);
        if (!actionRetentionMs.equals(previousActionRetentionMs)
                || !volumeDeleteRetentionMs.equals(previousVolumeDeleteRetentionMs)) {
            logger.info("Configured action expirations: volume delete = {}ms, all other actions = {}ms",
                    actionRetentionMs, volumeDeleteRetentionMs);
        }
        previousActionRetentionMs = actionRetentionMs;
        previousVolumeDeleteRetentionMs = volumeDeleteRetentionMs;
    }

    public long getActionRetentionMs() {
        return actionRetentionMs;
    }

    public long getVolumeDeleteRetentionMs() {
        return volumeDeleteRetentionMs;
    }
}
