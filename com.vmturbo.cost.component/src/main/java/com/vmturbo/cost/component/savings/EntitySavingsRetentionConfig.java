package com.vmturbo.cost.component.savings;

import java.util.concurrent.TimeUnit;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Manages action retention configuration values.
 */
public class EntitySavingsRetentionConfig {
    private final SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient;
    private long actionRetentionMs;
    private long volumeDeleteRetentionMs;

    /**
     * Constructor.
     *
     * @param settingServiceClient settings service client.
     */
    public EntitySavingsRetentionConfig(SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient) {
        this.settingServiceClient = settingServiceClient;
        updateValues();
    }

    /**
     * Query the settings manager for a global setting.  The global setting in the UI is expressed in months, but
     * internally, milliseconds are used, so this returns the configured value in milliseconds.  A month is assumed to
     * have a duration of 730 hours.
     *
     * @param spec the {@link GlobalSettingSpecs} to query.
     * @return the global setting value expressed in milliseconds.  If the configuration value isn't explicitly set,
     * the defined default value will be returned instead.
     */
    private long getSetting(GlobalSettingSpecs spec) {
        String settingName = spec.getSettingName();

        final SettingProto.GetSingleGlobalSettingRequest request =
                SettingProto.GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(settingName)
                        .build();

        final SettingProto.Setting setting = settingServiceClient.getGlobalSetting(request).getSetting();
        long durationInMonths = 0L;
        if (setting != null
                && setting.hasNumericSettingValue()
                && setting.getNumericSettingValue().hasValue()) {
            durationInMonths = (long)setting.getNumericSettingValue().getValue();
        } else {
            SettingProto.SettingSpec settingSpec = spec.createSettingSpec();
            durationInMonths = (long)settingSpec.getNumericSettingValueType().getDefault();
        }
        return TimeUnit.HOURS.toMillis(durationInMonths * 730L);
    }

    /**
     * Query the global settings manager to update the configured values.  This should be done once
     * at the beginning of periodic processing to avoid using stale configuration values.
     */
    public void updateValues() {
        this.actionRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsActionRetention);
        this.volumeDeleteRetentionMs = getSetting(GlobalSettingSpecs.CloudSavingsDeleteVolumeRetention);
    }

    public long getActionRetentionMs() {
        return actionRetentionMs;
    }

    public long getVolumeDeleteRetentionMs() {
        return volumeDeleteRetentionMs;
    }
}
