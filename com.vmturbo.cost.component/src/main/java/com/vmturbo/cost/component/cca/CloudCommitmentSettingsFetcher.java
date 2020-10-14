package com.vmturbo.cost.component.cca;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.component.cca.configuration.CloudCommitmentAnalysisConfigurationHolder;

/**
 * Class to fetch global settings relevant to a run of CCA.
 */
public class CloudCommitmentSettingsFetcher {

    private final SettingServiceBlockingStub settingServiceClient;

    private final CloudCommitmentAnalysisConfigurationHolder cloudCommitmentAnalysisConfigurationHolder;

    /**
     * List of settings to retrieve from global spec settings.
     */
    private final List<String> settingsToFetch = Lists.newArrayList(GlobalSettingSpecs.CloudCommitmentMinimumSavingsOverOnDemand.createSettingSpec().getName(),
            GlobalSettingSpecs.CloudCommitmentMaxDemandPercentage.createSettingSpec().getName(), GlobalSettingSpecs
                    .CloudCommitmentHistoricalLookbackPeriod.createSettingSpec().getName(), GlobalSettingSpecs.RunCloudCommitmentAnalysis.createSettingSpec().getName(),
            GlobalSettingSpecs.CloudCommitmentLogDetailedSummary.createSettingSpec().getName(), GlobalSettingSpecs.CloudCommitmentIncludeTerminatedEntities.createSettingSpec().getName());

    private Map<String, Setting> settingsMap = new HashMap<>();

    /**
     * The constructor of the CloudCommitmentSettingsFetcher. Takes in the settings service client.
     *
     * @param settingServiceClient The input settings service client.
     * @param cloudCommitmentAnalysisConfigurationHolder This holds settings specific to CCA specified
     * in the config map used in constructing the CCA request.
     */
    public CloudCommitmentSettingsFetcher(@Nonnull SettingServiceBlockingStub settingServiceClient,
            @Nonnull CloudCommitmentAnalysisConfigurationHolder cloudCommitmentAnalysisConfigurationHolder) {
        this.settingServiceClient = settingServiceClient;
        this.cloudCommitmentAnalysisConfigurationHolder = cloudCommitmentAnalysisConfigurationHolder;
    }

    /**
     * Fetches any cca setting which is an Integer.
     *
     * @param settingSpec The setting who's value we want to fetch.
     *
     * @return The integer value for the setting we want to retrieve.
     */
    private int fetchCloudCommitmentIntegerSetting(SettingSpec settingSpec) {
        final Setting setting = settingsMap.get(settingSpec.getName());

        final boolean validRetentionSetting = setting != null
                && setting.hasNumericSettingValue()
                && setting.getNumericSettingValue().hasValue();

        return validRetentionSetting
                ? (int)setting.getNumericSettingValue().getValue()
                : (int)settingSpec.getNumericSettingValueType().getDefault();
    }

    /**
     * Populate the settings map which has the list of settings to fetch.
     */
    public void populateSettingsMap() {
        settingServiceClient.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder()
                        .addAllSettingSpecName(settingsToFetch).build())
                        .forEachRemaining(setting -> {
                            settingsMap.put(setting.getSettingSpecName(), setting);
                        });
    }

    /**
     * Fetches any cca setting which is a boolean.
     *
     * @param settingSpec The setting who's value we want to fetch.
     *
     * @return The boolean value for the setting we want to retrieve.
     */
    private boolean fetchCloudCommitmentBooleanSetting(SettingSpec settingSpec) {
        final Setting setting = settingsMap.get(settingSpec.getName());

        final boolean validRetentionSetting = setting != null
                && setting.hasBooleanSettingValue();

        return validRetentionSetting ? setting.getBooleanSettingValue().getValue() : false;
    }

    /**
     * Setting which indicated if we want to run Cloud Commitment Analysis instead of legacy RI buy.
     *
     * @return true if we want to run CCA>
     */
    public boolean runCloudCommitmentAnalysis() {
        return fetchCloudCommitmentBooleanSetting(GlobalSettingSpecs.RunCloudCommitmentAnalysis.createSettingSpec());
    }

    /**
     * Get the historical look back period to get the analyzable demand for an entity.
     *
     * @return the historical look back period.
     */
    public long historicalLookBackPeriod() {
        return Instant.now().minus(fetchCloudCommitmentIntegerSetting(GlobalSettingSpecs
                .CloudCommitmentHistoricalLookbackPeriod.createSettingSpec()), ChronoUnit.DAYS).toEpochMilli();
    }

    /**
     * Check if we want to log a detailed summary of the Cloud Commitment Analysis.
     *
     * @return true if we want to log the detailed summary,
     */
    public boolean logDetailedSummary() {
        return fetchCloudCommitmentBooleanSetting(GlobalSettingSpecs.CloudCommitmentLogDetailedSummary.createSettingSpec());
    }

    /**
     * Get the setting to check if we want o include terminated entity demand in Cloud Commitment Analysis.
     *
     * @return true if we want to include terminated entity demand in CCA.
     */
    public boolean includeTerminatedEntities() {
        return fetchCloudCommitmentBooleanSetting(GlobalSettingSpecs.CloudCommitmentIncludeTerminatedEntities.createSettingSpec());
    }

    /**
     * Get the maximum percentage of an entity we want to be covered by a Cloud Commitment.
     *
     * @return Integer representing the maximum demand percentage.
     */
    public int maxDemandPercentage() {
        return fetchCloudCommitmentIntegerSetting(GlobalSettingSpecs.CloudCommitmentMaxDemandPercentage.createSettingSpec());
    }

    /**
     * Get the minimum savings over on demand we want for a Cloud Commitment to be recommended.
     *
     * @return Integer representing the minimum savings over on demand.
     */
    public int minimumSavingsOverOnDemand() {
        return fetchCloudCommitmentIntegerSetting(GlobalSettingSpecs.CloudCommitmentMinimumSavingsOverOnDemand.createSettingSpec());
    }

    /**
     * Get the minimum stability milliseconds attribute from the configuration holder.
     *
     * @return A long representing the minimum stability milliseconds attribute.
     */
    public long minSatbilityMilis() {
        return cloudCommitmentAnalysisConfigurationHolder.minStabilityMillis();
    }

    /**
     * Get the setting to check if we want to include suspended entity demand in Cloud Commitment Analysis.
     *
     * @return true if we want to include suspended entity demand in CCA.
     */
    public boolean allocationSuspended() {
        return cloudCommitmentAnalysisConfigurationHolder.allocationSuspended();
    }

    /**
     * Get the setting to check if we want to include flexible entity demand in Cloud Commitment Analysis.
     *
     * @return true if we want to include flexible entity demand in CCA.
     */
    public boolean allocationFlexible() {
        return cloudCommitmentAnalysisConfigurationHolder.allocationFlexible();
    }
}
