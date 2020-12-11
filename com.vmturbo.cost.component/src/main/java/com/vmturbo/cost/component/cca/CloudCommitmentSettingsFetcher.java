package com.vmturbo.cost.component.cca;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
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
    private float fetchCloudCommitmentFloatSetting(SettingSpec settingSpec) {

        final GetSingleGlobalSettingRequest request = GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(settingSpec.getName())
                .build();

        final Setting setting = settingServiceClient.getGlobalSetting(request).getSetting();

        final boolean validRetentionSetting = setting != null
                && setting.hasNumericSettingValue()
                && setting.getNumericSettingValue().hasValue();

        return validRetentionSetting
                ? setting.getNumericSettingValue().getValue()
                : settingSpec.getNumericSettingValueType().getDefault();
    }

    /**
     * Fetches any cca setting which is a boolean.
     *
     * @param settingSpec The setting who's value we want to fetch.
     *
     * @return The boolean value for the setting we want to retrieve.
     */
    private boolean fetchCloudCommitmentBooleanSetting(SettingSpec settingSpec) {
        final GetSingleGlobalSettingRequest request = GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(settingSpec.getName())
                .build();

        final Setting setting = settingServiceClient.getGlobalSetting(request).getSetting();

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
     * Get the historical look back in days to get the analyzable demand for an entity.
     *
     * @return the historical look back in days.
     */
    public int historicalLookBackDays() {
        return (int)fetchCloudCommitmentFloatSetting(GlobalSettingSpecs.CloudCommitmentHistoricalLookbackPeriod.createSettingSpec());
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
    public float maxDemandPercentage() {
        return fetchCloudCommitmentFloatSetting(GlobalSettingSpecs.CloudCommitmentMaxDemandPercentage.createSettingSpec());
    }

    /**
     * Get the minimum savings over on demand we want for a Cloud Commitment to be recommended.
     *
     * @return Integer representing the minimum savings over on demand.
     */
    public float minimumSavingsOverOnDemand() {
        return fetchCloudCommitmentFloatSetting(GlobalSettingSpecs.CloudCommitmentMinimumSavingsOverOnDemand.createSettingSpec());
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

    /**
     * Indicates whether the historical demand selection will be scoped to match the purchasing scope.
     * If historical demand selection is not scoped, all recorded demand will be used in calculating
     * covered demand.
     * @return True, if historical demand selection should be scoped to match the purchase recommendation
     * scope.
     */
    public boolean scopeHistoricalDemandSelection() {
        return cloudCommitmentAnalysisConfigurationHolder.scopeHistoricalDemandSelection();
    }
}
