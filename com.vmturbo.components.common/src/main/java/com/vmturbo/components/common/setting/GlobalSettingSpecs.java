package com.vmturbo.components.common.setting;

import static com.vmturbo.components.common.setting.SettingDTOUtil.createSettingCategoryPath;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.common.mail.MailConfiguration.EncryptionType;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OperatingSystem;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OsMigrationProfileOption;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredOfferingClass;

/**]
 * Enumeration for all the pre-built global settings.
 */
public enum GlobalSettingSpecs {

    SmtpServer("smtpServer", "SMTP Server",
            new StringSettingDataType(null, "*"),
            Collections.emptyList()),

    SmtpPort("smtpPort", "SMTP Port",
            numeric(1f/*min*/, 99999f/*max*/, 25f/*default*/),
            Collections.emptyList()),

    SmtpFromAddress("fromAddress", "'From' Address",
            new StringSettingDataType(null, "*"),
            Collections.emptyList()),

    SmtpUsername("smtpUsername", "Username",
            new StringSettingDataType(null, "*"),
            Collections.emptyList()),

    SmtpPassword("smtpPassword", "Password",
            new StringSettingDataType(null, "*"),
            Collections.emptyList()),

    SmtpEncription("smtpEncryption", "Encryption",
            new EnumSettingDataType<>(EncryptionType.NONE, EncryptionType.class),
            Collections.emptyList()),

    VmContent("VMContent", "Email content format - VM notifications",
            new StringSettingDataType("{6}: {5} \\nHost: {8}\\nDatastores: {9}\\nTarget: {7}\\nEvent: {0} - {4}\\nCategory: {1}\\nSeverity: {2}\\nState: {3}", "*"),
            Collections.emptyList()),

    PmContent("PMContent", "Email content format - PM notifications",
            new StringSettingDataType("{6}: {5} \\nDatastores: {10}\\nTarget: {7}\\nEvent: {0} - {4}\\nCategory: {1}\\nSeverity: {2}\\nState: {3}", "*"),
            Collections.emptyList()),

    StContent("StContent", "Email content format - Storage notifications",
            new StringSettingDataType("{6}: {5} \\nTarget: {7}\\nEvent: {0} - {4}\\nCategory: {1}\\nSeverity: {2}\\nState: {3}", "*"),
            Collections.emptyList()),

    StatsRetentionHours("numRetainedHours", "Hourly saved statistics [Hours]",
            numeric(24f/*min*/, 720f/*max*/, 72f/*default*/),
            Collections.emptyList()),

    StatsRetentionDays("numRetainedDays", "Daily saved statistics [Days]",
            numeric(35f/*min*/, 730f/*max*/, 60f/*default*/),
            Collections.emptyList()),

    StatsRetentionMonths("numRetainedMonths", "Monthly saved statistics [Months]",
            numeric(6f/*min*/, 240f/*max*/, 24f/*default*/),
            Collections.emptyList()),

    EmbeddedReportingRetentionDays("embeddedReportingRetentionDays", "Saved reporting data [Days]",
            numeric(30f/*min*/, 1825f/*max*/, 365f/*default*/),
            Collections.emptyList()),

    AuditLogRetentionDays("auditLogRetentionDays", "Saved audit-log entries [Days]",
            numeric(30f/*min*/, 1000f/*max*/, 365f/*default*/),
            Collections.emptyList()),

    PlanRetentionDays("planRetentionDays", "Saved plans [Days]",
            numeric(1f/*min*/, 365f/*max*/, 14f/*default*/),
            Collections.emptyList()),

    MaxConcurrentPlanInstances("maxConcurrentPlanInstances", "Maximum Number of Plan Instances Allowed To Run Concurrently",
            numeric(1f/*min*/, 1000f/*max*/, 2f/*default*/),
            Collections.emptyList()),

    MaxPlanInstancesPerPlan("maxPlanInstancesPerPlan", "Maximum Number of Plan Instances Per Plan",
            numeric(1f/*min*/, 10000f/*max*/, 10f/*default*/),
            Collections.emptyList()),

    // RI related settings
    RIPurchase("ri.purchase", "Type",
            new BooleanSettingDataType(true),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * Setting to indicate whether to run Cloud Commitment Analysis instead of legacy RI buy.
     */
    RunCloudCommitmentAnalysis("run.Cloud.Commitment.Analysis", "Run Cloud Commitment Analysis",
            new BooleanSettingDataType(false),
            Lists.newArrayList(CategoryPathConstants.RI)),

    CloudCommitmentAllocationRetentionDays(
            "cca.allocation.retentionDays",
            "Cloud Commitment Allocation Retention [Days]",
            numeric(1f, 90f, 35f),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * The setting for the cloud commitment historical lookback period.
     */
    CloudCommitmentHistoricalLookbackPeriod("cca.allocation.historicalLookBack",
            "Cloud Commitment Allocation Lookback [Days]",
            numeric(1f, 90f, 30f),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * The setting to log detailed summary for cloud commitment analysis.
     */
    CloudCommitmentLogDetailedSummary("cca.log.detailed.summary", "Cloud Commitment Log Detailed Summary",
            new BooleanSettingDataType(false),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * The setting to specify the maximum percentage of an entity to be covered by an RI.
     */
    CloudCommitmentMaxDemandPercentage("cca.max.demand.percentage", "Cloud Commitment Max Demand Percentage",
            numeric(1f, 100f, 100f),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * The setting to specify the minimum savings over on demand for an RI to be recommended.
     */
    CloudCommitmentMinimumSavingsOverOnDemand("cca.min.savings.onDemand", "Cloud Commitment Minimum Savings Over On Demand",
            numeric(1f, 100f, 10f),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * This setting is used to specify whether to include demand from terminated entities in CCA.
     */
    CloudCommitmentIncludeTerminatedEntities("cca.include.terminated.entities", "Cloud Commitment Include Terminated Entities",
            new BooleanSettingDataType(false),
            Lists.newArrayList(CategoryPathConstants.RI)),

    /**
     * Global RI Setting of demand type.
     */
    RIDemandType("ri.demandType", "Type",
            new EnumSettingDataType<>(RISettingsEnum.DemandType.CONSUMPTION,
                            RISettingsEnum.DemandType.class),
            Lists.newArrayList(CategoryPathConstants.RI)),
    /**
     * Global AWS RI setting for OfferingClass.
     */
    AWSPreferredOfferingClass("ri.aws.preferredOfferingClass", "Type",
            new EnumSettingDataType<>(RISettingsEnum.PreferredOfferingClass.STANDARD,
                            RISettingsEnum.PreferredOfferingClass.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AWS)),

    /**
     * Global AWS RI setting for preferred term, length of RI.
     */
    AWSPreferredTerm("ri.aws.preferredTerm", "Term",
            new EnumSettingDataType<>(RISettingsEnum.PreferredTerm.YEARS_1,
                                    RISettingsEnum.PreferredTerm.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AWS)),

    /**
     * Global AWS RI setting for preferred payment option.
     */
    AWSPreferredPaymentOption("ri.aws.preferredPaymentOption", "Payment",
            new EnumSettingDataType<>(RISettingsEnum.PreferredPaymentOption.ALL_UPFRONT,
                                    RISettingsEnum.PreferredPaymentOption.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AWS)),

    /**
     * Global AZURE RI setting for Offering Class.
     */
    AzurePreferredOfferingClass("ri.azure.preferredOfferingClass", "Type",
            new EnumSettingDataType<>(PreferredOfferingClass.CONVERTIBLE,
                                    RISettingsEnum.PreferredOfferingClass.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AZURE)),

    /**
     * Global AZURE RI setting for preferred term, length of RI.
     */
    AzurePreferredTerm("ri.azure.preferredTerm", "Term",
            new EnumSettingDataType<>(RISettingsEnum.PreferredTerm.YEARS_1,
                                    RISettingsEnum.PreferredTerm.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AZURE)),

    /**
     * Global AZURE RI setting for preferred payment option.
     */
    AzurePreferredPaymentOption("ri.azure.preferredPaymentOption", "Payment",
            new EnumSettingDataType<>(RISettingsEnum.PreferredPaymentOption.ALL_UPFRONT,
                                    RISettingsEnum.PreferredPaymentOption.class),
            Lists.newArrayList(CategoryPathConstants.RI, CategoryPathConstants.AZURE)),

    RecurrencePattern("recurrencePattern", "Recurrence Pattern",
            new StringSettingDataType("value: FREQ=WEEKLY;INTERVAL=2;BYHOUR=1", "*"),
            Collections.emptyList()),

    PreferredCurrentWeight("preferredCurrentWeight", "Weight",
            numeric(0.0f/*min*/, 1.0f/*max*/, 0.6f/*default*/),
            Collections.emptyList()),

    PreferredCoverage("preferredCoverage", "Coverage",
            numeric(0f/*min*/, 100f/*max*/, 80f/*default*/),
            Collections.emptyList()),

    RICoverageOverride("riCoverageOverride", "RI Coverage Override",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    DisableAllActions("disableAllActions", "Disable All Actions",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    /**
     * Max observation period for VM growth.
     */
    MaxVMGrowthObservationPeriod("maxVMGrowthObservationPeriod",
        "VM Growth Observation Period (in month)",
        numeric(1, 24, 1), Collections.emptyList()),

    /**
     * Don't allow providers to be over-provisioned without a limit.
     */
    AllowUnlimitedHostOverprovisioning("allowUnlimitedHostOverprovisioning",
            "Allow Unlimited Host Overprovisioning",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    /**
     * Enable ON-Prem virtual volume resource analysis
     */
    OnPremVolumeAnalysis("onPremVolumeAnalysis",
            "Enable analysis of on-prem Volumes",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    /**
     * Settings for OS migration.
     */
    SelectedMigrationProfileOption("selectedMigrationProfileOption", "Selected OS Migration Profile Option",
            new EnumSettingDataType<>(OsMigrationProfileOption.MATCH_SOURCE_TO_TARGET_OS,
                    OsMigrationProfileOption.class),
            Collections.emptyList()),

    MatchToSource("matchToSource", "Match source OS to target OS",
            new BooleanSettingDataType(true),
            Collections.emptyList()),

    ShowMatchSourceToTargetOsOption("showMatchSourceToTargetOsOption", "Show Match Source To Target OS Option",
            new BooleanSettingDataType(true),
            Collections.emptyList()),

    ShowByolOption("showByolOption", "Show BYOL Option",
            new BooleanSettingDataType(true),
            Collections.emptyList()),

    ShowCustomOsOption("showCustomOsOption", "Show Custom OS Option",
            new BooleanSettingDataType(true),
            Collections.emptyList()),

    LinuxTargetOs("linuxTargetOs", "Target OS for VMs with Linux OS",
            new EnumSettingDataType<>(OperatingSystem.LINUX, OperatingSystem.class),
            Collections.emptyList()),

    RhelTargetOs("rhelTargetOs", "Target OS for VMs with RHEL OS",
            new EnumSettingDataType<>(OperatingSystem.RHEL, OperatingSystem.class),
            Collections.emptyList()),

    SlesTargetOs("slesTargetOs", "Target OS for VMs with SLES OS",
            new EnumSettingDataType<>(OperatingSystem.SLES, OperatingSystem.class),
            Collections.emptyList()),

    WindowsTargetOs("windowsTargetOs", "Target OS for VMs with Windows OS",
            new EnumSettingDataType<>(OperatingSystem.WINDOWS, OperatingSystem.class),
            Collections.emptyList()),

    LinuxByol("linuxByol", "BYOL Target OS for VMs with Linux OS",
            new BooleanSettingDataType(true),
            Collections.emptyList()),

    RhelByol("rhelByol", "BYOL Target OS for VMs with RHEL OS",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    SlesByol("slesByol", "BYOL Target OS for VMs with SLES OS",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    WindowsByol("windowsByol", "BYOL Target OS for VMs with Windows OS",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    /**
     * Setting for Analytics and Telemetry
     */
    TelemetryEnabled("telemetryEnabled", "Enable Telemetry and Analytics",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    TelemetryTermsAccepted("telemetryTermsAccepted", "Determine whether user has accepted the use of Telemetry",
            new BooleanSettingDataType(false),
            Collections.emptyList()),

    /**
     * Targets Health Check settings: failed discovery count threshold to notify user.
     */
    FailedDiscoveryCountThreshold("failedDiscoveryCountThreshold",
                    "Number of successive failed discoveries after which we report the target as unhealthy",
                    numeric(1, 100, 3), Collections.emptyList()),

    /**
     * Targets Health Check settings: a  multiplier for the delayed data threshold (computed by default as
     * 'multiplier * rediscovery period').
     */
    DelayedDataThresholdMultiplier("delayedDataThresholdMultiplier", "A multiplier applied to a target's rediscovery"
                    + " interval. The resulting interval is used as a threshold for considering data to be delayed.",
                    numeric(1, 100, 10), Collections.emptyList()),

    /**
     * Cloud savings action execution retention durations
     */
    CloudSavingsDeleteVolumeRetention("expire.deleteVolume", "Delete volume execution retention",
            numeric(1f, 99999f, 12f), Collections.emptyList()),

    CloudSavingsActionRetention("expire.action", "Default action execution retention",
            numeric(1f, 99999f, 24f), Collections.emptyList());

    /**
     * A list of global settings that are visible to the UI.
     */
    public static final Set<GlobalSettingSpecs> VISIBLE_TO_UI = ImmutableSet.of(
        DisableAllActions, MaxVMGrowthObservationPeriod, AllowUnlimitedHostOverprovisioning,
            OnPremVolumeAnalysis);

    /**
     * Setting name to setting enumeration value map for fast access.
     */
    private static final Map<String, GlobalSettingSpecs> SETTINGS_MAP;

    private final String name;
    private final String displayName;
    private final SettingDataStructure<?> value;
    private final List<String> categoryPath;

    static {
        final GlobalSettingSpecs[] settings = GlobalSettingSpecs.values();
        final Map<String, GlobalSettingSpecs> result = new HashMap<>(settings.length);
        for (GlobalSettingSpecs setting : settings) {
            result.put(setting.getSettingName(), setting);
        }
        SETTINGS_MAP = Collections.unmodifiableMap(result);
    }

    GlobalSettingSpecs(@Nonnull String name, @Nonnull String displayName,
            @Nonnull SettingDataStructure value,
            @Nonnull List<String> categoryPath) {

        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.categoryPath = Objects.requireNonNull(categoryPath);
        this.value = Objects.requireNonNull(value);
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getSettingName() {
        return name;
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Finds a setting (enumeration value) by setting name.
     *
     * @param settingName setting name
     * @return setting enumeration value or empty optional, if not setting found by the name
     * @throws NullPointerException if {@code settingName} is null
     */
    @Nonnull
    public static Optional<GlobalSettingSpecs> getSettingByName(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Optional.ofNullable(SETTINGS_MAP.get(settingName));
    }

    /**
     * Constructs Protobuf representation of setting specification.
     *
     * @return Protobuf representation
     */
    @Nonnull
    public SettingSpec createSettingSpec() {
        final SettingSpec.Builder builder = SettingSpec.newBuilder()
                .setName(name)
                .setDisplayName(displayName)
                .setGlobalSettingSpec(
                    GlobalSettingSpec.newBuilder().build());
        if (hasCategoryPath()) {
            builder.setPath(createSettingCategoryPath(categoryPath));
        }
        value.build(builder);
        return builder.build();
    }

    /**
     * Extract the value from a setting.
     *
     * @param <T> type of a setting value
     * @param setting setting
     * @param cls class of a setting value
     * @return value, null if not present
     */
    @Nullable
    public <T> T getValue(@Nullable Setting setting, @Nonnull Class<T> cls) {
        Objects.requireNonNull(setting);
        Objects.requireNonNull(cls);
        Object result = value.getValue(setting);
        return cls.isInstance(result) ? cls.cast(result) : null;
    }

    private boolean hasCategoryPath() {
        return (categoryPath != null && !categoryPath.isEmpty());
    }

    public List<String> getCategoryPaths() {
        return Collections.unmodifiableList(categoryPath);
    }

    @Nonnull
    private static SettingDataStructure<?> numeric(float min, float max, float defaultValue) {
        return new NumericSettingDataType(min, max, defaultValue);
    }
}

