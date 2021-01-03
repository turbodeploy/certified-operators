package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredOfferingClass;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredPaymentOption;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredTerm;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AzurePreferredTerm;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.CloudCommitmentHistoricalLookbackPeriod;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.CloudCommitmentIncludeTerminatedEntities;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.RIDemandType;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.RIPurchase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingValueEntityTypeKey;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.service.SettingsService;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.ConfigChangesApiDTO;
import com.vmturbo.api.dto.scenario.IncludedCouponsApiDTO;
import com.vmturbo.api.dto.scenario.LoadChangesApiDTO;
import com.vmturbo.api.dto.scenario.MaxUtilizationApiDTO;
import com.vmturbo.api.dto.scenario.MigrateObjectApiDTO;
import com.vmturbo.api.dto.scenario.RelievePressureObjectApiDTO;
import com.vmturbo.api.dto.scenario.RemoveConstraintApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.dto.scenario.UtilizationApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
//import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.ConstraintType;
import com.vmturbo.api.enums.DestinationEntityType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.api.mappers.EnumMapper;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.DetailsCase;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.BusinessAccount;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.ConstraintGroup;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.GlobalIgnoreEntityType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IncludedCoupons;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel.Builder;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.OSMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OperatingSystem;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OsMigrationProfileOption;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Maps scenarios between their API DTO representation and their protobuf representation.
 */
public class ScenarioMapper {

    private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

    /**
     * The string constant expected by the UI for a custom scenario type.
     * Used as a fallback if a previously saved scenario doesn't have a set type for whatever
     * reason.
     */
    private static final String CUSTOM_SCENARIO_TYPE = "CUSTOM";

    private static final String DECOMMISSION_HOST_SCENARIO_TYPE = "DECOMMISSION_HOST";

    private static final String DISABLED = "DISABLED";

    /**
     * The string constant the UI uses to identify a plan scoped to the global environment.
     */
    private static final String MARKET_PLAN_SCOPE_CLASSNAME = "Market";

    /**
     * A default market scope that we pass back to the UI when there is no explicit scope defined
     * in a plan.
     */
    private static final BaseApiDTO MARKET_PLAN_SCOPE;

    /**
     * Name of alleviate pressure plan type.
     */
    static final String ALLEVIATE_PRESSURE_PLAN_TYPE = "ALLEVIATE_PRESSURE";

    /**
     * Plan rate of resize is high.
     */
    private static final float PLAN_RATE_OF_RESIZE = 3.0f;

    /**
     * Supported RI Purchase Profile Settings.
     */
    private static final EnumSet<GlobalSettingSpecs> SUPPORTED_RI_PROFILE_SETTINGS = EnumSet
                    .of(AWSPreferredOfferingClass, AWSPreferredPaymentOption, AWSPreferredTerm,
                            AzurePreferredTerm, RIDemandType,
                            CloudCommitmentHistoricalLookbackPeriod,
                            CloudCommitmentIncludeTerminatedEntities);

    /**
     * Map of source of target type from where the business account originates indexed by probe type.
     */
    private static final Map<String, CloudType> SUPPORTED_CLOUD_TYPE = ImmutableMap.of(
            SDKProbeType.AWS.getProbeType(), CloudType.AWS,
            SDKProbeType.AWS_BILLING.getProbeType(), CloudType.AWS,
            SDKProbeType.AZURE.getProbeType(), CloudType.AZURE);

    static {
        MARKET_PLAN_SCOPE = new BaseApiDTO();
        MARKET_PLAN_SCOPE.setUuid(MARKET_PLAN_SCOPE_CLASSNAME);
        MARKET_PLAN_SCOPE.setDisplayName("Global Environment");
        MARKET_PLAN_SCOPE.setClassName(MARKET_PLAN_SCOPE_CLASSNAME);
    }

    /** this set is used for creating settings based on max utilization plan configurations. The
     * list
     * of settings chosen is based on the classic implementation for this plan configuration, which
     * is hardcoded to set commodity utilization thresholds for these three commodity types.
     */
    public static final Set<String> MAX_UTILIZATION_SETTING_SPECS = ImmutableSet.of(
        EntitySettingSpecs.CpuUtilization.getSettingName(), EntitySettingSpecs.MemoryUtilization.getSettingName(),
        EntitySettingSpecs.StorageAmountUtilization.getSettingName()
    );

    private static final Logger logger = LogManager.getLogger();

    /**
     * Suffix for OS migration settings specifying target OS, eg "linuxTargetOs", "windowsTargetOs".
     * etc.
     */
    private static final String TARGET_OS_SETTING_SUFFIX = "TargetOs";

    /**
     * Suffix for OS migration settings specifying Bring Your Own License, eg "linuxByol",
     * "windowsByol", etc.
     */
    private static final String BYOL_SETTING_SUFFIX = "Byol";

    /**
     * Map from OS identifiers as used in the API DTO to those used in the Protobuf DTO.
     */
    private static final BiMap<OperatingSystem, OSType> apiOsToProtobufOs =
        ImmutableBiMap.<OperatingSystem, OSType>of(
            OperatingSystem.LINUX, OSType.LINUX,
            OperatingSystem.RHEL, OSType.RHEL,
            OperatingSystem.SLES, OSType.SUSE,
            OperatingSystem.WINDOWS, OSType.WINDOWS
        );

    /**
     * Map from OS identifiers as used in the Protobuf DTO to those used in the API DTO.
     */
    private static final BiMap<OSType, OperatingSystem> protobufOsToApiOs =
        apiOsToProtobufOs.inverse();

    private static final List<ConstraintType> ALLEVIATE_PRESSURE_IGNORE_CONSTRAINTS = Arrays.asList(
                    ConstraintType.NetworkCommodity, ConstraintType.StorageClusterCommodity,
                    ConstraintType.DataCenterCommodity);

    private final EnumMapper<OfferingClass> riOfferingClassMapper = EnumMapper.of(OfferingClass.class);
    private final EnumMapper<PaymentOption> riPaymentOptionMapper = EnumMapper.of(PaymentOption.class);
    private final EnumMapper<PreferredTerm> riTermMapper = EnumMapper.of(PreferredTerm.class);
    private final EnumMapper<DemandType> riDemandTypeMapper = EnumMapper.of(DemandType.class);

    private final TemplatesUtils templatesUtils;

    private final SettingsService settingsService;

    private final RepositoryApi repositoryApi;

    private final SettingsManagerMapping settingsManagerMapping;

    private final SettingsMapper settingsMapper;

    private PoliciesService policiesService;

    private final GroupServiceBlockingStub groupRpcService;

    private final GroupMapper groupMapper;

    private final UuidMapper uuidMapper;

    public ScenarioMapper(@Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final TemplatesUtils templatesUtils,
                          @Nonnull final SettingsService settingsService,
                          @Nonnull final SettingsManagerMapping settingsManagerMapping,
                          @Nonnull final SettingsMapper settingsMapper,
                          @Nonnull final PoliciesService policiesService,
                          @Nonnull final GroupServiceBlockingStub groupRpcService,
                          @Nonnull final GroupMapper groupMapper,
                          @Nonnull final UuidMapper uuidMapper) {

        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
        this.settingsService = Objects.requireNonNull(settingsService);
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.policiesService = Objects.requireNonNull(policiesService);
        this.groupRpcService = Objects.requireNonNull(groupRpcService);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
    }

    /**
     * Map a ScenarioApiDTO to an equivalent {@link ScenarioInfo}.
     *
     * @param name The name of the scenario.
     * @param dto The DTO to be converted.
     * @return The ScenarioInfo equivalent of the input DTO.
     * @throws OperationFailedException UuidMapper throws, one of the underlying operations
     *          required to map the UUID to an {@link UuidMapper.ApiId} fails
     * @throws InvalidOperationException e.g in case if alleviate pressure plan, if we don't get
     *         cluster information.
     * @throws IllegalArgumentException when constraint is of an unsupported configuration
     * @throws UnknownObjectException if an invalid setting manager UUID is provided
     */
    @Nonnull
    public ScenarioInfo toScenarioInfo(final String name,
                                       @Nonnull final ScenarioApiDTO dto)
            throws OperationFailedException, InvalidOperationException, IllegalArgumentException,
                UnknownObjectException {
        final ScenarioInfo.Builder infoBuilder = ScenarioInfo.newBuilder();
        if (name != null) {
            infoBuilder.setName(name);
        }
        if (dto.getType() != null) {
            infoBuilder.setType(dto.getType());
            infoBuilder.addAllChanges(addChangesRelevantToPlanType(dto));
        }
        // TODO: Right now, API send template id and entity id together for Topology Addition, and it
        // doesn't have a field to tell whether it is template id or entity id. Later on,
        // API should tell us if it is template id or not, please see (OM-26675).
        final Set<Long> templateIds = getTemplatesIds(dto.getTopologyChanges());

        infoBuilder.addAllChanges(getTopologyChanges(dto.getTopologyChanges(),
            dto.getConfigChanges(), templateIds, getScope(dto.getScope())));
        infoBuilder.addAllChanges(getConfigChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getPolicyChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getLoadChanges(dto.getLoadChanges()));
        // check the scenario and add default automation settings if necessary
        infoBuilder.addAllChanges(checkAndCreateDefaultAutomationSettingsForPlan(dto));
        infoBuilder.setScope(getScope(dto.getScope()));
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config
        return infoBuilder.build();
    }

    /*
     * Provides changes relevant for given plan type.
     */
    private Iterable<ScenarioChange> addChangesRelevantToPlanType(ScenarioApiDTO dto) throws InvalidOperationException {
        final List<ScenarioChange> changes = new ArrayList<>();
        switch (dto.getType()) {
            case ALLEVIATE_PRESSURE_PLAN_TYPE:
                final List<RelievePressureObjectApiDTO> relievePressureList =
                                dto.getTopologyChanges().getRelievePressureList();
                if (CollectionUtils.isEmpty(relievePressureList)) {
                    throw new InvalidOperationException(
                                    "Cluster list is empty for alleviate pressure plan.");
                }
                // 1) Set Merge Policy for given clusters.
                changes.add(getMergePolicyForSourceAndDestinationClusters(relievePressureList));
                // 2) Apply constraints on hot cluster.
                changes.add(getIgnoreConstraintsForHotCluster(relievePressureList));
                // 3) Disable provision, suspend, resize and reconfigure.
                changes.add(getChangeWithGlobalSettingsDisabled(ConfigurableActionSettings.Provision));
                changes.add(getChangeWithGlobalSettingsDisabled(ConfigurableActionSettings.Suspend));
                changes.add(getChangeWithGlobalSettingsDisabled(ConfigurableActionSettings.Resize));
                changes.add(getChangeWithGlobalSettingsDisabled(ConfigurableActionSettings.Reconfigure));
                break;
            default:
                break;
        }
        return changes;
    }

    private ScenarioChange getIgnoreConstraintsForHotCluster(
                    List<RelievePressureObjectApiDTO> relievePressureList) {
        final PlanChanges.Builder planChangesBuilder = PlanChanges.newBuilder();
        relievePressureList.stream()
           .flatMap(relievePressureDto -> relievePressureDto.getSources().stream())
           .map(obj -> Long.valueOf(obj.getUuid()))
           .distinct()
           .flatMap(clusterId -> ALLEVIATE_PRESSURE_IGNORE_CONSTRAINTS.stream()
               // Create an IgnoreConstraint message for each commodity to ignore for the cluster.
                       .map(constraintType -> IgnoreConstraint.newBuilder()
                               .setIgnoreGroup(ConstraintGroup.newBuilder()
                                       .setCommodityType(constraintType.name())
                                       .setGroupUuid(clusterId)
                                       .build())
                               .build()))
           .forEach(planChangesBuilder::addIgnoreConstraints);


        return ScenarioChange.newBuilder()
            .setPlanChanges(planChangesBuilder)
            .build();
    }

    /*
     * Provides globally disabled setting for given entity specification.
     */
    private ScenarioChange getChangeWithGlobalSettingsDisabled(ConfigurableActionSettings spec) {
        return ScenarioChange.newBuilder()
            .setSettingOverride(SettingOverride.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(spec.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(DISABLED))))
            .build();
    }

    /*
     * Create merge policy out of cluster ids in relieve pressure list.
     */
    private ScenarioChange getMergePolicyForSourceAndDestinationClusters(List<RelievePressureObjectApiDTO> relievePressureList)
                    throws InvalidOperationException {
        MergePolicy.Builder mergePolicyBuilder = MergePolicy.newBuilder()
            .setMergeType(MergeType.CLUSTER);

        relievePressureList.forEach(element -> {
            mergePolicyBuilder.addAllMergeGroupIds(element.getSources().stream()
                .map(obj -> Long.valueOf(obj.getUuid()))
                .collect(Collectors.toList()));
            mergePolicyBuilder.addAllMergeGroupIds(element.getDestinations().stream()
                .map(obj -> Long.valueOf(obj.getUuid()))
                .collect(Collectors.toList()));
        });

        // Should be two groups : one source and one destination for alleviate pressure plan.
        if (mergePolicyBuilder.getMergeGroupIdsCount() != 2) {
            throw new InvalidOperationException(
                            "Invalid number of clusters : " + mergePolicyBuilder.getMergeGroupIdsCount());
        }

        // Source and destination clusters should be different
        if (mergePolicyBuilder.getMergeGroupIds(0) == mergePolicyBuilder.getMergeGroupIds(1)) {
            throw new InvalidOperationException(
                            "Source and destination clusters are same.");
        }

        ScenarioChange change = ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setPolicyChange(PolicyChange.newBuilder()
                    .setPlanOnlyPolicy(Policy.newBuilder()
                        .setPolicyInfo(PolicyInfo.newBuilder()
                            .setMerge(mergePolicyBuilder)))))
            .build();
        return change;
    }

    @Nonnull
    private Iterable<ScenarioChange> getLoadChanges(@Nonnull LoadChangesApiDTO loadChangesApiDTO) {
        List<ScenarioChange> changes = new ArrayList<>();

        if (loadChangesApiDTO == null) {
            return changes;
        }

        // Set utilization changes.
        List<UtilizationApiDTO> utilizationList = loadChangesApiDTO.getUtilizationList();
        if (CollectionUtils.isNotEmpty(utilizationList)) {
            changes.addAll(getUtilizationChanges(utilizationList));
        }

        // get max utilization changes
        List<MaxUtilizationApiDTO> maxUtilizationList = loadChangesApiDTO.getMaxUtilizationList();
        if (CollectionUtils.isNotEmpty(maxUtilizationList)) {
            changes.addAll(convertMaxUtilizationToSettingOverride(maxUtilizationList));
        }

        // Set baseline changes
        String baselineDate = loadChangesApiDTO.getBaselineDate();
        if (!StringUtils.isEmpty(baselineDate)) {
            changes.add(getHistoricalBaselineChanges(baselineDate));
        }

        return changes;
    }

    @Nonnull
    private ScenarioChange getHistoricalBaselineChanges(@Nonnull String baselineDate) {
        final ScenarioChange change = ScenarioChange.newBuilder()
                   .setPlanChanges(PlanChanges.newBuilder()
                       .setHistoricalBaseline(HistoricalBaseline.newBuilder()
                           .setBaselineDate(DateTimeUtil.parseTime(baselineDate))))
                   .build();
        return change;
    }

    /**
     * Convert any {@link UtilizationApiDTO} objects to {@link ScenarioChange}.
     *
     * @param utilizationList a list of the utilization settings from the UI
     * @return a list of matching {@link ScenarioChange} objects
     */
    @VisibleForTesting
    @Nonnull
    List<ScenarioChange> getUtilizationChanges(@Nonnull final List<UtilizationApiDTO> utilizationList) {
        List<ScenarioChange> scenarioChanges = new ArrayList<>(utilizationList.size());

        for (UtilizationApiDTO utilization : utilizationList) {
            final UtilizationLevel.Builder utilizationLevel = UtilizationLevel.newBuilder();

            utilizationLevel.setPercentage(utilization.getPercentage());

            if (utilization.getTarget() != null &&
                    utilization.getTarget().getUuid() != null) {
                utilizationLevel.setGroupOid(Long.parseLong(utilization.getTarget().getUuid()));
            }

            scenarioChanges.add(ScenarioChange.newBuilder()
                    .setPlanChanges(PlanChanges.newBuilder()
                            .setUtilizationLevel(utilizationLevel))
                    .build());
        }

        return scenarioChanges;
    }

    /**
     * Convert any {@link MaxUtilizationApiDTO} objects to {@link ScenarioChange} objects capturing
     * the effects of the max utilization setting.
     *
     * @param maxUtilizations a list of the max utilization settings from the UI
     * @return a list of matching {@link ScenarioChange} objects
     */
    @VisibleForTesting
    @Nonnull
    List<ScenarioChange> convertMaxUtilizationToSettingOverride(@Nonnull final List<MaxUtilizationApiDTO> maxUtilizations) {
        List<ScenarioChange> scenarioChanges = new ArrayList<>(maxUtilizations.size());

        for (MaxUtilizationApiDTO maxUtilization : maxUtilizations) {
            SettingOverride.Builder settingBuilder = SettingOverride.newBuilder();
            final Integer maxUtilizationPercentage = maxUtilization.getMaxPercentage();
            // if the UUID is null or "Market", we don't set the Group OID, since by default the scope is "Market"
            if (maxUtilization.getTarget() != null
                && maxUtilization.getTarget().getUuid() != null
                && !(MarketMapper.MARKET.equals(maxUtilization.getTarget().getUuid()))) {
                // get the target oid for this change
                settingBuilder.setGroupOid(Long.parseLong(maxUtilization.getTarget().getUuid()));
            }

            if (maxUtilization.getSelectedEntityType() != null) {
                settingBuilder.setEntityType(
                    ApiEntityType.fromStringToSdkType(maxUtilization.getSelectedEntityType()));
            }
            // We need to map from the UI general max utilization setting to the more specific
            // max utilization settings contained in MAX_UTILIZATION_SETTING_SPECS.
            for (String settingName : MAX_UTILIZATION_SETTING_SPECS) {
                if (!EntitySettingSpecs.getSettingByName(settingName).isPresent()) {
                    continue;
                }
                SettingSpec spec =
                    EntitySettingSpecs.getSettingByName(settingName).get().getSettingSpec();
                if (maxUtilization.getSelectedEntityType() == null
                    || isSettingSpecForEntityType(EntitySettingSpecs.getSettingByName(settingName).get().getSettingSpec(),
                    settingBuilder.getEntityType())) {
                    final Setting setting = createMaxUtilizationSetting(settingName,
                        maxUtilizationPercentage);
                    scenarioChanges.add(ScenarioChange.newBuilder()
                        .setSettingOverride(settingBuilder.setSetting(setting).build())
                        .build());
                }
            }
        }
        return scenarioChanges;
    }

    /**
     * Create a max utilization setting given a name and a percentage.
     *
     * @param settingName the name of the setting
     * @param percentage of the utilization level
     * @return the new Setting
     */
    private Setting createMaxUtilizationSetting(String settingName, float percentage) {
        return Setting.newBuilder()
            .setSettingSpecName(settingName)
            .setNumericSettingValue(NumericSettingValue.newBuilder()
                .setValue(percentage).build())
            .build();
    }

    /**
     * Given an entity and a settings the method will return if the setting can be applied to that
     * entity.
     *
     * @param settingSpec the setting
     * @param entityType the entity
     * @return if the setting can applied to the entity
     */
    public boolean isSettingSpecForEntityType(SettingSpec settingSpec, Integer entityType) {
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();
        // if scope is "all entity type" then we are true
        if (scope.hasAllEntityType()) {
            return true;
        }

        // otherwise scope may be a set of entity types.
        if (scope.hasEntityTypeSet()) {
            // return true if the entity type is in the entity type set.
            return scope.getEntityTypeSet().getEntityTypeList().contains(entityType);
        }
        // default = no
        return false;
    }
    /**
     * Extract all the policy changes, both in the enable list and in the disable list,
     * from a {@link ConfigChangesApiDTO}, and convert them to a list of {@link
     * ScenarioChange}s, each carrying one {@link PolicyChange}.
     *
     * @param configChanges configuration changes received from the UI
     * @return the policy changes
     */
    private Iterable<ScenarioChange> getPolicyChanges(ConfigChangesApiDTO configChanges) {
        Set<ScenarioChange> changes = new HashSet<>();
        if (configChanges != null) {
            List<PolicyApiDTO> enabledList = configChanges.getAddPolicyList();
            if (enabledList != null) {
                changes.addAll(getPolicyChanges(enabledList));
            }
            List<PolicyApiDTO> disabledList = configChanges.getRemovePolicyList();
            if (disabledList != null) {
                changes.addAll(getPolicyChanges(disabledList));
            }
        }
        return changes;
    }

    /**
     * Map a list of {@link PolicyApiDTO}s to a collection of {@link ScenarioChange}s,
     * each carrying one {@link PolicyChange}.
     *
     * @param policyDTOs list of policies received from the API
     * @return collection of scenario changes to be used by the server
     */
    private Collection<ScenarioChange> getPolicyChanges(List<PolicyApiDTO> policyDTOs) {
        return policyDTOs.stream()
                    .map(this::mapPolicyChange)
                    .collect(Collectors.toList());
    }

    /**
     * Map a policy in the UI representation ({@link PolicyApiDTO}) to a policy in the
     * server representation (one {@link PolicyChange} within a {@link ScenarioChange}).
     *
     * <p>The Plan UI passes either a reference (by uuid) to an existing server policy, or
     * a full policy definition (with references to server groups) that exists only in
     * the context of that specific plan. The latter will have a null uuid.
     *
     * @param dto a UI representation of a policy
     * @return a scenario change with one policy change
     */
    private ScenarioChange mapPolicyChange(PolicyApiDTO dto) {
        return dto.getUuid() != null
                    // this is a reference to a server policy
                    ? ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder().setPolicyChange(PolicyChange.newBuilder()
                                .setEnabled(dto.isEnabled())
                                .setPolicyId(Long.valueOf(dto.getUuid()))
                                .build()).build())
                        .build()
                    // this is a full policy definition
                    : ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder().setPolicyChange(PolicyChange.newBuilder()
                        .setEnabled(true)
                        .setPlanOnlyPolicy(policiesService.toPolicy(dto))
                        .build()).build())
                    .build();
    }

    /**
     * Map a {@link Scenario} to an equivalent {@link ScenarioApiDTO}.
     *
     * @param scenario The scenario to be converted.
     * @return The ScenarioApiDTO equivalent of the scenario.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public ScenarioApiDTO toScenarioApiDTO(@Nonnull final Scenario scenario)
            throws ConversionException, InterruptedException {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        Scenario convertedScenario = toApiChanges(scenario);
        final ScenarioChangeMappingContext context = new ScenarioChangeMappingContext(repositoryApi,
                templatesUtils, groupRpcService, groupMapper, convertedScenario.getScenarioInfo().getChangesList());
        boolean isAlleviatePressurePlan = false;
        dto.setUuid(Long.toString(convertedScenario.getId()));
        dto.setDisplayName(convertedScenario.getScenarioInfo().getName());
        if (convertedScenario.getScenarioInfo().hasType()) {
            dto.setType(convertedScenario.getScenarioInfo().getType());
            isAlleviatePressurePlan = convertedScenario.getScenarioInfo().getType()
                .equals(ALLEVIATE_PRESSURE_PLAN_TYPE);
        } else {
            dto.setType(CUSTOM_SCENARIO_TYPE);
        }
        dto.setScope(buildApiScopeObjects(convertedScenario));
        dto.setProjectionDays(buildApiProjChanges());
        dto.setTopologyChanges(buildApiTopologyChanges(convertedScenario.getScenarioInfo().getChangesList(),
            context));

        if (isAlleviatePressurePlan) {
            // Show cluster info in UI for Alleviate pressure plan.
            updateApiDTOForAlleviatePressurePlan(convertedScenario.getScenarioInfo()
                .getScope().getScopeEntriesList(), dto);
        } else {
            // Show configuration settings in UI when it is not Alleviate pressure plan.
            dto.setConfigChanges(buildApiConfigChanges(convertedScenario.getScenarioInfo().getChangesList(), context));
        }
        dto.setLoadChanges(buildLoadChangesApiDTO(convertedScenario.getScenarioInfo().getChangesList(), context));
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config
        return dto;
    }

    /**
     * Transform changes into a consistent format used by the UI/API. This is needed because some
     * settings that in the backend are considered settings override, in the api and then in the
     * ui are considered different fields. One example is the maxUtilizationSettings, which is
     * defined as a setting override in the backend, but not in the ui/api.
     *
     * @param scenario The scenario to be converted.
     * @return The converted scenario.
     */
    @VisibleForTesting
    protected Scenario toApiChanges(@Nonnull final Scenario scenario) {
        final Map<Integer, MaxUtilizationLevel> entityTypeToMaxUtilization = new HashMap<>();
        final HashMap<Long, MaxUtilizationLevel> groupIdToMaxUtilization = new HashMap<>();
        final List<ScenarioChange> changes = new ArrayList<>();
        for (ScenarioChange change : scenario.getScenarioInfo().getChangesList()) {
            if (change.hasSettingOverride() && MAX_UTILIZATION_SETTING_SPECS
                    .contains(change.getSettingOverride().getSetting().getSettingSpecName())) {
                int entityType = change.getSettingOverride().getEntityType();
                Builder maxUtilizationLevelBuilder =
                    MaxUtilizationLevel.newBuilder()
                        .setSelectedEntityType(entityType)
                        .setPercentage((int)change
                            .getSettingOverride().getSetting().getNumericSettingValue().getValue());
                if (change.getSettingOverride().hasGroupOid()) {
                    Long groupId = change.getSettingOverride().getGroupOid();
                    maxUtilizationLevelBuilder.setGroupOid(groupId);
                    groupIdToMaxUtilization.put(groupId, maxUtilizationLevelBuilder.build());
                } else if (!entityTypeToMaxUtilization.containsKey(entityType)) {
                    entityTypeToMaxUtilization.put(entityType, maxUtilizationLevelBuilder.build());
                }
            } else {
                changes.add(change);
            }
        }
        // The UI needs one maxUtilization setting per entity type
        for (int entityType : entityTypeToMaxUtilization.keySet()) {
            changes.add(createMaxUtilizationScenarioChange(entityTypeToMaxUtilization.get(entityType)));
        }
        // The UI needs one maxUtilization setting per group
        for (Long groupId : groupIdToMaxUtilization.keySet()) {
            changes.add(createMaxUtilizationScenarioChange(groupIdToMaxUtilization.get(groupId)));
        }
        return scenario.toBuilder()
            .setScenarioInfo(scenario
                .getScenarioInfo().toBuilder().clearChanges()
                .addAllChanges(changes))
            .build();
    }

    private ScenarioChange createMaxUtilizationScenarioChange(MaxUtilizationLevel maxUtilizationLevel) {
        return ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setMaxUtilizationLevel(maxUtilizationLevel)
            .build()).build();
    }

    private void updateApiDTOForAlleviatePressurePlan(List<PlanScopeEntry> scopeEntriesList,
                    ScenarioApiDTO dto) {
        // pressure plan will have two clusters in scope.
        BaseApiDTO srcCluster =  convertPlanScopeEntryToBaseApiDTO(scopeEntriesList.get(0));
        BaseApiDTO destinationCluster =  convertPlanScopeEntryToBaseApiDTO(scopeEntriesList.get(1));
        RelievePressureObjectApiDTO alleviatePressureDTO = new RelievePressureObjectApiDTO();
        alleviatePressureDTO.setSources(Collections.singletonList(srcCluster));
        alleviatePressureDTO.setDestinations(Collections.singletonList(destinationCluster));
        dto.getTopologyChanges().setRelievePressureList(ImmutableList.of(alleviatePressureDTO));
    }

    private BaseApiDTO convertPlanScopeEntryToBaseApiDTO(PlanScopeEntry scopeEntry) {
        BaseApiDTO baseApi = new BaseApiDTO();
        baseApi.setUuid(String.valueOf(scopeEntry.getScopeObjectOid()));
        baseApi.setDisplayName(scopeEntry.getDisplayName());
        baseApi.setClassName(scopeEntry.getClassName());
        return baseApi;
    }

    @Nonnull
    private static LoadChangesApiDTO buildLoadChangesApiDTO(@Nonnull List<ScenarioChange> changes,
                                                            @Nonnull ScenarioChangeMappingContext context) {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        if (CollectionUtils.isEmpty(changes)) {
            return loadChanges;
        }
        final Stream<UtilizationLevel> utilizationLevels = changes.stream()
                .filter(ScenarioMapper::scenarioChangeHasUtilizationLevel)
                .map(ScenarioChange::getPlanChanges)
                .map(PlanChanges::getUtilizationLevel);

        final List<UtilizationApiDTO> utilizationApiDTOS = utilizationLevels
                .map(utilLevel -> createUtilizationApiDto(utilLevel, context)).collect(Collectors.toList());
        loadChanges.setUtilizationList(utilizationApiDTOS);

        // convert max utilization changes too
        loadChanges.setMaxUtilizationList(getMaxUtilizationApiDTOs(changes, context));

        // Set historical baseline date from scenario
        changes.stream()
            .filter(change -> change.hasPlanChanges() &&
                change.getPlanChanges().hasHistoricalBaseline())
            .findFirst()
            .ifPresent(c -> loadChanges.setBaselineDate(DateTimeUtil.toString(c.getPlanChanges()
                .getHistoricalBaseline().getBaselineDate())));
        return loadChanges;
    }

    @VisibleForTesting
    @Nonnull
    static List<MaxUtilizationApiDTO> getMaxUtilizationApiDTOs(List<ScenarioChange> changes,
                                                                       ScenarioChangeMappingContext context) {
        return changes.stream()
                .filter(change -> change.getPlanChanges().hasMaxUtilizationLevel())
                .map(ScenarioChange::getPlanChanges)
                .map(PlanChanges::getMaxUtilizationLevel)
                .map(maxUtilizationLevel -> {
                    MaxUtilizationApiDTO maxUtilization = new MaxUtilizationApiDTO();
                    // TODO: how to handle the projection day? Seems to be always set to 0 in the UI.
                    // Leaving it unset for now, since it's not in the source object, and we aren't
                    // handling these anyways.
                    maxUtilization.setMaxPercentage(maxUtilizationLevel.getPercentage());
                    if (maxUtilizationLevel.hasGroupOid()) {
                        maxUtilization.setTarget(context.dtoForId(maxUtilizationLevel.getGroupOid()));
                    }

                    if (maxUtilizationLevel.hasSelectedEntityType()) {
                        maxUtilization.setSelectedEntityType(
                                ApiEntityType.fromSdkTypeToEntityTypeString(maxUtilizationLevel.getSelectedEntityType()));
                    }

                    return maxUtilization;
                })
                .collect(Collectors.toList());
    }

    /**
     * Create a {@link UtilizationApiDTO} object representing the utilization level change passed in.
     *
     * <p>Utilization level changes are only applied globally, to all VM's in the plan scope. So we
     * set a hardcoded "Virtual Machines" target on the resulting object. This corresponds with the
     * behavior in the UI, which expects this hardcoded default target</p>
     *
     * @param utilizationLevel The UtilizationLevel containing the change amount
     * @param mappingContext Mapping context object
     * @return a {@link UtilizationApiDTO} representing the same change, with hardcoded VM's target.
     */
    @Nonnull
    private static UtilizationApiDTO createUtilizationApiDto(@Nonnull UtilizationLevel utilizationLevel, ScenarioChangeMappingContext mappingContext) {
        final UtilizationApiDTO utilizationDTO = new UtilizationApiDTO();
        utilizationDTO.setPercentage(utilizationLevel.getPercentage());
        if (utilizationLevel.hasGroupOid()) {
            utilizationDTO.setTarget(mappingContext.dtoForId(utilizationLevel.getGroupOid()));
        }

        return utilizationDTO;
    }

    private static boolean scenarioChangeHasUtilizationLevel(@Nonnull ScenarioChange change) {
        return change.hasPlanChanges() && change.getPlanChanges().hasUtilizationLevel();
    }

    @Nonnull
    @VisibleForTesting
    List<ScenarioChange> buildSettingChanges(@Nullable final List<SettingApiDTO<String>> settingsList) throws
            OperationFailedException {
        if (CollectionUtils.isEmpty(settingsList)) {
            return Collections.singletonList(getRateOfResizeScenarioChange());
        }
        // First we convert them back to "real" settings.
        final List<SettingApiDTO> convertedSettingOverrides =
            settingsManagerMapping.convertFromPlanSetting(settingsList);
        final Map<SettingValueEntityTypeKey, Setting> settingProtoOverrides =
                settingsMapper.toProtoSettings(convertedSettingOverrides);

        final ImmutableList.Builder<ScenarioChange> retChanges = ImmutableList.builder();

        for (SettingApiDTO<String> apiDto : convertedSettingOverrides) {
            Setting protoSetting = settingProtoOverrides.get(SettingsMapper.getSettingValueEntityTypeKey(apiDto));
            if (protoSetting == null) {
                String dtoDescription;
                try {
                    dtoDescription = OBJECT_WRITER.writeValueAsString(apiDto);
                } catch (JsonProcessingException e) {
                    dtoDescription = apiDto.getUuid();
                }
                logger.warn("Unable to map scenario change for setting: {}", dtoDescription);
            } else {
                final SettingOverride.Builder settingOverride = SettingOverride.newBuilder()
                    .setSetting(protoSetting);
                if (apiDto.getEntityType() != null) {
                    settingOverride.setEntityType(
                            ApiEntityType.fromStringToSdkType(apiDto.getEntityType()));
                }

                if (apiDto.getSourceGroupUuid() != null) {
                    settingOverride.setGroupOid(uuidMapper.fromUuid(apiDto.getSourceGroupUuid()).oid());
                }

                retChanges.add(ScenarioChange.newBuilder()
                        .setSettingOverride(settingOverride)
                        .build());
            }
        }

        // The rate of resize is temporarily set to 3. We should get it from settingsList.
        retChanges.add(getRateOfResizeScenarioChange());

        return retChanges.build();
    }

    /**
     * In migrate to cloud plans, we need to override and cloud scaling action modes to manual,
     * so that eg disabled cloud VM scaling doesn't prevent migration actions.
     *
     * @return a list of settings override changes for cloud scale action modes.
     */
    private List<ScenarioChange> getMigrationActionModeScenarioChanges() {
        return Arrays.asList(
            ScenarioChange.newBuilder().setSettingOverride(SettingOverride.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.CloudComputeScale.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))))
                .build(),
            ScenarioChange.newBuilder().setSettingOverride(SettingOverride.newBuilder()
                .setEntityType(ApiEntityType.DATABASE.typeNumber())
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.CloudDBScale.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))))
                .build(),
            ScenarioChange.newBuilder().setSettingOverride(SettingOverride.newBuilder()
                .setEntityType(ApiEntityType.DATABASE_SERVER.typeNumber())
                .setSetting(Setting.newBuilder()
                    .setSettingSpecName(ConfigurableActionSettings.CloudDBServerScale.getSettingName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()))))
                .build()
        );
    }

    /**
     * TODO: Get rate of resize setting from settingsList.
     * The rate of resize is temporarily set to 3.
     *
     * @return rate of resize scenario change
     */
    private ScenarioChange getRateOfResizeScenarioChange() {
        return ScenarioChange.newBuilder()
            .setSettingOverride(SettingOverride.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setSetting(Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.RateOfResize.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(PLAN_RATE_OF_RESIZE))))
            .build();
    }

    /**
     * Maps list of {@link ConfigChangesApiDTO} to list of {@link ScenarioChange}.
     *
     * @param configChanges the changes to map to list of scenarioChange
     * @return list of {@link ScenarioChange} built from configChanges
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *                                  to an {@link ApiId} fails.
     * @throws IllegalArgumentException when constraint is of an unsupported configuration
     */
    @Nonnull
    private List<ScenarioChange> getConfigChanges(@Nullable final ConfigChangesApiDTO configChanges)
            throws OperationFailedException, IllegalArgumentException {
        if (configChanges == null) {
            return Collections.emptyList();
        }

        final ImmutableList.Builder<ScenarioChange> scenarioChanges = ImmutableList.builder();

        scenarioChanges.addAll(buildSettingChanges(configChanges.getAutomationSettingList()));

        if (!CollectionUtils.isEmpty(configChanges.getRemoveConstraintList())) {
            scenarioChanges.add(buildPlanChanges(configChanges));
        }
        if (configChanges.getRiSettingList() != null && !configChanges.getRiSettingList().isEmpty()) {
            @Nullable ScenarioChange riSetting = buildRISettingChanges(configChanges.getRiSettingList());
            if (riSetting != null) {
                scenarioChanges.add(riSetting);
            }
        }

        // TODO: Other Plan Changes are incorporated in buildPlanChanges
        // based on whether there are constraints to remove.  Possibly Combine logic.
        final IncludedCouponsApiDTO includedCoupons = configChanges.getIncludedCoupons();
        if (includedCoupons != null) {
            final List<Long> includedCouponOidsList = includedCoupons.getIncludedCouponOidsList();
            // The UI send a null OIDs list if the user hasn't made any selections from the
            // RI Inventory to differentiate from an empty set of OIDS to represent not
            // including any RIs.
            if (includedCouponOidsList != null) {
                final PlanChanges.Builder planChangesBuilder = PlanChanges.newBuilder();
                planChangesBuilder.setIncludedCoupons(IncludedCoupons.newBuilder()
                                         .addAllIncludedCouponOids(includedCouponOidsList)
                                         .setIsWhiteList(includedCoupons.isIswhiteList())
                                         .build());
                scenarioChanges.add(ScenarioChange.newBuilder()
                                    .setPlanChanges(planChangesBuilder.build()).build());
            }
        }

        final BusinessUnitApiDTO businessUnitApiDTO = configChanges.getSubscription();
        // Make sure all the fields being set are present.
        if (businessUnitApiDTO != null) {
            final PlanChanges.Builder planChangesBuilder = PlanChanges.newBuilder();
            BusinessAccount.Builder subscription = BusinessAccount.newBuilder();
            final String uuid = businessUnitApiDTO.getUuid();
            if (!StringUtils.isEmpty(uuid)) {
                subscription.setUuid(uuid);
            }
            final String accountId = businessUnitApiDTO.getAccountId();
            if (!StringUtils.isEmpty(accountId)) {
                subscription.setAccountId(accountId);
            }
            final String displayName = businessUnitApiDTO.getDisplayName();
            if (!StringUtils.isEmpty(displayName)) {
                subscription.setDisplayName(displayName);
            }
            final String className = businessUnitApiDTO.getClassName();
            if (!StringUtils.isEmpty(className)) {
                subscription.setClassName(className);
            }
            final com.vmturbo.api.enums.CloudType cloudType = businessUnitApiDTO.getCloudType();
            if (cloudType != null) {
                subscription.setCloudType(cloudType.name());
            }
            planChangesBuilder.setSubscription(subscription.build());
            scenarioChanges.add(ScenarioChange.newBuilder()
                            .setPlanChanges(planChangesBuilder.build()).build());
        }

        // For a Migration plan, the business account in configChanges pertains to the destination
        // account and/or rate card/discount account.  For another plan type it may have a
        // different use case in the future.  If a business account is present, process it along
        // with the TopologyMigration PlanChanges for an MPC plan, and in the appropriate PlanChanges
        // type for other plans.

        return scenarioChanges.build();
    }

    /**
     * Build RI setting scenario change
     *
     * @param riSettingList a list of ri settings
     * @return a ScenarioChange
     */
    @Nullable
    ScenarioChange buildRISettingChanges(List<SettingApiDTO> riSettingList) {
        final ImmutableMap<String, SettingApiDTO> settings = Maps.uniqueIndex(riSettingList, SettingApiDTO::getUuid);

        final Boolean isRIBuyEnabled = SettingsMapper.inputValueToString(settings.get(RIPurchase.getSettingName()))
                .map(BooleanUtils::toBoolean)
                .orElse(true);
        if (!isRIBuyEnabled) {
            // only run optimize workload, no need to run buy RI
            return null;
        }

        RIProviderSetting.Builder awsRISetting = RIProviderSetting.newBuilder();
        RIProviderSetting.Builder azureRISetting = RIProviderSetting.newBuilder();
        RISetting.Builder riSetting = RISetting.newBuilder();

        SUPPORTED_RI_PROFILE_SETTINGS.forEach(setting -> {
            SettingsMapper.inputValueToString(settings.get(setting.getSettingName())).ifPresent(settingValue -> {
                switch (setting) {
                    case RIDemandType:
                        riDemandTypeMapper.valueOf(settingValue)
                                .ifPresent(riSetting::setDemandType);
                        break;
                    case AWSPreferredOfferingClass:
                        riOfferingClassMapper.valueOf(settingValue)
                                .ifPresent(awsRISetting::setPreferredOfferingClass);
                        break;
                    case AWSPreferredPaymentOption:
                        riPaymentOptionMapper.valueOf(settingValue)
                                .ifPresent(awsRISetting::setPreferredPaymentOption);
                        break;
                    case AWSPreferredTerm:
                        riTermMapper.valueOf(settingValue).map(PreferredTerm::getYears)
                                .ifPresent(awsRISetting::setPreferredTerm);
                        break;
                    case AzurePreferredTerm:
                        riTermMapper.valueOf(settingValue).map(PreferredTerm::getYears)
                                .ifPresent(azureRISetting::setPreferredTerm);
                        break;
                    case CloudCommitmentHistoricalLookbackPeriod:
                        riSetting.setLookBackDurationDays(Integer.parseInt(settingValue));
                        break;
                    case CloudCommitmentIncludeTerminatedEntities:
                        riSetting.setIncludeTerminatedEntityDemand(Boolean.parseBoolean(settingValue));
                        break;
                }
            });
        });


        if (StringUtils.isNotEmpty(awsRISetting.toString())) {
            riSetting.putRiSettingByCloudtype(CloudType.AWS.name(), awsRISetting.build());
        }
        if (StringUtils.isNotEmpty(azureRISetting.toString())) {
            riSetting.putRiSettingByCloudtype(CloudType.AZURE.name(), azureRISetting.build());
        }
        return ScenarioChange.newBuilder().setRiSetting(riSetting).build();
    }


    /**
     * Maps {@link ConfigChangesApiDTO} to {@link ScenarioChange}.
     *
     * @param configChanges the changes to map to scenarioChange
     * @return the {@link ScenarioChange} built from configChanges
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *                                  to an {@link ApiId} fails.
     * @throws IllegalArgumentException when constraint is of an unsupported configuration
     */
    @Nonnull
    private ScenarioChange buildPlanChanges(@Nonnull ConfigChangesApiDTO configChanges)
            throws OperationFailedException, IllegalArgumentException {
        final PlanChanges.Builder planChangesBuilder = PlanChanges.newBuilder();
        final List<RemoveConstraintApiDTO> constraintsToRemove = configChanges.getRemoveConstraintList();
        if (!CollectionUtils.isEmpty(constraintsToRemove)) {
            planChangesBuilder.addAllIgnoreConstraints(getIgnoreConstraintsPlanSetting(constraintsToRemove));
        }

        return ScenarioChange.newBuilder().setPlanChanges(planChangesBuilder.build()).build();
    }

    /**
     * Maps list of {@link RemoveConstraintApiDTO} to {@link IgnoreConstraint}.
     *
     * @param constraints list of constraints to map
     * @return {@link IgnoreConstraint} mapped from constraints
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *                                  to an {@link ApiId} fails.
     * @throws IllegalArgumentException when constraint is of an unsupported configuration
     */
    @Nonnull
    private List<IgnoreConstraint> getIgnoreConstraintsPlanSetting(
            @Nonnull List<RemoveConstraintApiDTO> constraints)
            throws OperationFailedException, IllegalArgumentException {
        final ImmutableList.Builder<IgnoreConstraint> ignoreConstraintsBuilder = ImmutableList.builder();
        for (RemoveConstraintApiDTO constraint : constraints) {
            final IgnoreConstraint ignoreConstraint = toIgnoreConstraint(constraint);
            ignoreConstraintsBuilder.add(ignoreConstraint);
        }
        return ignoreConstraintsBuilder.build();
    }

    /**
     * Checks if {@link RemoveConstraintApiDTO} should ignore constraints of all entities.
     *
     * @param constraint the constraint to check
     * @return true if all entity constraints should be ignored
     */
    private static boolean isIgnoreAllEntities(RemoveConstraintApiDTO constraint) {
        return ConstraintType.GlobalIgnoreConstraint == constraint.getConstraintType() &&
                constraint.getTarget() == null &&
                constraint.getTargetEntityType() == null;
    }

    /**
     * Checks if {@link RemoveConstraintApiDTO} should ignore constraints of particular entity.
     *
     * @param constraint the constraint to check
     * @return true if constraints should be ignored for specific entityType
     */
    private static boolean isIgnoreAllConstraintsForEntityType(RemoveConstraintApiDTO constraint) {
        return ConstraintType.GlobalIgnoreConstraint == constraint.getConstraintType() &&
                constraint.getTarget() == null &&
                constraint.getTargetEntityType() != null;
    }

    /**
     * Checks if {@link RemoveConstraintApiDTO} should ignore constraints on group of entities.
     *
     * @param constraint the constraint to check
     * @return true if constraints should be ignored specific group
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *                                  to an {@link ApiId} fails.
     */
    private boolean isIgnoreConstraintForGroup(@Nonnull final RemoveConstraintApiDTO constraint)
            throws OperationFailedException {
        return constraint.getTarget() != null &&
                constraint.getTarget().getUuid() != null &&
                uuidMapper.fromUuid(constraint.getTarget().getUuid()).isGroup();
    }

    /**
     * Maps {@link RemoveConstraintApiDTO} to  {@link IgnoreConstraint}.
     *
     * @param constraint the dto to map
     * @return IgnoreConstraint mapped from constraint
     * @throws OperationFailedException If one of the underlying operations required to map the UUID
     *                                  to an {@link ApiId} fails.
     * @throws IllegalArgumentException when constraint is of an unsupported configuration
     */
    @Nonnull
    IgnoreConstraint toIgnoreConstraint(@Nonnull RemoveConstraintApiDTO constraint) throws OperationFailedException, IllegalArgumentException {
        final IgnoreConstraint.Builder ignoreConstraint = IgnoreConstraint.newBuilder();
        if (isIgnoreAllEntities(constraint)) {
            ignoreConstraint.setIgnoreAllEntities(true);
        } else if (isIgnoreAllConstraintsForEntityType(constraint)) {
            final String uiEntityType = constraint.getTargetEntityType();
            final EntityType targetEntityType = ApiEntityType.fromString(uiEntityType).sdkType();
            final GlobalIgnoreEntityType globalIgnoreEntityType = GlobalIgnoreEntityType.newBuilder()
                    .setEntityType(targetEntityType)
                    .build();
            ignoreConstraint.setGlobalIgnoreEntityType(globalIgnoreEntityType);
        } else if (isIgnoreConstraintForGroup(constraint)) {
            final ApiId apiId = uuidMapper.fromUuid(constraint.getTarget().getUuid());
            final ConstraintType constraintType = constraint.getConstraintType() == null ? ConstraintType.GlobalIgnoreConstraint :
                    constraint.getConstraintType();
            ConstraintGroup constraintGroup = ConstraintGroup.newBuilder()
                    .setGroupUuid(apiId.oid())
                    .setCommodityType(constraintType.name())
            .build();
            ignoreConstraint.setIgnoreGroup(constraintGroup);
        } else {
            throw new IllegalArgumentException("RemoveConstraintApiDTO configuration not supported");
        }
        return ignoreConstraint.build();
    }

    /**
     * Check if storage suspension and host provision exist in the ScenarioApiDTO. If not, creating a
     * default setting for storage suspension disabled in all plans and a default setting for host
     * provision disabled in decommission host plans.
     *
     * @param dto The ScenarioApiDTO given by UI
     * @return a list of ScenarioChanges created to represent the default automation settings
     */

    @Nonnull
    private List<ScenarioChange> checkAndCreateDefaultAutomationSettingsForPlan(@Nonnull final ScenarioApiDTO dto) {
        boolean hasPMProvisionSetting = false;
        final ImmutableList.Builder<ScenarioChange> changes = ImmutableList.builder();
        if (dto.getConfigChanges() != null && dto.getConfigChanges().getAutomationSettingList() != null) {
            List<SettingApiDTO<String>> automationSettingList = dto.getConfigChanges().getAutomationSettingList();
            for(SettingApiDTO setting : automationSettingList) {
                if ( setting.getUuid() != null  ) {
                    final Optional<SettingSpec> settingSpec =  SettingsMapper.getSettingSpec(setting.getUuid());
                    if (settingSpec.isPresent()) {
                        SettingsMapper.validateSettingValue(setting.getValue().toString(), settingSpec.get());
                    }

                    if ( setting.getEntityType() != null
                            && EntityType.PHYSICAL_MACHINE_VALUE == ApiEntityType.fromString(setting.getEntityType()).typeNumber()
                            && setting.getUuid().equalsIgnoreCase(ConfigurableActionSettings.Provision.getSettingName())) {
                        hasPMProvisionSetting = true;
                    }
                }
            }
        }
        // DECOMMISSION HOST plan should have host provision disabled as default
        if (!hasPMProvisionSetting && dto.getType() != null && dto.getType().equalsIgnoreCase(DECOMMISSION_HOST_SCENARIO_TYPE)) {
            final SettingOverride.Builder settingOverride = SettingOverride.newBuilder()
                    .setSetting(Setting.newBuilder()
                            .setSettingSpecName(ConfigurableActionSettings.Provision.getSettingName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED)))
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);
            changes.add(ScenarioChange.newBuilder().setSettingOverride(settingOverride).build());
        }
        return changes.build();
    }

    @Nonnull
    private List<ScenarioChange> getTopologyChanges(@Nonnull final TopologyChangesApiDTO topoChanges,
                                                    @Nullable final ConfigChangesApiDTO configChanges,
                                                    @Nonnull final Set<Long> templateIds,
                                                    PlanScope planScope)
            throws IllegalArgumentException, UnknownObjectException {
        if (topoChanges == null) {
            return Collections.emptyList();
        }

        final ImmutableList.Builder<ScenarioChange> changes = ImmutableList.builder();

        CollectionUtils.emptyIfNull(topoChanges.getAddList())
            .forEach(change -> changes.addAll(mapTopologyAddition(change, templateIds)));

        CollectionUtils.emptyIfNull(topoChanges.getRemoveList())
            .forEach(change -> changes.add(mapTopologyRemoval(change)));

        CollectionUtils.emptyIfNull(topoChanges.getReplaceList())
            .forEach(change -> changes.add(mapTopologyReplace(change)));

        List<MigrateObjectApiDTO> migrateObjects = topoChanges.getMigrateList();
        if (CollectionUtils.isNotEmpty(migrateObjects)) {
            // 1. Create the migration change
            changes.add(getTopologyMigrationChange(migrateObjects, configChanges));
            // 2. Create the RI Buy configuration if none exist in configuration
            if (configChanges == null || CollectionUtils.isEmpty(configChanges.getRiSettingList())) {
                changes.add(getRiSettings(planScope));
            }
            // 3. Override action mode settings, we always want manual actions in the plan
            changes.addAll(getMigrationActionModeScenarioChanges());
        }

        return changes.build();
    }

    /**
     * Right now, at API side, there is no field to tell uuid is entity id or template id. This function
     * will be used to get all involved template ids.
     *
     * @param changes Topology changes in the scenario.
     * @return all involved template ids.
     */
    private Set<Long> getTemplatesIds(final TopologyChangesApiDTO changes) {

        if (changes == null || changes.getAddList() == null) {
            return Collections.emptySet();
        }
        // only need to check Addition id, because for Replace Id, it already has template field.
        final Set<Long> additionIds = changes.getAddList().stream()
            .map(AddObjectApiDTO::getTarget)
            .map(BaseApiDTO::getUuid)
            .map(Long::valueOf)
            .collect(Collectors.toSet());

        // Send rpc request to template service and return all template ids.
        final Set<Long> templateIds = templatesUtils.getTemplatesByIds(additionIds).stream()
            .map(Template::getId)
            .collect(Collectors.toSet());
        return templateIds;
    }

    private static int projectionDay(@Nonnull final Integer projDay) {
        return projDay == null ? 0 : projDay;
    }

    private static Collection<Integer> projectionDays(@Nonnull final List<Integer> projDays) {
        return projDays == null ? Collections.emptyList() : projDays;
    }

    @VisibleForTesting
    List<ScenarioChange> mapTopologyAddition(@Nonnull final AddObjectApiDTO change,
                                            @Nonnull final Set<Long> templateIds) {

        Preconditions.checkArgument(change.getTarget() != null,
                "Topology additions must contain a target");
        // Default count to 1
        int count = change.getCount() != null ? change.getCount() : 1;

        final long uuid = Long.parseLong(change.getTarget().getUuid());
        final List<ScenarioChange> changes = new ArrayList<>();

        final TopologyAddition.Builder additionBuilder = TopologyAddition.newBuilder()
            .addAllChangeApplicationDays(projectionDays(change.getProjectionDays()))
            .setAdditionCount(count);

        if (change.getTargetEntityType() != null) {
            additionBuilder.setTargetEntityType(
                    ApiEntityType.fromStringToSdkType(change.getTargetEntityType()));
        }

        if (templateIds.contains(uuid)) {
            additionBuilder.setTemplateId(uuid);
        } else {
            final GetGroupResponse groupResponse = groupRpcService.getGroup(GroupID.newBuilder()
                    .setId(uuid)
                    .build());
            if (groupResponse.hasGroup()) {
                additionBuilder.setGroupId(uuid);
            } else {
                additionBuilder.setEntityId(uuid);
            }
        }
        changes.add(ScenarioChange.newBuilder()
            .setTopologyAddition(additionBuilder)
            .build());
        return changes;
    }

    /**
     * Create a {@link ScenarioChange} representing an {@link RISetting} loaded with the current real-time RI purchase
     * settings. This is required in the MCP context for initiating and providing setting values to the RI buy algorithm.
     *
     * @param planScope The scope of the plan
     * @return a {@link ScenarioChange} representing the current real-time RI purchase settings
     * @throws UnknownObjectException if an invalid setting manager UUID is provided
     */
    public ScenarioChange getRiSettings(PlanScope planScope) throws UnknownObjectException {
        // Retrieve the provider for the plan scope
        Optional<CloudType> provider = getProvider(planScope);

        List<SettingApiDTO> settingApiDTOs =
                this.settingsService.getSettingsByUuid("reservedinstancemanager").stream()
                        .map(d -> (SettingApiDTO)d)
                        .filter(d -> !provider.isPresent() || (d.getCategories() != null && d.getCategories().contains(provider.get().name().toLowerCase())))
                        .collect(Collectors.toList());
        return buildRISettingChanges(settingApiDTOs);
    }

    /**
     * Returns the CSP provider name for the specified regionId.
     *
     * @param planScope The scope of the plan
     * @return          The provider type
     */
    private Optional<CloudType> getProvider(PlanScope planScope) {
        try {
            List<PlanScopeEntry> scopeEntries = planScope.getScopeEntriesList();
            if (!scopeEntries.isEmpty()) {
                // Retrieve the SE list for the region (should only be one)
                List<ServiceEntityApiDTO> seList = this.repositoryApi.getRegion(Arrays.asList(scopeEntries.get(0).getScopeObjectOid())).getSEList();
                if (!seList.isEmpty()) {
                    // Retrieve the target
                    TargetApiDTO target = seList.get(0).getDiscoveredBy();
                    if (target != null && target.getType() != null) {
                        return CloudType.fromProbeType(target.getType());
                    }
                }
            }
        } catch (InterruptedException | ConversionException e) {
            logger.error("An error occurred trying to find the provider type for scope: {}", planScope, e);
        }
        return Optional.empty();
    }

    /**
     * Convert a {@link BaseApiDTO} to a {@link MigrationReference}.
     *
     * @param baseApiDTO either a source or destination of a {@link MigrateObjectApiDTO}
     * @return a {@link MigrationReference} object converted from the {@param baseApiDTO}
     * @throws IllegalArgumentException when the {@param baseApiDTO} className cannot be converted
     */
    private static MigrationReference getMigrationReferenceFromBaseApiDTO(BaseApiDTO baseApiDTO)
            throws IllegalArgumentException {
        long oid = Long.valueOf(baseApiDTO.getUuid());
        MigrationReference.Builder referenceBuilder = MigrationReference.newBuilder()
                .setOid(oid);
        String className = baseApiDTO.getClassName();
        EntityType entityType = ApiEntityType.fromString(className).sdkType();
        if (!entityType.equals(EntityType.UNKNOWN)) {
            referenceBuilder.setEntityType(entityType.getNumber());
        } else {
            final GroupType groupType = GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.get(className);
            if (groupType == null) {
                throw new IllegalArgumentException(
                        String.format("BaseApiDTO with OID: %s has className: %s which is not a valid entityType or groupType. Failed to convert to MigrationReference",
                                oid, className));
            }
            referenceBuilder.setGroupType(groupType.getNumber());
        }
        return referenceBuilder.build();
    }

    /**
     * Create the {@link TopologyMigration} corresponding to a particular {@link MigrateObjectApiDTO}.
     *
     * @param migrateObjects the list of {@link MigrateObjectApiDTO} object from which the scenario is being created
     * @param configChanges the list of {@link ConfigChangesApiDTO} from which to extract OS
     *                      migration configuration
     * @return a {@link ScenarioChange} symbolizing the cloud migration
     * @throws IllegalArgumentException when either a source or destination className cannot be converted
     */
    public static ScenarioChange getTopologyMigrationChange(
        @Nonnull final List<MigrateObjectApiDTO> migrateObjects,
        @Nullable final ConfigChangesApiDTO configChanges) throws IllegalArgumentException {
            Set<MigrationReference> sources = Sets.newHashSet();
            Set<MigrationReference> destinations = Sets.newHashSet();
            migrateObjects.forEach(migrateObject -> {
                if (CollectionUtils.isNotEmpty(migrateObject.getSources())
                    && CollectionUtils.isNotEmpty(migrateObject.getDestinations())) {
                    sources.addAll(migrateObject.getSources().stream()
                            .collect(Collectors.mapping(
                                    ScenarioMapper::getMigrationReferenceFromBaseApiDTO, Collectors.toSet())));
                    destinations.addAll(migrateObject.getDestinations().stream()
                            .collect(Collectors.mapping(
                                    ScenarioMapper::getMigrationReferenceFromBaseApiDTO, Collectors.toSet())));

                } else {
                    // Using deprecated source and destination fields for backward compatibility.
                    // This logic should be removed when deprecated fields are finally deleted.
                    sources.add(getMigrationReferenceFromBaseApiDTO(migrateObject.getSource()));
                    destinations.add(getMigrationReferenceFromBaseApiDTO(migrateObject.getDestination()));
                }
            });


        MigrateObjectApiDTO firstMigrateObject = migrateObjects.get(0);
            final TopologyMigration.Builder migrationBuilder = TopologyMigration.newBuilder()
                .addAllSource(sources)
                .addAllDestination(destinations)
                .setDestinationEntityType(firstMigrateObject.getDestinationEntityType() == DestinationEntityType.VirtualMachine
                        ? TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE
                        : TopologyMigration.DestinationEntityType.DATABASE_SERVER)
                .setRemoveNonMigratingWorkloads(firstMigrateObject.getRemoveNonMigratingWorkloads())
                .addAllOsMigrations(buildOsMigrations(configChanges));
        if (configChanges != null) {
            final BusinessUnitApiDTO destinationAccountDto = configChanges.getSubscription();
            if (destinationAccountDto != null) {
                final MigrationReference destinationAccount = MigrationReference.newBuilder()
                                .setOid(Long.valueOf(destinationAccountDto.getUuid()))
                                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                .build();
                migrationBuilder.setDestinationAccount(destinationAccount);
            }
        }

        return ScenarioChange.newBuilder()
                        .setTopologyMigration(migrationBuilder)
                        .build();
    }

    @VisibleForTesting
    ScenarioChange mapTopologyRemoval(@Nonnull final RemoveObjectApiDTO change) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology removals must contain a target");

        final long uuid = Long.parseLong(change.getTarget().getUuid());
        final GetGroupResponse groupResponse = groupRpcService.getGroup(GroupID.newBuilder()
                .setId(uuid)
                .build());
        final TopologyRemoval.Builder removalBuilder =
            TopologyRemoval.newBuilder()
                    .setChangeApplicationDay(projectionDay(change.getProjectionDay()));

        if (change.getTargetEntityType() != null) {
            removalBuilder.setTargetEntityType(ApiEntityType.fromString(change.getTargetEntityType()).typeNumber());
        }

        if (groupResponse.hasGroup()) {
            removalBuilder.setGroupId(uuid);
        } else {
            removalBuilder.setEntityId(uuid);
        }
        return ScenarioChange.newBuilder()
                    .setTopologyRemoval(removalBuilder.build())
                    .build();
    }

    @VisibleForTesting
    ScenarioChange mapTopologyReplace(@Nonnull final ReplaceObjectApiDTO change) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology replace must contain a target");
        Preconditions.checkArgument(change.getTemplate() != null,
                "Topology replace must contain a template");

        final long uuid = Long.parseLong(change.getTarget().getUuid());
        final GetGroupResponse groupResponse = groupRpcService.getGroup(GroupID.newBuilder()
                .setId(uuid)
                .build());
        final TopologyReplace.Builder replaceBuilder =
            TopologyReplace.newBuilder()
                .setChangeApplicationDay(projectionDay(change.getProjectionDay()))
                .setAddTemplateId(Long.parseLong(change.getTemplate().getUuid()));

        if (change.getTargetEntityType() != null) {
            replaceBuilder.setTargetEntityType(
                    ApiEntityType.fromStringToSdkType(change.getTargetEntityType()));
        }

        if (groupResponse.hasGroup()) {
            replaceBuilder.setRemoveGroupId(uuid);
        } else {
            replaceBuilder.setRemoveEntityId(uuid);
        }
        return ScenarioChange.newBuilder()
                    .setTopologyReplace(replaceBuilder.build())
                    .build();
    }

    /**
     * If there are any scope entries in the list of scopeDTO's, create a PlanScope object that
     * represents the scope contents in XL DTO schema objects.
     * Otherwise, throw an exception.
     * @param scopeDTOs the list of scope DTO objects.
     * @return the equivalent PlanScope, if any scope DTO's were found.
     * @throws IllegalArgumentException if any of the scope DTOs in the list does not have a uuid,
     *                                  or if the class name provided in a scopeDTO does not match
     *                                  the class name internally associated with the DTO's uuid.
     *                                  Also, if the scope is empty or it is the whole Market; we
     *                                  disallow such usage.
     * @throws OperationFailedException if looking up the uuid from a scopeDTO fails.
     */
    private PlanScope getScope(@Nullable final List<BaseApiDTO> scopeDTOs)
            throws IllegalArgumentException, OperationFailedException {
        // convert scope info from BaseApiDTO's to PlanScopeEntry objects
        if (scopeDTOs == null || scopeDTOs.size() == 0) {
            throw new IllegalArgumentException(
                    "No scope specified. Please specify a group or an entity as a scope.");
        }

        ApiId resolvedScope;
        PlanScope.Builder scopeBuilder = PlanScope.newBuilder();
        // add all of the scope entries to the builder
        for (BaseApiDTO scopeDTO : scopeDTOs) {
            if (scopeDTO.getUuid() == null) {
                throw new IllegalArgumentException("Found scopes with invalid uuid: "
                        + scopeDTO.getDisplayName());
            }
            resolvedScope = uuidMapper.fromUuid(scopeDTO.getUuid());
            // Validate class name only, no need for display name (we'll use what we get from the
            // apiId)
            // - If it is empty, then we'll just use apiId's class name.
            // - If it is populated, check that it matches the apiId's class name.
            if (scopeDTO.getClassName() != null
                    && !scopeDTO.getClassName().equalsIgnoreCase(resolvedScope.getClassName())) {
                throw new IllegalArgumentException(FormattedString.format(
                    "Incorrect class name {} (expected: {}) for scope with uuid {}",
                    resolvedScope.getClassName(), scopeDTO.getClassName(),
                        resolvedScope.uuid()));
            }
            // Since all scope entries are additive, if any scope is the Market scope, then this is
            // effectively the same as an unscoped plan (which implies the whole Market), so throw
            // an exception since we disallow using the whole Market as a scope. In the future,
            // if we support scope reduction entries, this may change.
            if (resolvedScope.isRealtimeMarket()) {
                throw new IllegalArgumentException("Cannot use the whole market as a scope. Please specify a group or an entity as a scope.");
            }

            scopeBuilder.addScopeEntriesBuilder()
                    .setScopeObjectOid(resolvedScope.oid())
                    .setClassName(resolvedScope.getClassName())
                    .setDisplayName(resolvedScope.getDisplayName());
        }
        // we have a customized scope -- return it
        return scopeBuilder.build();
    }

    private List<BaseApiDTO> buildApiScopeObjects(@Nonnull final Scenario scenario) {
        // if there are any scope elements defined on this plan, return them as a list of group
        // references
        if (scenario.hasScenarioInfo()) {
            ScenarioInfo info = scenario.getScenarioInfo();
            if (info.hasScope()) {
                PlanScope planScope = info.getScope();
                if (planScope.getScopeEntriesCount() > 0) {
                    // return the list of scope objects
                    return planScope.getScopeEntriesList().stream()
                            .map(scopeEntry -> {
                                BaseApiDTO scopeDTO = new BaseApiDTO();
                                scopeDTO.setUuid(Long.toString(scopeEntry.getScopeObjectOid()));
                                scopeDTO.setClassName(scopeEntry.getClassName());
                                scopeDTO.setDisplayName(scopeEntry.getDisplayName());
                                return scopeDTO;
                            })
                            .collect(Collectors.toList());
                }
            }
        }
        // no scope to read -- return a default global scope for the UI to use.
        return Collections.singletonList(MARKET_PLAN_SCOPE);
    }

    private List<Integer> buildApiProjChanges() {
        // --- START HAX --- Tracking Issue: OM-14951
        // TODO (roman, Jan 20 2017): We need to extend the Plan Orchestrator with support
        // for projection period changes, and do the appropriate conversion.
        // As part of the effort to get the plan UI functional, hard-coding the default
        // projection changes used by the UI here.
        return Collections.singletonList(0);
        // --- END HAX ---
    }


    /**
     * Get Key from Value in a Map.
     *
     * @param <K>     the type of the map keys.
     * @param <V>     the type of the map values
     * @param map the map with keys of type K and values of type V.
     * @param value the value for which to get the key.
     * @return a stream of related keys for a value in map.
     */
    private <K, V> Stream<K> keys(Map<K, V> map, V value) {
        return map
          .entrySet()
          .stream()
          .filter(entry -> value.equals(entry.getValue()))
          .map(Map.Entry::getKey);
    }

    /**
     * Convert {@link BusinessAccount} to a list of {@link BusinessUnitApiDTO}.
     *
     * @param ba the BusinessAccount
     * @return the {@link BusinessUnitApiDTO}
     */
    private BusinessUnitApiDTO createBusinessUnitApiDTO(BusinessAccount ba) {
        BusinessUnitApiDTO businessAccount = new BusinessUnitApiDTO();
        businessAccount.setUuid(ba.getUuid());
        businessAccount.setAccountId(ba.getAccountId());
        businessAccount.setDisplayName(ba.getDisplayName());
        businessAccount.setClassName(ba.getClassName());
        final String cloudTypeName = ba.getCloudType();
        Optional<CloudType> cloudTypeOpt = CloudType.fromString(cloudTypeName);
        if (cloudTypeOpt.isPresent()) {
            final CloudType cloudType = cloudTypeOpt.get();
            if (SUPPORTED_CLOUD_TYPE.containsValue(cloudType)) {
                Optional<String> sdkCloudTypeName = keys(SUPPORTED_CLOUD_TYPE, cloudType).findFirst();
                if (sdkCloudTypeName.isPresent()) {
                    businessAccount.setCloudType(SUPPORTED_CLOUD_TYPE.containsValue(cloudType)
                                    ? com.vmturbo.api.enums.CloudType
                                                    .fromProbeType(sdkCloudTypeName.get())
                                    : com.vmturbo.api.enums.CloudType.UNKNOWN);
                }
            }
        }
        return businessAccount;
    }

    /**
     * Convert {@link RISetting} to a list of {@link SettingApiDTO}.
     *
     * @param ri the RISetting
     * @return a list of {@link SettingApiDTO}
     */
    private List<SettingApiDTO> createRiSettingApiDTOs(RISetting ri) {
        List<SettingApiDTO> riSettingDTOs = new ArrayList<>();

        // AWS
        if (ri.containsRiSettingByCloudtype(CloudType.AWS.name())) {
            RIProviderSetting riSettingProvider = ri.getRiSettingByCloudtypeMap().get(CloudType.AWS.name());

            riSettingDTOs.add(createStringSettingApiDTO(AWSPreferredOfferingClass.getSettingName(),
                    riSettingProvider.getPreferredOfferingClass().name(),
                    AWSPreferredOfferingClass.getDisplayName()));

            riSettingDTOs.add(createStringSettingApiDTO(AWSPreferredPaymentOption.getSettingName(),
                    riSettingProvider.getPreferredPaymentOption().name(),
                    AWSPreferredPaymentOption.getDisplayName()));

            Optional<PreferredTerm> term = riTermMapper.valueOf(PreferredTerm
                    .getPrefferedTermEnum(riSettingProvider.getPreferredTerm())
                    .orElse(null));
            riSettingDTOs.add(createStringSettingApiDTO(AWSPreferredTerm.getSettingName(),
                    term.map(PreferredTerm::name).orElse(null),
                    AWSPreferredTerm.getDisplayName()));
        }

        // Azure
        if (ri.containsRiSettingByCloudtype(CloudType.AZURE.name())) {
            RIProviderSetting riSettingProvider = ri.getRiSettingByCloudtypeMap().get(CloudType.AZURE.name());
            Optional<PreferredTerm> term = riTermMapper.valueOf(PreferredTerm
                    .getPrefferedTermEnum(riSettingProvider.getPreferredTerm())
                    .orElse(null));
            riSettingDTOs.add(createStringSettingApiDTO(AzurePreferredTerm.getSettingName(),
                    term.map(PreferredTerm::name).orElse(null),
                    AzurePreferredTerm.getDisplayName()));
        }

        if (ri.hasDemandType()) {
            final SettingApiDTO<String> demandTypeDto = new SettingApiDTO<>();
            demandTypeDto.setUuid(RIDemandType.getSettingName());
            demandTypeDto.setValue(ri.getDemandType().name());
            demandTypeDto.setDisplayName(RIDemandType.getDisplayName());
            riSettingDTOs.add(demandTypeDto);
        }

        if (ri.hasLookBackDurationDays()) {
            final SettingApiDTO<Integer> lookbackDurationDto = new SettingApiDTO<>();
            lookbackDurationDto.setUuid(CloudCommitmentHistoricalLookbackPeriod.getSettingName());
            lookbackDurationDto.setValue(ri.getLookBackDurationDays());
            lookbackDurationDto.setDisplayName(CloudCommitmentHistoricalLookbackPeriod.getDisplayName());
            riSettingDTOs.add(lookbackDurationDto);
        }

        if (ri.hasIncludeTerminatedEntityDemand()) {
            final SettingApiDTO<Boolean> terminatedInclusionDto = new SettingApiDTO<>();
            terminatedInclusionDto.setUuid(CloudCommitmentIncludeTerminatedEntities.getSettingName());
            terminatedInclusionDto.setValue(ri.getIncludeTerminatedEntityDemand());
            terminatedInclusionDto.setDisplayName(CloudCommitmentIncludeTerminatedEntities.getDisplayName());
            riSettingDTOs.add(terminatedInclusionDto);
        }

        return riSettingDTOs;
    }

    /**
     * Create OSMigration DTOs for the TopologyMigration DTO from the migration config API DTOs.
     *
     * @param configChanges the Configuration changes API DTO containing OS migration settings
     * @return a list of OSMigration DTOs to be applied by the CommoditiesEditor
     * @throws IllegalArgumentException For unrecognized migration profile options or destination
     * OS values
     */
    @Nonnull
    private static List<OSMigration> buildOsMigrations(@Nullable ConfigChangesApiDTO configChanges)
        throws IllegalArgumentException {
        Boolean matchToSource = null; // Have seen no specific setting yet
        Map<String, String> osSpecificSettings = new HashMap<>();

        if (configChanges != null && configChanges.getOsMigrationSettingList() != null) {
            for (SettingApiDTO<String> setting : configChanges.getOsMigrationSettingList()) {
                if (GlobalSettingSpecs.SelectedMigrationProfileOption.getSettingName().equals(setting.getUuid())) {
                    switch (OsMigrationProfileOption.valueOf(setting.getValue())) {
                        case MATCH_SOURCE_TO_TARGET_OS:
                            // No special configuration needed, No other settings can override.
                            return Collections.emptyList();
                        case BYOL:
                            // All OSes map to themselves but with BYOL set true. No other settings
                            // can override, so we're done.
                            return buildByolMigrations();
                        case CUSTOM_OS:
                            // Custom, which cannot be overridden, but we need to process the other
                            // settings to find the details so we can't break.
                            matchToSource = false;
                            break;
                        default:
                            throw new IllegalArgumentException("Unrecognized migration OS profile option "
                                + setting.getValue());
                    }
                } else if (GlobalSettingSpecs.MatchToSource.getSettingName().equals(setting.getUuid())) {
                    // Can be overridden by selectedMigrationProfileOption but not override it.
                    if (matchToSource == null) {
                        matchToSource = true;
                    }
                } else {
                    osSpecificSettings.put(setting.getUuid(), setting.getValue());
                }
            }
        }

        if (BooleanUtils.isTrue(matchToSource) || osSpecificSettings.isEmpty()) {
            // No specific mappings are needed.
            return Collections.emptyList();
        }

        return buildCustomOSMigrations(osSpecificSettings);
    }

    /**
     * Map custome OS migration settings for individual operating systems to the
     * corresponding OSMigtation protoufs.
     *
     * @param osSpecificSettings A map of os-specific custom settings ("xxxTargetOs"
     *                           and "xxxByol" for each operating system name "xxx"
     *                           in lowercase).
     * @return The corresponding OSMigration protobufs for all OSes.
     * @throws IllegalArgumentException if a requested destination OS type is not recognized.
     */
    @Nonnull
    private static List<OSMigration> buildCustomOSMigrations(
        @Nonnull Map<String, String> osSpecificSettings) throws IllegalArgumentException {
         List<OSMigration> osMigrations = new ArrayList<>();
         for (Entry<OSType, OperatingSystem> entry : protobufOsToApiOs.entrySet()) {
             OSType targetOs = entry.getKey();
             final String targetOsString = osSpecificSettings.get(
                 entry.getValue().name().toLowerCase() + TARGET_OS_SETTING_SUFFIX);

             if (targetOsString != null) {
                 targetOs = apiOsNameStringToDtoName(targetOsString);
             }

             boolean byol = false;
             final String byolString = osSpecificSettings.get(
                 entry.getValue().name().toLowerCase() + BYOL_SETTING_SUFFIX);
             if (byolString != null) {
                 byol = Boolean.parseBoolean(byolString);
             }

             osMigrations.add(OSMigration.newBuilder()
                 .setFromOs(entry.getKey()).setToOs(targetOs).setByol(byol).build());
         }

         return osMigrations;
    }

    /**
     * Map an OS name string as used in the value of an API OS Migration configuration setting
     * to the corresponding OSType enum. For backwards compatibility both "SLES" and "SUSE"
     * map to OSType.SUSE, but "SLES" is the preferred name in the external API.
     *
     * @param osString the OS name string
     * @return The corresponding OSType value
     * @throws IllegalArgumentException if the OS type is not recognized.
     */
    @Nonnull
    private static OSType apiOsNameStringToDtoName(@Nonnull final String osString)
        throws IllegalArgumentException {
        // Backward compatibility, recognize SUSE as well as SLES
        if (OSType.SUSE.name().equals(osString)) {
            return OSType.SUSE;
        }

        return apiOsToProtobufOs.get(OperatingSystem.valueOf(osString));
    }

    /**
     * Build OS migrations for a full Bring Your Own License migration.
     *
     * @return a list of BYOL OS migrations for each supported os.
     */
    @Nonnull
    private static List<OSMigration> buildByolMigrations() {
        List<OSMigration> osMigrations = new ArrayList<>();
        for (OSType os : protobufOsToApiOs.keySet()) {
            osMigrations.add(OSMigration.newBuilder()
                .setFromOs(os).setToOs(os).setByol(true).build());
        }

        return osMigrations;
    }

    /**
     * Convert a list of {@link OSMigration} to a list of {@link SettingApiDTO}.
     *
     * @param osMigrations the OSMigrations to convert
     * @return a list of {@link SettingApiDTO} suitable for passing to
     *     {@link ConfigChangesApiDTO#setOsMigrationSettingList}
     * @throws ConversionException if unable to map an OS to an API equivalent name.
     */
    @Nonnull
    private List<SettingApiDTO<String>> createOsMigrationSettingApiDTOs(
        @Nonnull List<OSMigration> osMigrations) throws ConversionException {
        List<SettingApiDTO<String>> settingDTOs = new ArrayList<>();

        // One or both of these must become false through the loop below
        boolean allByol = true;
        boolean allMatchToSource = true;

        for (OperatingSystem os : OperatingSystem.values()) {
            OSType dtoOS = apiOsToProtobufOs.get(os);

            OSMigration osMigration = osMigrations.stream()
                .filter(migration -> migration.getFromOs() == dtoOS)
                .findAny()
                .orElse(OSMigration.newBuilder().setFromOs(dtoOS).setToOs(dtoOS).build());

            boolean sameOs = osMigration.getFromOs() == osMigration.getToOs();
            allByol = allByol && sameOs && osMigration.getByol();
            allMatchToSource = allMatchToSource && sameOs && !osMigration.getByol();

            OperatingSystem targetOs = protobufOsToApiOs.get(osMigration.getToOs());
            if (targetOs == null) {
                throw new ConversionException("Cannot map scenario OS "
                    + osMigration.getToOs().name() + " to an OS type defined in the API.");
            }

            final String settingKey = os.name().toLowerCase();
            settingDTOs.add(createStringSettingApiDTO(settingKey + TARGET_OS_SETTING_SUFFIX,
                targetOs.name()));

            settingDTOs.add(createStringSettingApiDTO(settingKey + BYOL_SETTING_SUFFIX,
                Boolean.toString(osMigration.getByol())));
        }

        OsMigrationProfileOption migrationOption = OsMigrationProfileOption.CUSTOM_OS;
        if (allMatchToSource) {
            migrationOption = OsMigrationProfileOption.MATCH_SOURCE_TO_TARGET_OS;
        } else if (allByol) {
            migrationOption = OsMigrationProfileOption.BYOL;
        }

        settingDTOs.add(createStringSettingApiDTO(
            GlobalSettingSpecs.SelectedMigrationProfileOption.getSettingName(),
            migrationOption.name()));

        settingDTOs.add(createStringSettingApiDTO(
            GlobalSettingSpecs.MatchToSource.getSettingName(),
            Boolean.toString(allMatchToSource)));

        return settingDTOs;
    }

    /**
     * Create a {@link SettingApiDTO} given a uuid, value and displayName.
     *
     * @param uuid        uuid of the setting
     * @param value       value of the setting
     * @param displayName displayName of the setting
     * @return {@link SettingApiDTO}
     */
    private SettingApiDTO createStringSettingApiDTO(@Nonnull final String uuid,
                                                    @Nonnull final String value,
                                                    @Nonnull final String displayName) {
        SettingApiDTO<String> settingDTO = new SettingApiDTO<>();
        settingDTO.setUuid(uuid);
        settingDTO.setValue(value);
        settingDTO.setDisplayName(displayName);

        return settingDTO;
    }

    /**
     * Create a {@link SettingApiDTO} given a uuid and value. The displayName will be
     * set to the uuid.
     *
     * @param uuid        uuid of the setting
     * @param value       value of the setting
     * @return {@link SettingApiDTO}
     */
    private SettingApiDTO createStringSettingApiDTO(@Nonnull final String uuid,
                                                    @Nonnull final String value) {
        return createStringSettingApiDTO(uuid, value, uuid);
    }

    /**
     * Build the ConfigChangesApiDto part of a ScenarioApiDto from the given scenario changes.
     *
     * @param changes the scenario configuration to convert
     * @param mappingContext a context for use in the conversion process
     * @return the REST API representation of the configuration changes part of the scenario
     * @throws ConversionException If it is not possible to convert the scenario to the REST
     *     API representation
     */
    @Nonnull
    private ConfigChangesApiDTO buildApiConfigChanges(
        @Nonnull final List<ScenarioChange> changes,
        @Nonnull final ScenarioChangeMappingContext mappingContext)
        throws ConversionException {
        final Optional<TopologyMigration> migration = changes.stream()
            .filter(ScenarioChange::hasTopologyMigration)
            .map(ScenarioChange::getTopologyMigration)
            .findFirst();

        final List<SettingApiDTO<String>> osMigrationSettings =
            migration.isPresent()
                ? createOsMigrationSettingApiDTOs(migration.get().getOsMigrationsList())
                : Collections.emptyList();

        // Resize settings are not controllable for Migration plans
        final List<SettingApiDTO<String>> settingChanges = migration.isPresent()
            ? Collections.emptyList()
            : changes.stream()
                .filter(ScenarioChange::hasSettingOverride)
                .map(ScenarioChange::getSettingOverride)
                // TODO: Show rate of resize setting correctly in the plan UI.
                // Skip rate of resize setting and don't return it because plan UI doesn't show it correctly.
                .filter(override -> !EntitySettingSpecs.RateOfResize.getSettingName()
                    .equals(override.getSetting().getSettingSpecName()))
                .flatMap(override -> createApiSettingFromOverride(override, mappingContext).stream())
                .collect(Collectors.toList());

        final List<PlanChanges> allPlanChanges = changes.stream()
                .filter(ScenarioChange::hasPlanChanges).map(ScenarioChange::getPlanChanges)
                .collect(Collectors.toList());

        final List<SettingApiDTO> riSetting = changes.stream()
                .filter(ScenarioChange::hasRiSetting)
                .map(ScenarioChange::getRiSetting)
                .flatMap(ri -> createRiSettingApiDTOs(ri).stream())
                .collect(Collectors.toList());

        final List<RemoveConstraintApiDTO> removeConstraintApiDTOS = getRemoveConstraintsDtos(allPlanChanges, mappingContext);

        final ConfigChangesApiDTO outputChanges = new ConfigChangesApiDTO();

        outputChanges.setOsMigrationSettingList(osMigrationSettings);
        outputChanges.setRemoveConstraintList(removeConstraintApiDTOS);
        outputChanges.setAutomationSettingList(settingsManagerMapping
                .convertToPlanSetting(settingChanges));
        outputChanges.setAddPolicyList(Lists.newArrayList());
        outputChanges.setRemovePolicyList(Lists.newArrayList());
        Optional<IncludedCoupons> includedCoupons = allPlanChanges
                .stream().filter(PlanChanges::hasIncludedCoupons)
                .map(PlanChanges::getIncludedCoupons)
                .findFirst();
        if (includedCoupons.isPresent()) {
            final IncludedCouponsApiDTO includedCouponsDto = new IncludedCouponsApiDTO();
            IncludedCoupons includedCouponsMsg = includedCoupons.get();
            final List<Long> includedCouponOidsList =
                    includedCouponsMsg.getIncludedCouponOidsList();
            if (includedCouponOidsList != null) {
                includedCouponsDto.setIncludedCouponOidsList(includedCouponOidsList);
                includedCouponsDto.setIswhiteList(includedCouponsMsg.hasIsWhiteList()
                        ? includedCouponsMsg.getIsWhiteList()
                        : true);
            }
            outputChanges.setIncludedCoupons(includedCouponsDto);
        }

        outputChanges.setRiSettingList(riSetting);
        changes.stream()
                .filter(change -> change.getDetailsCase() ==  DetailsCase.PLAN_CHANGES
                        && change.getPlanChanges().hasPolicyChange())
                .forEach(change -> buildApiPolicyChange(
                        change.getPlanChanges().getPolicyChange(), outputChanges, policiesService));

        Optional<BusinessAccount> subscription = allPlanChanges
                        .stream().filter(PlanChanges::hasSubscription)
                        .map(PlanChanges::getSubscription)
                        .findFirst();
        if (subscription.isPresent()) {
            final BusinessUnitApiDTO businessAccountApiDto = createBusinessUnitApiDTO(subscription
                            .get());
            outputChanges.setSubscription(businessAccountApiDto);
        }
        return outputChanges;
    }

    /**
     * Returns remove constraint plan changes if plan changes have it
     *
     * @param allPlanChanges
     * @return remove constraint changes
     */
    private List<RemoveConstraintApiDTO> getRemoveConstraintsDtos(final List<PlanChanges> allPlanChanges, ScenarioChangeMappingContext mappingContext) {
        return allPlanChanges.stream().filter(planChanges ->
                !CollectionUtils.isEmpty(planChanges.getIgnoreConstraintsList()))
                .map(PlanChanges::getIgnoreConstraintsList).flatMap(List::stream)
                .map(constraint -> this.toRemoveConstraintApiDTO(constraint, mappingContext)).collect(Collectors.toList());
    }

    @VisibleForTesting
    @Nonnull
    RemoveConstraintApiDTO toRemoveConstraintApiDTO(@Nonnull IgnoreConstraint constraint, ScenarioChangeMappingContext mappingContext) {
        final RemoveConstraintApiDTO constraintApiDTO = new RemoveConstraintApiDTO();

        if (constraint.hasIgnoreAllEntities() && constraint.getIgnoreAllEntities()) {
            constraintApiDTO.setConstraintType(ConstraintType.GlobalIgnoreConstraint);
        } else if (constraint.hasGlobalIgnoreEntityType()) {
            EntityType entityType = constraint.getGlobalIgnoreEntityType().getEntityType();
            constraintApiDTO.setTargetEntityType(
                    ApiEntityType.fromSdkTypeToEntityTypeString(entityType.getNumber()));
            constraintApiDTO.setConstraintType(ConstraintType.GlobalIgnoreConstraint);
        } else if (constraint.hasIgnoreGroup()) { // Per Group Settings
            ConstraintGroup constraintGroup = constraint.getIgnoreGroup();
            constraintApiDTO.setConstraintType(
                    ConstraintType.valueOf(constraintGroup.getCommodityType()));
            //ConstraintGroup configuration only supports groups, can derive dto mappingContext will return
            final BaseApiDTO baseApiDTO = mappingContext.dtoForId(constraintGroup.getGroupUuid());
            constraintApiDTO.setTarget(baseApiDTO);
            if (mappingContext.groupIdExists(constraintGroup.getGroupUuid())) {
                constraintApiDTO.setTargetEntityType(((GroupApiDTO)baseApiDTO).getGroupType());
            }
        }
        return constraintApiDTO;
    }

    @VisibleForTesting
    @Nonnull
    protected Collection<SettingApiDTO<String>> createApiSettingFromOverride(
            @Nonnull final SettingOverride settingOverride,
            @Nonnull final ScenarioChangeMappingContext mappingContext) throws IllegalStateException {
        final SettingApiDTOPossibilities possibilities =
                settingsMapper.toSettingApiDto(settingOverride.getSetting());

        if (settingOverride.hasEntityType() && settingOverride.hasGroupOid()) {
            new IllegalStateException("Not Supported: SettingOverride with groupOid but no entityType");
        }

        if (!settingOverride.hasEntityType()) {
            return possibilities.getAll();
        }

        final String entityType = ApiEntityType.fromType(settingOverride.getEntityType()).apiStr();

        SettingApiDTO<String> settingSpec = possibilities.getSettingForEntityType(entityType)
                .orElseThrow(() -> new IllegalStateException("Entity type " + entityType +
                        " not supported by the setting " +
                        settingOverride.getSetting().getSettingSpecName() + " being overriden."));

        if (settingOverride.hasGroupOid()) {
            BaseApiDTO target = mappingContext.dtoForId(settingOverride.getGroupOid());
            settingSpec.setSourceGroupUuid(target.getUuid());
            settingSpec.setSourceGroupName(target.getDisplayName());
        }

        return Collections.singletonList(settingSpec);
    }

    @Nonnull
    private TopologyChangesApiDTO buildApiTopologyChanges(
            @Nonnull final List<ScenarioChange> changes, ScenarioChangeMappingContext context) {
        final TopologyChangesApiDTO outputChanges = new TopologyChangesApiDTO();
        changes.forEach(change -> {
            switch (change.getDetailsCase()) {
                case TOPOLOGY_ADDITION:
                    buildApiTopologyAddition(change.getTopologyAddition(), outputChanges, context);
                    break;
                case TOPOLOGY_REMOVAL:
                    buildApiTopologyRemoval(change.getTopologyRemoval(), outputChanges, context);
                    break;
                case TOPOLOGY_REPLACE:
                    buildApiTopologyReplace(change.getTopologyReplace(), outputChanges, context);
                    break;
                case TOPOLOGY_MIGRATION:
                    buildApiTopologyMigration(change.getTopologyMigration(), outputChanges, context);
                    break;
                default:
            }
        });

        return outputChanges;
    }

    private static void buildApiTopologyAddition(@Nonnull final TopologyAddition addition,
                                                 @Nonnull final TopologyChangesApiDTO outputChanges,
                                                 @Nonnull final ScenarioChangeMappingContext context) {
        final AddObjectApiDTO changeApiDTO = new AddObjectApiDTO();
        changeApiDTO.setCount(addition.getAdditionCount());
        switch (addition.getAdditionTypeCase()) {
            case ENTITY_ID:
                changeApiDTO.setTarget(context.dtoForId(addition.getEntityId()));
                break;
            case TEMPLATE_ID:
                changeApiDTO.setTarget(context.dtoForId(addition.getTemplateId()));
                break;
            case GROUP_ID:
                changeApiDTO.setTarget(context.dtoForId(addition.getGroupId()));
                break;
            case ADDITIONTYPE_NOT_SET:
                logger.warn("Unset addition type in topology addition: {}", addition);
                return;
        }
        changeApiDTO.setProjectionDays(addition.getChangeApplicationDaysList());
        if (addition.hasTargetEntityType()) {
            changeApiDTO.setTargetEntityType(
                    ApiEntityType.fromSdkTypeToEntityTypeString(addition.getTargetEntityType()));
        }

        final List<AddObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getAddList(),
            new ArrayList<>());
        changeApiDTOs.add(changeApiDTO);
        outputChanges.setAddList(changeApiDTOs);
    }

    private static void buildApiTopologyRemoval(@Nonnull final TopologyRemoval removal,
                                                @Nonnull final TopologyChangesApiDTO outputChanges,
                                                @Nonnull final ScenarioChangeMappingContext context) {
        final List<RemoveObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getRemoveList(),
                new ArrayList<>());
        final RemoveObjectApiDTO changeApiDTO = new RemoveObjectApiDTO();
        switch (removal.getRemovalTypeCase()) {
            case ENTITY_ID:
                changeApiDTO.setTarget(context.dtoForId(removal.getEntityId()));
                break;
            case GROUP_ID:
                changeApiDTO.setTarget(context.dtoForId(removal.getGroupId()));
                break;
            case REMOVALTYPE_NOT_SET:
                logger.warn("Unset removal type in topology removal: {}", removal);
                return;
        }
        changeApiDTO.setProjectionDay(removal.getChangeApplicationDay());

        changeApiDTOs.add(changeApiDTO);
        if (removal.hasTargetEntityType()) {
            changeApiDTO.setTargetEntityType(
                    ApiEntityType.fromSdkTypeToEntityTypeString(removal.getTargetEntityType()));
        }
        outputChanges.setRemoveList(changeApiDTOs);
    }

    private static void buildApiTopologyReplace(@Nonnull final TopologyReplace replace,
                                                @Nonnull final TopologyChangesApiDTO outputChanges,
                                                @Nonnull final ScenarioChangeMappingContext context) {
        ReplaceObjectApiDTO changeApiDTO = new ReplaceObjectApiDTO();
        switch (replace.getReplaceTypeCase()) {
            case REMOVE_ENTITY_ID:
                changeApiDTO.setTarget(context.dtoForId(replace.getRemoveEntityId()));
                break;
            case REMOVE_GROUP_ID:
                changeApiDTO.setTarget(context.dtoForId(replace.getRemoveGroupId()));
                break;
            case REPLACETYPE_NOT_SET:
                logger.warn("Unset replace type in topology replace: {}", replace);
                return;
        }
        changeApiDTO.setTemplate(context.dtoForId(replace.getAddTemplateId()));
        changeApiDTO.setProjectionDay(replace.getChangeApplicationDay());

        if (replace.hasTargetEntityType()) {
            changeApiDTO.setTargetEntityType(
                    ApiEntityType.fromSdkTypeToEntityTypeString(replace.getTargetEntityType()));
        }

        List<ReplaceObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getReplaceList(),
            new ArrayList<>());
        changeApiDTOs.add(changeApiDTO);
        outputChanges.setReplaceList(changeApiDTOs);
    }

    private void buildApiPolicyChange(PolicyChange policyChange,
                    ConfigChangesApiDTO outputChanges, PoliciesService policiesService) {
        try {
            PolicyApiDTO policy = policyChange.hasPolicyId()
                    // A policy with a policy ID is a server policy
                    ? policiesService.getPolicyByUuid(String.valueOf(policyChange.getPolicyId()))
                    // A policy without a policy ID is one that was defined only for the plan
                    // where it was defined
                    : policiesService.toPolicyApiDTO(policyChange.getPlanOnlyPolicy());
            if (policyChange.getEnabled()) {
                outputChanges.getAddPolicyList().add(policy);
            } else {
                outputChanges.getRemovePolicyList().add(policy);
            }
        } catch (Exception e) {
            logger.error("Error handling policy change");
            logger.error(policyChange, e);
        }
    }

    /**
     * Build a list of {@link MigrateObjectApiDTO} mapped from a single {@link TopologyMigration} object, and add it
     * to {@param outputChanges} migrateList.
     *
     * @param migration a {@link TopologyMigration} object whose data should be mapped to a list of {@link MigrateObjectApiDTO}
     * @param outputChanges the {@link TopologyChangesApiDTO} object on which migrateList should be populated
     * @param context a {@link ScenarioChangeMappingContext} holding a cache of entities and groups
     */
    private void buildApiTopologyMigration(@Nonnull final TopologyMigration migration,
                                           @Nonnull final TopologyChangesApiDTO outputChanges,
                                           @Nonnull final ScenarioChangeMappingContext context) {
        final List<MigrateObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(
                outputChanges.getMigrateList(),
                new ArrayList<>());
        // Default value is true
        boolean removeNonMigratingWorkloads = true;
        // Make VM the default (somewhat arbitrary, but more common)
        DestinationEntityType destinationEntityType = migration.hasDestinationEntityType() && migration.getDestinationEntityType() == TopologyMigration.DestinationEntityType.DATABASE_SERVER
                ? DestinationEntityType.DatabaseServer
                : DestinationEntityType.VirtualMachine;
        if (migration.hasRemoveNonMigratingWorkloads()) {
            removeNonMigratingWorkloads = migration.getRemoveNonMigratingWorkloads();
        }

        List<MigrationReference> sources = migration.getSourceList();
        List<MigrationReference> destinations = migration.getDestinationList();

        List<MigrateObjectApiDTO> migrateList = getMigrateList(
                sources,
                destinations,
                context,
                removeNonMigratingWorkloads,
                destinationEntityType);

        migrateList = getDeprecatedFormatMigrateList(
                migrateList.get(0),
                sources,
                destinations,
                context,
                removeNonMigratingWorkloads,
                destinationEntityType);

        changeApiDTOs.addAll(migrateList);
        outputChanges.setMigrateList(changeApiDTOs);
    }

    /**
     * Generate a migrate list of {@link MigrateObjectApiDTO} of length 1 representing the characteristics of a
     * cloud migration.
     *
     * @param sources the entities to migrate to a new cloud location
     * @param destinations the locations to which workloads should be migrated
     * @param context a {@link ScenarioChangeMappingContext} holding a cache of entities and groups
     * @param removeNonMigratingWorkloads whether workloads in the specified migration destinations should be removed
     * @param destinationEntityType the entity type of migrated workloads
     * @return a list of length 1 of {@link MigrateObjectApiDTO} representing the migration at hand
     */
    @Nonnull
    private static List<MigrateObjectApiDTO> getMigrateList(
            @Nonnull final List<MigrationReference> sources,
            @Nonnull final List<MigrationReference> destinations,
            @Nonnull final ScenarioChangeMappingContext context,
            @Nonnull final Boolean removeNonMigratingWorkloads,
            @Nonnull final DestinationEntityType destinationEntityType) {
        List<BaseApiDTO> sourceDtos = sources.stream()
                .map(source -> context.dtoForId(source.getOid()))
                .collect(Collectors.toList());
        List<BaseApiDTO> destinationDtos = destinations.stream()
                .map(destination -> context.dtoForId(destination.getOid()))
                .collect(Collectors.toList());

        MigrateObjectApiDTO migrateObjectApiDTO = new MigrateObjectApiDTO();
        migrateObjectApiDTO.setSources(sourceDtos);
        migrateObjectApiDTO.setDestinations(destinationDtos);
        migrateObjectApiDTO.setRemoveNonMigratingWorkloads(removeNonMigratingWorkloads);
        migrateObjectApiDTO.setDestinationEntityType(destinationEntityType);
        // this is the default value - we do not support projection in migration plans
        migrateObjectApiDTO.setProjectionDay(0);
        return Lists.newArrayList(migrateObjectApiDTO);
    }

    /**
     * Generate a migrate list of {@link MigrateObjectApiDTO} the same length as {@param sources}. This should be
     * removed when the deprecated source and destination fields of {@link MigrateObjectApiDTO} are removed.
     *
     * @param migrateObjectApiDTO the entity created by getMigrateList that should be used as the first item in the returned collection
     * @param sources the entities to migrate to a new cloud location
     * @param destinations the locations to which workloads should be migrated
     * @param context a {@link ScenarioChangeMappingContext} holding a cache of entities and groups
     * @param removeNonMigratingWorkloads whether workloads in the specified migration destinations should be removed
     * @param destinationEntityType the entity type of migrated workloads
     * @return a list of length 1 of {@link MigrateObjectApiDTO} representing the migration at hand
     */
    @Nonnull
    private static List<MigrateObjectApiDTO> getDeprecatedFormatMigrateList(
            @Nonnull final MigrateObjectApiDTO migrateObjectApiDTO,
            @Nonnull final List<MigrationReference> sources,
            @Nonnull final List<MigrationReference> destinations,
            @Nonnull final ScenarioChangeMappingContext context,
            @Nonnull final Boolean removeNonMigratingWorkloads,
            @Nonnull final DestinationEntityType destinationEntityType) {
        final List<MigrateObjectApiDTO> changeApiDTOs = Lists.newArrayList();
        for (int sourceNum = 0; sourceNum < sources.size(); sourceNum++) {
            MigrateObjectApiDTO changeApiDTO;
            if (sourceNum == 0) {
                changeApiDTO = migrateObjectApiDTO;
            } else {
                changeApiDTO = new MigrateObjectApiDTO();
                // this is the default value- we do not support projection in migration plans
                changeApiDTO.setProjectionDay(0);
                changeApiDTO.setRemoveNonMigratingWorkloads(removeNonMigratingWorkloads);
                changeApiDTO.setDestinationEntityType(destinationEntityType);
            }
            MigrationReference source = sources.get(sourceNum);
            MigrationReference destination = destinations.get(Math.min(sourceNum, destinations.size() - 1));
            switch (source.getTypeCase()) {
                case ENTITY_TYPE:
                case GROUP_TYPE:
                    changeApiDTO.setSource(context.dtoForId(source.getOid()));
                    break;
                case TYPE_NOT_SET:
                    logger.error("Unset source entity type in topology migration- source: {}",
                            source);
                    return Lists.newArrayList();
            }
            switch (destination.getTypeCase()) {
                case ENTITY_TYPE:
                case GROUP_TYPE:
                    changeApiDTO.setDestination(context.dtoForId(destination.getOid()));
                    break;
                case TYPE_NOT_SET:
                    logger.error("Unset destination entity type in topology migration- destination: {}",
                            destination);
                    return Lists.newArrayList();
            }
            changeApiDTOs.add(changeApiDTO);
        }
        return changeApiDTOs;
    }

    /**
     * A context object to map {@link ScenarioChange} objects to their API equivalents.
     *
     * Mainly intended to abstract away the details of interacting with other services in order
     * to supply all necessary information to the API (e.g. the names of groups, templates, and
     * entities).
     *
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    public static class ScenarioChangeMappingContext {
        private final Map<Long, ServiceEntityApiDTO> serviceEntityMap;
        private final Map<Long, TemplateApiDTO> templatesMap;
        private final Map<Long, GroupApiDTO> groupMap;

        public ScenarioChangeMappingContext(@Nonnull final RepositoryApi repositoryApi,
                                            @Nonnull final TemplatesUtils templatesUtils,
                                            @Nonnull final GroupServiceBlockingStub groupRpcService,
                                            @Nonnull final GroupMapper groupMapper,
                                            @Nonnull final List<ScenarioChange> changes)
                throws ConversionException, InterruptedException {
            // Get type information about entities involved in the scenario changes. We get it
            // in a single call to reduce the number of round-trips and total wait-time.
            //
            // Here we are retrieving the entire ServiceEntity DTO from the repository.
            // This shouldn't be an issue as long as scenarios don't contain absurd numbers of entities,
            // and as long as we're not retrieving detailed information about lots of scenarios.
            // If necessary we can optimize it by exposing an API call that returns only entity types.
            this.serviceEntityMap = repositoryApi.entitiesRequest(PlanDTOUtil.getInvolvedEntities(changes))
                .getSEMap();
            // Get all involved templates
            this.templatesMap =
                    templatesUtils.getTemplatesMapByIds(PlanDTOUtil.getInvolvedTemplates(changes));

            final Set<Long> involvedGroups = PlanDTOUtil.getInvolvedGroups(changes);
            if (!involvedGroups.isEmpty()) {
                final Iterator<Grouping> iterator = groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(
                                GroupFilter.newBuilder()
                                    .addAllId(involvedGroups)
                        ).build());
                final List<Grouping> groups = Lists.newArrayList(iterator);
                this.groupMap =
                        groupMapper.groupsToGroupApiDto(groups, false);
            } else {
                groupMap = Collections.emptyMap();
            }
        }

        /**
         * This function is used to get the API DTO of a specific object in the system referenced
         * by a scenario change.
         *
         * @param id The ID of the object.
         * @return A {@link BaseApiDTO} (or one of its subclasses) describing the object. If no
         *         object with that ID exists in the {@link ScenarioChangeMappingContext}, return
         *         a filler {@link BaseApiDTO}.
         */
        public BaseApiDTO dtoForId(final long id) {
            if (serviceEntityMap.containsKey(id)) {
                return serviceEntityMap.get(id);
            } else if (templatesMap.containsKey(id)) {
                return templatesMap.get(id);
            } else if (groupIdExists(id)) {
                return groupMap.get(id);
            } else {
                logger.error("Unable to find entity, template, or group with ID {} when mapping "
                    + "scenario change. Could the object have been removed from the system/topology?",
                    id);
                final BaseApiDTO entity = new BaseApiDTO();
                entity.setUuid(Long.toString(id));
                entity.setDisplayName(ApiEntityType.UNKNOWN.apiStr());
                return entity;
            }
        }

        /**
         * Return if a group with the given id exists.
         *
         * @param id The ID of the object.
         * @return If a group with the given id exists.
         */
        boolean groupIdExists(final long id) {
            return groupMap.containsKey(id);
        }
    }


}
