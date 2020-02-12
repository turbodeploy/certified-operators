package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredOfferingClass;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredPaymentOption;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredTerm;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingValueEntityTypeKey;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.ConfigChangesApiDTO;
import com.vmturbo.api.dto.scenario.IncludedCouponsApiDTO;
import com.vmturbo.api.dto.scenario.LoadChangesApiDTO;
import com.vmturbo.api.dto.scenario.MaxUtilizationApiDTO;
import com.vmturbo.api.dto.scenario.RelievePressureObjectApiDTO;
import com.vmturbo.api.dto.scenario.RemoveConstraintApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.dto.scenario.UtilizationApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ConstraintType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.PlanDTOUtil;
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
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.ConstraintGroup;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IncludedCoupons;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel.Builder;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

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
     * Hardcoded group display name for current utilization changes.
     */
    private static final String VIRTUAL_MACHINES_DISPLAY_NAME = "Virtual Machines";

    /**
     * Supported RI Purchase Profile Settings.
     */
    private static final EnumSet<GlobalSettingSpecs> SUPPORTED_RI_PROFILE_SETTINGS = EnumSet
                    .of(AWSPreferredOfferingClass, AWSPreferredPaymentOption, AWSPreferredTerm,
                        RIDemandType);

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

    private static final List<ConstraintType> ALLEVIATE_PRESSURE_IGNORE_CONSTRAINTS = Arrays.asList(
                    ConstraintType.NetworkCommodity, ConstraintType.StorageClusterCommodity,
                    ConstraintType.DataCenterCommodity);

    private final EnumMapper<OfferingClass> riOfferingClassMapper = EnumMapper.of(OfferingClass.class);
    private final EnumMapper<PaymentOption> riPaymentOptionMapper = EnumMapper.of(PaymentOption.class);
    private final EnumMapper<PreferredTerm> riTermMapper = EnumMapper.of(PreferredTerm.class);
    private final EnumMapper<DemandType> riDemandTypeMapper = EnumMapper.of(DemandType.class);

    private final TemplatesUtils templatesUtils;

    private final RepositoryApi repositoryApi;

    private final SettingsManagerMapping settingsManagerMapping;

    private final SettingsMapper settingsMapper;

    private PoliciesService policiesService;

    private final GroupServiceBlockingStub groupRpcService;

    private final GroupMapper groupMapper;

    private final UuidMapper uuidMapper;

    public ScenarioMapper(@Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final TemplatesUtils templatesUtils,
                          @Nonnull final SettingsManagerMapping settingsManagerMapping,
                          @Nonnull final SettingsMapper settingsMapper,
                          @Nonnull final PoliciesService policiesService,
                          @Nonnull final GroupServiceBlockingStub groupRpcService,
                          @Nonnull final GroupMapper groupMapper,
                          @Nonnull final UuidMapper uuidMapper) {

        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.policiesService = Objects.requireNonNull(policiesService);
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
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
     */
    @Nonnull
    public ScenarioInfo toScenarioInfo(final String name,
                                       @Nonnull final ScenarioApiDTO dto) throws OperationFailedException, InvalidOperationException {
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

        infoBuilder.addAllChanges(getTopologyChanges(dto.getTopologyChanges(), templateIds));
        infoBuilder.addAllChanges(getConfigChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getPolicyChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getLoadChanges(dto.getLoadChanges()));
        // check the scenario and add default automation settings if necessary
        infoBuilder.addAllChanges(checkAndCreateDefaultAutomationSettingsForPlan(dto));
        getScope(dto.getScope()).ifPresent(infoBuilder::setScope);
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
                changes.add(getChangeWithGlobalSettingsDisabled(EntitySettingSpecs.Provision));
                changes.add(getChangeWithGlobalSettingsDisabled(EntitySettingSpecs.Suspend));
                changes.add(getChangeWithGlobalSettingsDisabled(EntitySettingSpecs.Resize));
                changes.add(getChangeWithGlobalSettingsDisabled(EntitySettingSpecs.Reconfigure));
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
    private ScenarioChange getChangeWithGlobalSettingsDisabled(EntitySettingSpecs spec) {
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
                    UIEntityType.fromStringToSdkType(maxUtilization.getSelectedEntityType()));
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
     */
    @Nonnull
    public ScenarioApiDTO toScenarioApiDTO(@Nonnull final Scenario scenario) {
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
            .setScenarioInfo(ScenarioInfo
                .newBuilder().addAllChanges(changes).setName(scenario.getScenarioInfo().getName()))
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
                                UIEntityType.fromSdkTypeToEntityTypeString(maxUtilizationLevel.getSelectedEntityType()));
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
            return Collections.emptyList();
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
                            UIEntityType.fromStringToSdkType(apiDto.getEntityType()));
                }

                if (apiDto.getSourceGroupUuid() != null) {
                    settingOverride.setGroupOid(uuidMapper.fromUuid(apiDto.getSourceGroupUuid()).oid());
                }

                retChanges.add(ScenarioChange.newBuilder()
                        .setSettingOverride(settingOverride)
                        .build());
            }
        }

        return retChanges.build();
    }

    @Nonnull
    private List<ScenarioChange> getConfigChanges(@Nullable final ConfigChangesApiDTO configChanges) throws OperationFailedException {
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

        RISetting.Builder awsRISetting = RISetting.newBuilder();

        SUPPORTED_RI_PROFILE_SETTINGS.forEach(setting -> {
            SettingsMapper.inputValueToString(settings.get(setting.getSettingName())).ifPresent(settingValue -> {
                switch (setting) {
                    case AWSPreferredOfferingClass:
                        riOfferingClassMapper.valueOf(settingValue).ifPresent(awsRISetting::setPreferredOfferingClass);
                        break;
                    case AWSPreferredPaymentOption:
                        riPaymentOptionMapper.valueOf(settingValue).ifPresent(awsRISetting::setPreferredPaymentOption);
                        break;
                    case AWSPreferredTerm:
                        riTermMapper.valueOf(settingValue).map(PreferredTerm::getYears).ifPresent(awsRISetting::setPreferredTerm);
                        break;
                    case RIDemandType:
                        riDemandTypeMapper.valueOf(settingValue).ifPresent(awsRISetting::setDemandType);
                        break;
                }
            });
        });
        return ScenarioChange.newBuilder().setRiSetting(awsRISetting).build();
    }

    @Nonnull
    private ScenarioChange buildPlanChanges(@Nonnull ConfigChangesApiDTO configChanges) {
        final PlanChanges.Builder planChangesBuilder = PlanChanges.newBuilder();
        final List<RemoveConstraintApiDTO> constraintsToRemove = configChanges.getRemoveConstraintList();
        if (!CollectionUtils.isEmpty(constraintsToRemove)) {
            planChangesBuilder.addAllIgnoreConstraints(getIgnoreConstraintsPlanSetting(constraintsToRemove));
        }

        return ScenarioChange.newBuilder().setPlanChanges(planChangesBuilder.build()).build();
    }

    @Nonnull
    private List<IgnoreConstraint> getIgnoreConstraintsPlanSetting(
            @Nonnull List<RemoveConstraintApiDTO> constraintsToIgnore) {
        final ImmutableList.Builder<IgnoreConstraint> ignoreConstraintsBuilder = ImmutableList.builder();
        for (RemoveConstraintApiDTO constraint : constraintsToIgnore) {
            final IgnoreConstraint ignoreConstraint = toIgnoreConstraint(constraint);
            ignoreConstraintsBuilder.add(ignoreConstraint);
        }
        return ignoreConstraintsBuilder.build();
    }

    @Nonnull
    private IgnoreConstraint toIgnoreConstraint(@Nonnull RemoveConstraintApiDTO constraint) {
        ConstraintGroup.Builder constraintGroup = ConstraintGroup.newBuilder();
        ConstraintType constraintType = constraint.getConstraintType();
        if (constraintType == null || constraintType == ConstraintType.GlobalIgnoreConstraint) {
            constraintGroup.setCommodityType(ConstraintType.GlobalIgnoreConstraint.name());
        } else {
            constraintGroup.setCommodityType(constraintType.name());
            // group uuid is only available if it's not global constraint
            constraintGroup.setGroupUuid(Long.parseLong(constraint.getTarget().getUuid()));
        }
        return IgnoreConstraint.newBuilder()
                .setIgnoreGroup(constraintGroup)
                .build();
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
                if (setting.getEntityType() != null && setting.getUuid() != null
                                && UIEntityType.fromString(setting.getEntityType()).typeNumber() == EntityType.PHYSICAL_MACHINE_VALUE
                                && setting.getUuid().equalsIgnoreCase(EntitySettingSpecs.Provision.getSettingName())) {
                    hasPMProvisionSetting = true;
                }
            }
        }
        // DECOMMISSION HOST plan should have host provision disabled as default
        if (!hasPMProvisionSetting && dto.getType() != null && dto.getType().equalsIgnoreCase(DECOMMISSION_HOST_SCENARIO_TYPE)) {
            final SettingOverride.Builder settingOverride = SettingOverride.newBuilder()
                    .setSetting(Setting.newBuilder()
                            .setSettingSpecName(EntitySettingSpecs.Provision.getSettingName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED)))
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);
            changes.add(ScenarioChange.newBuilder().setSettingOverride(settingOverride).build());
        }
        return changes.build();
    }

    @Nonnull
    private List<ScenarioChange> getTopologyChanges(final TopologyChangesApiDTO topoChanges,
                                           @Nonnull final Set<Long> templateIds) {
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

        if (!CollectionUtils.isEmpty(topoChanges.getMigrateList())) {
            logger.warn("Skipping {} migration changes.",
                    topoChanges.getMigrateList().size());
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
                    UIEntityType.fromStringToSdkType(change.getTargetEntityType()));
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
            removalBuilder.setTargetEntityType(UIEntityType.fromString(change.getTargetEntityType()).typeNumber());
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
                    UIEntityType.fromStringToSdkType(change.getTargetEntityType()));
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
     * @param scopeDTOs the list of scope DTO objects, which can be empty or null.
     * @return the equivalent PlanScope, if any scope DTO's were found. Empty otherwise.
     */
    private Optional<PlanScope> getScope(@Nullable final List<BaseApiDTO> scopeDTOs) {
        // convert scope info from BaseApiDTO's to PlanScopeEntry objects
        if (scopeDTOs == null || scopeDTOs.size() == 0) {
            return Optional.empty(); // no scope to convert
        }

        PlanScope.Builder scopeBuilder = PlanScope.newBuilder();
        // add all of the scope entries to the builder
        for (BaseApiDTO scopeDTO : scopeDTOs) {
            // Since all scope entries are additive, if any scope is the Market scope, then this is
            // effectively the same as an unscoped plan, so return the empty scope. In the future,
            // if we support scope reduction entries, this may change.
            if (scopeDTO.getClassName().equalsIgnoreCase(MARKET_PLAN_SCOPE_CLASSNAME)) {
                return Optional.empty();
            }

            long objectId = Long.parseLong(scopeDTO.getUuid());
            scopeBuilder.addScopeEntriesBuilder()
                    .setScopeObjectOid(objectId)
                    .setClassName(scopeDTO.getClassName())
                    .setDisplayName(scopeDTO.getDisplayName());
        }
        // we have a customized scope -- return it
        return Optional.of(scopeBuilder.build());
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
     * Convert {@link RISetting} to {@link SettingApiDTO}.
     *
     * @param ri the RISetting
     * @return a list of SettingApiDTO
     */
    private List<SettingApiDTO> createRiSettingApiDTO(RISetting ri) {
        List<SettingApiDTO> riSettings = new ArrayList<>();

        SettingApiDTO<String> offeringClassDto = new SettingApiDTO<>();
        offeringClassDto.setUuid(AWSPreferredOfferingClass.getSettingName());
        offeringClassDto.setValue(ri.getPreferredOfferingClass().name());
        offeringClassDto.setDisplayName(AWSPreferredOfferingClass.getDisplayName());
        riSettings.add(offeringClassDto);

        SettingApiDTO<String> paymentDto = new SettingApiDTO<>();
        paymentDto.setUuid(AWSPreferredPaymentOption.getSettingName());
        paymentDto.setValue(ri.getPreferredPaymentOption().name());
        paymentDto.setDisplayName(AWSPreferredPaymentOption.getDisplayName());
        riSettings.add(paymentDto);

        SettingApiDTO<String> termDto = new SettingApiDTO<>();
        termDto.setUuid(AWSPreferredTerm.getSettingName());
        Optional<PreferredTerm> term = riTermMapper.valueOf(PreferredTerm.getPrefferedTermEnum(ri.getPreferredTerm()).orElse(null));
        termDto.setValue(term.map(PreferredTerm::name).orElse(null));
        termDto.setDisplayName(AWSPreferredTerm.getDisplayName());
        riSettings.add(termDto);

        if (ri.hasDemandType()) {
            final SettingApiDTO<String> demandTypeDto = new SettingApiDTO<>();
            demandTypeDto.setUuid(RIDemandType.getSettingName());
            demandTypeDto.setValue(ri.getDemandType().name());
            demandTypeDto.setDisplayName(RIDemandType.getDisplayName());
            riSettings.add(demandTypeDto);
        }
        return riSettings;
    }

    @Nonnull
    private ConfigChangesApiDTO buildApiConfigChanges(@Nonnull final List<ScenarioChange> changes, @Nonnull final ScenarioChangeMappingContext mappingContext) {
        final List<SettingApiDTO<String>> settingChanges = changes.stream()
                .filter(ScenarioChange::hasSettingOverride)
                .map(ScenarioChange::getSettingOverride)
                .flatMap(override -> createApiSettingFromOverride(override, mappingContext).stream())
                .collect(Collectors.toList());

        final List<PlanChanges> allPlanChanges = changes.stream()
                .filter(ScenarioChange::hasPlanChanges).map(ScenarioChange::getPlanChanges)
                .collect(Collectors.toList());

        final List<SettingApiDTO> riSetting = changes.stream()
                        .filter(ScenarioChange::hasRiSetting)
                        .map(ScenarioChange::getRiSetting)
                        .flatMap(ri -> createRiSettingApiDTO(ri).stream())
                        .collect(Collectors.toList());

        final List<RemoveConstraintApiDTO> removeConstraintApiDTOS = getRemoveConstraintsDtos(allPlanChanges, mappingContext );

        final ConfigChangesApiDTO outputChanges = new ConfigChangesApiDTO();

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
        // Currently as IgnoreConstraint for all entities is passed as a API parameter(OM-18012),
        // the UI has no way to displayIgnoreConstraint setting for all entities. So we are only
        // converting the IgnoreConstraint if it is set for groups.
        if (constraint.hasIgnoreGroup()) {
            ConstraintGroup constraintGroup = constraint.getIgnoreGroup();
            constraintApiDTO.setConstraintType(
                    ConstraintType.valueOf(constraintGroup.getCommodityType()));
            constraintApiDTO.setTarget(mappingContext.dtoForId(constraintGroup.getGroupUuid()));
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

        final String entityType = UIEntityType.fromType(settingOverride.getEntityType()).apiStr();

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
                    UIEntityType.fromSdkTypeToEntityTypeString(addition.getTargetEntityType()));
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
                    UIEntityType.fromSdkTypeToEntityTypeString(removal.getTargetEntityType()));
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
                    UIEntityType.fromSdkTypeToEntityTypeString(replace.getTargetEntityType()));
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
     * A context object to map {@link ScenarioChange} objects to their API equivalents.
     *
     * Mainly intended to abstract away the details of interacting with other services in order
     * to supply all necessary information to the API (e.g. the names of groups, templates, and
     * entities).
     */
    public static class ScenarioChangeMappingContext {
        private final Map<Long, ServiceEntityApiDTO> serviceEntityMap;
        private final Map<Long, TemplateApiDTO> templatesMap;
        private final Map<Long, GroupApiDTO> groupMap;

        public ScenarioChangeMappingContext(@Nonnull final RepositoryApi repositoryApi,
                                            @Nonnull final TemplatesUtils templatesUtils,
                                            @Nonnull final GroupServiceBlockingStub groupRpcService,
                                            @Nonnull final GroupMapper groupMapper,
                                            @Nonnull final List<ScenarioChange> changes) {
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

            this.groupMap = new HashMap<>();
            final Set<Long> involvedGroups = PlanDTOUtil.getInvolvedGroups(changes);
            if (!involvedGroups.isEmpty()) {
                final Iterator<Grouping> iterator = groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(
                                GroupFilter.newBuilder()
                                    .addAllId(involvedGroups)
                        ).build());
                final List<Grouping> groups = Lists.newArrayList(iterator);
                final Map<Long, GroupApiDTO> apiGroups =
                        groupMapper.groupsToGroupApiDto(groups, false);
                for (Entry<Long, GroupApiDTO> entry : apiGroups.entrySet()) {
                    groupMap.put(entry.getKey(), entry.getValue());
                }
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
            } else if (groupMap.containsKey(id)) {
                return groupMap.get(id);
            } else {
                logger.error("Unable to find entity, template, or group with ID {} when mapping "
                    + "scenario change. Could the object have been removed from the system/topology?",
                    id);
                final BaseApiDTO entity = new BaseApiDTO();
                entity.setUuid(Long.toString(id));
                entity.setDisplayName(UIEntityType.UNKNOWN.apiStr());
                return entity;
            }
        }
    }


}
