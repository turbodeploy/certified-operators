package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.ConfigChangesApiDTO;
import com.vmturbo.api.dto.scenario.LoadChangesApiDTO;
import com.vmturbo.api.dto.scenario.RemoveConstraintApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.dto.scenario.UtilizationApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ConstraintType;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.DetailsCase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

import jdk.nashorn.internal.ir.annotations.Immutable;

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

    /**
     * The string constant the UI uses to identify a plan scoped to the global environment.
     */
    private static final String MARKET_PLAN_SCOPE_CLASSNAME = "Market";

    /**
     * A default market scope that we pass back to the UI when there is no explicit scope defined
     * in a plan.
     */
    private static final BaseApiDTO MARKET_PLAN_SCOPE;

    static {
        MARKET_PLAN_SCOPE = new BaseApiDTO();
        MARKET_PLAN_SCOPE.setUuid(MARKET_PLAN_SCOPE_CLASSNAME);
        MARKET_PLAN_SCOPE.setDisplayName("Global Environment");
        MARKET_PLAN_SCOPE.setClassName(MARKET_PLAN_SCOPE_CLASSNAME);
    }

    private static final Logger logger = LogManager.getLogger();

    private final TemplatesUtils templatesUtils;

    private final RepositoryApi repositoryApi;

    private final SettingsManagerMapping settingsManagerMapping;

    private final SettingsMapper settingsMapper;

    private PoliciesService policiesService;

    public ScenarioMapper(@Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final TemplatesUtils templatesUtils,
                          @Nonnull final SettingsManagerMapping settingsManagerMapping,
                          @Nonnull final SettingsMapper settingsMapper,
                          @Nonnull final PoliciesService policiesService) {

        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.policiesService = Objects.requireNonNull(policiesService);
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
    }

    /**
     * Map a ScenarioApiDTO to an equivalent {@link ScenarioInfo}.
     *
     * @param name The name of the scenario.
     * @param dto The DTO to be converted.
     * @return The ScenarioInfo equivalent of the input DTO.
     */
    @Nonnull
    public ScenarioInfo toScenarioInfo(final String name,
                                       @Nonnull final ScenarioApiDTO dto) {
        final ScenarioInfo.Builder infoBuilder = ScenarioInfo.newBuilder();
        if (name != null) {
            infoBuilder.setName(name);
        }

        // TODO: Right now, API send template id and entity id together for Topology Addition, and it
        // doesn't have a field to tell whether it is template id or entity id. Later on,
        // API should tell us if it is template id or not, please see (OM-26675).
        final Set<Long> templateIds = getTemplatesIds(dto.getTopologyChanges());

        infoBuilder.addAllChanges(getTopologyChanges(dto.getTopologyChanges(), templateIds));
        infoBuilder.addAllChanges(getConfigChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getPolicyChanges(dto.getConfigChanges()));
        infoBuilder.addAllChanges(getLoadChanges(dto.getLoadChanges()));
        getScope(dto.getScope()).ifPresent(infoBuilder::setScope);
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config

        if (dto.getType() != null) {
            infoBuilder.setType(dto.getType());
        }

        return infoBuilder.build();
    }

    @Nonnull
    private Iterable<ScenarioChange> getLoadChanges(@Nonnull LoadChangesApiDTO loadChangesApiDTO) {
        if (loadChangesApiDTO == null) {
            return Collections.emptyList();
        }
        ImmutableList.Builder<ScenarioChange> scenariChanges = ImmutableList.builder();
        final List<UtilizationApiDTO> utilizationList = loadChangesApiDTO.getUtilizationList();
        if (!CollectionUtils.isEmpty(utilizationList)) {
            final List<UtilizationLevel> utilizationLevels = utilizationList.stream().map(util ->
                    UtilizationLevel.newBuilder().setPercentage(util.getPercentage()).build())
                    .collect(Collectors.toList());
            final ScenarioChange change = ScenarioChange.newBuilder().setPlanChanges(
                    PlanChanges.newBuilder().addAllUtilizationLevel(utilizationLevels).build()
            ).build();
            scenariChanges.add(change);
        }
        return scenariChanges.build();
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
        dto.setUuid(Long.toString(scenario.getId()));
        dto.setDisplayName(scenario.getScenarioInfo().getName());
        if (scenario.getScenarioInfo().hasType()) {
            dto.setType(scenario.getScenarioInfo().getType());
        } else {
            dto.setType(CUSTOM_SCENARIO_TYPE);
        }

        dto.setScope(buildApiScopeObjects(scenario));
        dto.setProjectionDays(buildApiProjChanges());
        dto.setTopologyChanges(buildApiTopologyChanges(scenario.getScenarioInfo().getChangesList()));
        dto.setConfigChanges(buildApiConfigChanges(scenario.getScenarioInfo().getChangesList()));
        dto.setLoadChanges(buildLoadChangesApiDTO(scenario.getScenarioInfo().getChangesList()));
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config
        return dto;
    }

    @Nonnull
    private LoadChangesApiDTO buildLoadChangesApiDTO(@Nonnull List<ScenarioChange> changes) {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        if (CollectionUtils.isEmpty(changes)) {
            return loadChanges;
        }
        final Stream<UtilizationLevel> utilizationLevels = changes.stream()
                .filter(this::scenarioChangeHasUtilizationLevel)
                .map(ScenarioChange::getPlanChanges)
                .map(PlanChanges::getUtilizationLevelList)
                .flatMap(List::stream);

        final List<UtilizationApiDTO> utilizationApiDTOS = utilizationLevels
                .map(this::createUtilizationApiDto).collect(Collectors.toList());
        loadChanges.setUtilizationList(utilizationApiDTOS);
        return loadChanges;
    }

    @Nonnull
    private UtilizationApiDTO createUtilizationApiDto(@Nonnull UtilizationLevel utilizationLevel) {
        final UtilizationApiDTO utilizationDTO = new UtilizationApiDTO();
        utilizationDTO.setPercentage(utilizationLevel.getPercentage());
        return utilizationDTO;
    }

    private boolean scenarioChangeHasUtilizationLevel(@Nonnull ScenarioChange change) {
        return change.hasPlanChanges() &&
                !CollectionUtils.isEmpty(change.getPlanChanges().getUtilizationLevelList());
    }

    @Nonnull
    private List<ScenarioChange> buildSettingChanges(@Nullable final List<SettingApiDTO> settingsList) {
        if (CollectionUtils.isEmpty(settingsList)) {
            return Collections.emptyList();
        }

        // First we convert them back to "real" settings.
        final List<SettingApiDTO> convertedSettingOverrides =
                settingsManagerMapping.convertFromPlanSetting(settingsList);
        final Map<String, Setting> settingProtoOverrides =
                settingsMapper.toProtoSettings(convertedSettingOverrides);

        final ImmutableList.Builder<ScenarioChange> retChanges = ImmutableList.builder();
        convertedSettingOverrides.forEach(apiDto -> {
            Setting protoSetting = settingProtoOverrides.get(apiDto.getUuid());
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
                            ServiceEntityMapper.fromUIEntityType(apiDto.getEntityType()));
                }
                retChanges.add(ScenarioChange.newBuilder()
                        .setSettingOverride(settingOverride)
                        .build());
            }
        });

        return retChanges.build();
    }

    @Nonnull
    private List<ScenarioChange> getConfigChanges(@Nullable final ConfigChangesApiDTO configChanges) {
        if (configChanges == null) {
            return Collections.emptyList();
        }

        final ImmutableList.Builder<ScenarioChange> scenarioChanges = ImmutableList.builder();

        scenarioChanges.addAll(buildSettingChanges(configChanges.getAutomationSettingList()));
        if (!CollectionUtils.isEmpty(configChanges.getRemoveConstraintList())) {
            scenarioChanges.add(buildPlanChanges(configChanges));
        }

        return scenarioChanges.build();
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
        return IgnoreConstraint.newBuilder()
                .setCommodityType(constraint.getConstraintType().name())
                .setGroupUuid(Long.parseLong(constraint.getTarget().getUuid())).build();
    }

    @Nonnull
    private static List<ScenarioChange> getTopologyChanges(final TopologyChangesApiDTO topoChanges,
                                           @Nonnull final Set<Long> templateIds) {
        if (topoChanges == null) {
            return Collections.emptyList();
        }

        final ImmutableList.Builder<ScenarioChange> changes = ImmutableList.builder();

        CollectionUtils.emptyIfNull(topoChanges.getAddList())
            .forEach(change -> changes.add(mapTopologyAddition(change, templateIds)));

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

    private static ScenarioChange mapTopologyAddition(@Nonnull final AddObjectApiDTO change,
                                            @Nonnull final Set<Long> templateIds) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology additions must contain a target");
        // Default count to 1
        int count = change.getCount() != null ? change.getCount() : 1;

        final Long uuid = Long.parseLong(change.getTarget().getUuid());

        final TopologyAddition.Builder additionBuilder = TopologyAddition.newBuilder()
            .addAllChangeApplicationDays(projectionDays(change.getProjectionDays()))
            .setAdditionCount(count);
        if (templateIds.contains(uuid)) {
            additionBuilder.setTemplateId(uuid);
        } else {
            additionBuilder.setEntityId(uuid);
        }

        return ScenarioChange.newBuilder()
            .setTopologyAddition(additionBuilder)
            .build();
    }

    private static ScenarioChange mapTopologyRemoval(@Nonnull final RemoveObjectApiDTO change) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology removals must contain a target");

        return ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder()
                .setChangeApplicationDay(projectionDay(change.getProjectionDay()))
                .setEntityId(Long.parseLong(change.getTarget().getUuid())))
            .build();
    }

    private static ScenarioChange mapTopologyReplace(@Nonnull final ReplaceObjectApiDTO change) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology replace must contain a target");
        Preconditions.checkArgument(change.getTemplate() != null,
                "Topology replace must contain a template");

        return ScenarioChange.newBuilder()
            .setTopologyReplace(TopologyReplace.newBuilder()
                .setChangeApplicationDay(projectionDay(change.getProjectionDay()))
                .setAddTemplateId(Long.parseLong(change.getTemplate().getUuid()))
                .setRemoveEntityId(Long.parseLong(change.getTarget().getUuid())))
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

    @Nonnull
    private ConfigChangesApiDTO buildApiConfigChanges(@Nonnull final List<ScenarioChange> changes) {
        final List<SettingApiDTO> settingChanges = changes.stream()
                .filter(ScenarioChange::hasSettingOverride)
                .map(ScenarioChange::getSettingOverride)
                .map(this::createApiSettingFromOverride)
                .collect(Collectors.toList());

        final List<PlanChanges> allPlanChanges = changes.stream()
                .filter(ScenarioChange::hasPlanChanges).map(ScenarioChange::getPlanChanges)
                .collect(Collectors.toList());

        final List<RemoveConstraintApiDTO> removeConstraintApiDTOS = getRemoveConstraintsDtos(allPlanChanges);

        final ConfigChangesApiDTO outputChanges = new ConfigChangesApiDTO();

        outputChanges.setRemoveConstraintList(removeConstraintApiDTOS);
        outputChanges.setAutomationSettingList(settingsManagerMapping
                .convertToPlanSetting(settingChanges));
        outputChanges.setAddPolicyList(Lists.newArrayList());
        outputChanges.setRemovePolicyList(Lists.newArrayList());
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
    private List<RemoveConstraintApiDTO> getRemoveConstraintsDtos(final List<PlanChanges> allPlanChanges) {
        return allPlanChanges.stream().filter(planChanges ->
                !CollectionUtils.isEmpty(planChanges.getIgnoreConstraintsList()))
                .map(PlanChanges::getIgnoreConstraintsList).flatMap(List::stream)
                .map(this::toRemoveConstraintApiDTO).collect(Collectors.toList());
    }

    @Nonnull
    private RemoveConstraintApiDTO toRemoveConstraintApiDTO(@Nonnull IgnoreConstraint constraint) {
        final RemoveConstraintApiDTO constraintApiDTO = new RemoveConstraintApiDTO();
        constraintApiDTO.setConstraintType(
                ConstraintType.valueOf(constraint.getCommodityType()));
        final BaseApiDTO targetGroup = new BaseApiDTO();
        targetGroup.setUuid(Long.toString(constraint.getGroupUuid()));
        constraintApiDTO.setTarget(targetGroup);
        return constraintApiDTO;
    }

    @Nonnull
    private SettingApiDTO createApiSettingFromOverride(@Nonnull final SettingOverride settingOverride) {
        final SettingApiDTO apiDTO =
                SettingsMapper.toSettingApiDto(settingOverride.getSetting());
        if (settingOverride.hasEntityType()) {
            apiDTO.setEntityType(
                    ServiceEntityMapper.toUIEntityType(settingOverride.getEntityType()));
        }
        return apiDTO;
    }

    @Nonnull
    private TopologyChangesApiDTO buildApiTopologyChanges(
            @Nonnull final List<ScenarioChange> changes) {
        // Get type information about entities involved in the scenario changes. We get it
        // in a single call to reduce the number of round-trips and total wait-time.
        //
        // Here we are retrieving the entire ServiceEntity DTO from the repository.
        // This shouldn't be an issue as long as scenarios don't contain absurd numbers of entities,
        // and as long as we're not retrieving detailed information about lots of scenarios.
        // If necessary we can optimize it by exposing an API call that returns only entity types.
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.getServiceEntitiesById(
                    ServiceEntitiesRequest.newBuilder(PlanDTOUtil.getInvolvedEntities(changes))
                            .build())
                .entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                // The .get() here is safe because we filtered out entries where the entity
                // information is not present.
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        // Get all involved templates
        final Map<Long, TemplateApiDTO> templatesMap =
            templatesUtils.getTemplatesMapByIds(PlanDTOUtil.getInvolvedTemplates(changes));

        final TopologyChangesApiDTO outputChanges = new TopologyChangesApiDTO();
        changes.forEach(change -> {
            switch (change.getDetailsCase()) {
                case TOPOLOGY_ADDITION:
                    buildApiTopologyAddition(change.getTopologyAddition(),
                            outputChanges, serviceEntityMap, templatesMap);
                    break;
                case TOPOLOGY_REMOVAL:
                    buildApiTopologyRemoval(change.getTopologyRemoval(),
                            outputChanges, serviceEntityMap, templatesMap);
                    break;
                case TOPOLOGY_REPLACE:
                    buildApiTopologyReplace(change.getTopologyReplace(),
                            outputChanges, serviceEntityMap, templatesMap);
                    break;
                default:
            }
        });

        return outputChanges;
    }

    private static void buildApiTopologyAddition(@Nonnull final TopologyAddition addition,
                                                 @Nonnull final TopologyChangesApiDTO outputChanges,
                                                 @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
                                                 @Nonnull final Map<Long, TemplateApiDTO> templateMap) {
        AddObjectApiDTO changeApiDTO = new AddObjectApiDTO();
        changeApiDTO.setCount(addition.getAdditionCount());
        final long uuid = addition.hasTemplateId() ? addition.getTemplateId() : addition.getEntityId();
        changeApiDTO.setTarget(dtoForId(uuid, serviceEntityMap, templateMap));
        changeApiDTO.setProjectionDays(addition.getChangeApplicationDaysList());

        List<AddObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getAddList(),
            new ArrayList<>());
        changeApiDTOs.add(changeApiDTO);
        outputChanges.setAddList(changeApiDTOs);
    }

    private static void buildApiTopologyRemoval(@Nonnull final TopologyRemoval removal,
                                                @Nonnull final TopologyChangesApiDTO outputChanges,
                                                @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
                                                @Nonnull final Map<Long, TemplateApiDTO> templateMap) {
        List<RemoveObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getRemoveList(),
                new ArrayList<>());
        RemoveObjectApiDTO changeApiDTO = new RemoveObjectApiDTO();
        changeApiDTO.setTarget(dtoForId(removal.getEntityId(), serviceEntityMap, templateMap));
        changeApiDTO.setProjectionDay(removal.getChangeApplicationDay());

        changeApiDTOs.add(changeApiDTO);
        outputChanges.setRemoveList(changeApiDTOs);
    }

    private static void buildApiTopologyReplace(@Nonnull final TopologyReplace replace,
                                                @Nonnull final TopologyChangesApiDTO outputChanges,
                                                @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
                                                @Nonnull final Map<Long, TemplateApiDTO> templateMap) {
        ReplaceObjectApiDTO changeApiDTO = new ReplaceObjectApiDTO();
        changeApiDTO.setTarget(dtoForId(replace.getRemoveEntityId(), serviceEntityMap, templateMap));
        changeApiDTO.setTemplate(dtoForId(replace.getAddTemplateId(), serviceEntityMap, templateMap));
        changeApiDTO.setProjectionDay(replace.getChangeApplicationDay());

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
     * This function is used to build {@link BaseApiDTO}. And it passed into Service Entity Map and Template
     * Map to check if id is entity or template.
     *
     * @param id uuid for BaseApiDTO.
     * @param serviceEntityMap Map contains all involved service entity.
     * @param templateMap Map contains all involved templates.
     * @return {@link BaseApiDTO}.
     */
    // TODO: Handle groups.
    private static BaseApiDTO dtoForId(long id,
                                       @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
                                       @Nonnull final Map<Long, TemplateApiDTO> templateMap) {
        BaseApiDTO entity = new BaseApiDTO();
        Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional.ofNullable(serviceEntityMap.get(id));
        Optional<TemplateApiDTO> templateApiDTO = Optional.ofNullable(templateMap.get(id));
        // First we check if id is service entity id, if not, then check if it is template id. Otherwise
        // we set it is UNKNOWN.
        entity.setClassName(serviceEntityApiDTO.map(ServiceEntityApiDTO::getClassName)
                .orElse(templateApiDTO.map(TemplateApiDTO::getClassName)
                    .orElse(ServiceEntityMapper.toUIEntityType(EntityType.UNKNOWN.getNumber()))));
        entity.setDisplayName(serviceEntityApiDTO.map(ServiceEntityApiDTO::getDisplayName)
                .orElse(templateApiDTO.map(TemplateApiDTO::getDisplayName)
                    .orElse(ServiceEntityMapper.toUIEntityType(EntityType.UNKNOWN.getNumber()))));
        entity.setUuid(Long.toString(id));

        return entity;
    }
}
