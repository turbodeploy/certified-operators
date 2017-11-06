package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.MigrateObjectApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Maps scenarios between their API DTO representation and their protobuf representation.
 */
public class ScenarioMapper {
    /**
     * The string constant expected by the UI for a custom scenario type.
     * Used as a fallback if a previously saved scenario doesn't have a set type for whatever
     * reason.
     */
    private static final String CUSTOM_SCENARIO_TYPE = "CUSTOM";

    private static final Logger logger = LogManager.getLogger();

    private RepositoryApi repositoryApi;

    private TemplatesUtils templatesUtils;

    public ScenarioMapper(@Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final TemplatesUtils templatesUtils) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
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
        ScenarioInfo.Builder infoBuilder = ScenarioInfo.newBuilder();
        if (name != null) {
            infoBuilder.setName(name);
        }

        // TODO: Right now, API send template id and entity id together for Topology Addition, and it
        // doesn't have a field to tell whether it is template id or entity id. Later on,
        // API should tell us if it is template id or not, please see (OM-26675).
        final Set<Long> templatesId = getTemplatesIds(dto.getTopologyChanges());
        addTopologyChanges(dto.getTopologyChanges(), infoBuilder, templatesId);
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config

        if (dto.getType() != null) {
            infoBuilder.setType(dto.getType());
        }

        return infoBuilder.build();
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

        dto.setScope(buildApiScopeChanges());
        dto.setProjectionDays(buildApiProjChanges());
        dto.setTopologyChanges(buildApiTopologyChanges(scenario.getScenarioInfo().getChangesList()));
        // TODO (gabriele, Oct 27 2017) We need to extend the Plan Orchestrator with support
        // for the other types of changes: time based topology, load and config

        return dto;
    }

    private static void addTopologyChanges(final TopologyChangesApiDTO topoChanges,
                                           @Nonnull final ScenarioInfo.Builder infoBuilder,
                                           @Nonnull final Set<Long> templatesId) {
        // build Topology changes
        if (topoChanges != null) {
            List<AddObjectApiDTO> addTopoChanges = topoChanges.getAddList();
            List<RemoveObjectApiDTO> removeTopoChanges = topoChanges.getRemoveList();
            List<ReplaceObjectApiDTO> replaceTopoChanges = topoChanges.getReplaceList();
            List<MigrateObjectApiDTO> migrateTopoChanges = topoChanges.getMigrateList();

            if (addTopoChanges != null) {
                addTopoChanges.forEach(change -> {
                    addTopologyAddition(change, infoBuilder, templatesId);
                });
            }

            if (removeTopoChanges != null) {
                removeTopoChanges.forEach(change -> {
                    addTopologyRemoval(change, infoBuilder);
                });
            }

            if (replaceTopoChanges != null) {
                replaceTopoChanges.forEach(change -> {
                    addTopologyReplace(change, infoBuilder);
                });
            }

            if (migrateTopoChanges != null) {
                migrateTopoChanges.forEach(change -> {
                    logger.info("Skipping migration change of type Migrate: Not supported");
                });
            }
        }
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

    private static void addTopologyAddition(@Nonnull final AddObjectApiDTO change,
                                            @Nonnull final ScenarioInfo.Builder infoBuilder,
                                            @Nonnull final Set<Long> templateIds) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology additions must contain a target");
        // Default count to 1
        int count = change.getCount() != null? change.getCount() : 1;

        final Long uuid = Long.parseLong(change.getTarget().getUuid());

        final TopologyAddition.Builder additionBuilder = TopologyAddition.newBuilder()
            .addAllChangeApplicationDays(projectionDays(change.getProjectionDays()))
            .setAdditionCount(count);
        if (templateIds.contains(uuid)) {
            additionBuilder.setTemplateId(uuid);
        }
        else {
            additionBuilder.setEntityId(uuid);
        }

        ScenarioChange.Builder changeBuilder = ScenarioChange.newBuilder();
        infoBuilder.addChanges(changeBuilder.setTopologyAddition(additionBuilder.build()));
    }

    private static void addTopologyRemoval(@Nonnull final RemoveObjectApiDTO change,
                                           @Nonnull final ScenarioInfo.Builder infoBuilder) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology removals must contain a target");

        ScenarioChange.Builder changeBuilder = ScenarioChange.newBuilder();
        infoBuilder.addChanges(changeBuilder.setTopologyRemoval(
            TopologyRemoval.newBuilder()
                .setChangeApplicationDay(projectionDay(change.getProjectionDay()))
                .setEntityId(Long.parseLong(change.getTarget().getUuid()))
                .build()));
    }

    private static void addTopologyReplace(@Nonnull final ReplaceObjectApiDTO change,
                                           @Nonnull final ScenarioInfo.Builder infoBuilder) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology replace must contain a target");
        Preconditions.checkArgument(change.getTemplate() != null,
                "Topology replace must contain a template");

        ScenarioChange.Builder changeBuilder = ScenarioChange.newBuilder();
        infoBuilder.addChanges(changeBuilder.setTopologyReplace(
            TopologyReplace.newBuilder()
                .setChangeApplicationDay(projectionDay(change.getProjectionDay()))
                .setAddTemplateId(Long.parseLong(change.getTemplate().getUuid()))
                .setRemoveEntityId(Long.parseLong(change.getTarget().getUuid()))
                .build()));
    }

    private List<BaseApiDTO> buildApiScopeChanges() {
        // --- START HAX --- Tracking Issue: OM-14951
        // TODO (roman, Jan 20 2017): We need to extend the Plan Orchestrator with support
        // for scope changes, and do the appropriate conversion.
        // As part of the effort to get the plan UI functional, hard-coding the default
        // scope changes used by the UI here.
        BaseApiDTO scopeDto = new BaseApiDTO();
        scopeDto.setUuid("Market");
        scopeDto.setDisplayName("Global Environment");
        scopeDto.setClassName("Market");

        return Collections.singletonList(scopeDto);
        // --- END HAX ---
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
    private TopologyChangesApiDTO buildApiTopologyChanges(
            @Nonnull final List<ScenarioChange> changes) {
        final TopologyChangesApiDTO outputChanges = new TopologyChangesApiDTO();

        // Get type information about entities involved in the scenario changes. We get it
        // in a single call to reduce the number of round-trips and total wait-time.
        //
        // Here we are retrieving the entire ServiceEntity DTO from the repository.
        // This shouldn't be an issue as long as scenarios don't contain absurd numbers of entities,
        // and as long as we're not retrieving detailed information about lots of scenarios.
        // If necessary we can optimize it by exposing an API call that returns only entity types.
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.getServiceEntitiesById(PlanDTOUtil.getInvolvedEntities(changes))
                .entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                // The .get() here is safe because we filtered out entries where the entity
                // information is not present.
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        // Get all involved templates
        final Map<Long, TemplateApiDTO> templatesMap =
            templatesUtils.getTemplatesMapByIds(PlanDTOUtil.getInvolvedTemplates(changes));

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
                    // Do nothing
                    logger.info("Unknown change type: " + change.getDetailsCase());
            }
        });

        return outputChanges;
    }

    private static void buildApiTopologyAddition(@Nonnull final TopologyAddition addition,
                                                 @Nonnull final TopologyChangesApiDTO outputChanges,
                                                 @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
                                                 @Nonnull final Map<Long, TemplateApiDTO> templateMap) {
        List<AddObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getAddList(),
                new ArrayList<>());
        AddObjectApiDTO changeApiDTO = new AddObjectApiDTO();
        changeApiDTO.setCount(addition.getAdditionCount());
        final long uuid = addition.hasTemplateId() ? addition.getTemplateId() : addition.getEntityId();
        changeApiDTO.setTarget(dtoForId(uuid, serviceEntityMap, templateMap));
        changeApiDTO.setProjectionDays(addition.getChangeApplicationDaysList());

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
        List<ReplaceObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getReplaceList(),
                new ArrayList<>());
        ReplaceObjectApiDTO changeApiDTO = new ReplaceObjectApiDTO();
        changeApiDTO.setTarget(dtoForId(replace.getRemoveEntityId(), serviceEntityMap, templateMap));
        changeApiDTO.setTemplate(dtoForId(replace.getAddTemplateId(), serviceEntityMap, templateMap));
        changeApiDTO.setProjectionDay(replace.getChangeApplicationDay());

        changeApiDTOs.add(changeApiDTO);
        outputChanges.setReplaceList(changeApiDTOs);
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
