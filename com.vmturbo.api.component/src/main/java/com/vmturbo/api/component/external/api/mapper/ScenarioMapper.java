package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.MigrateObjectApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.enums.PlanChangeType.PlanModificationState;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
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

    public ScenarioMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
    }

    /**
     * Map a ScenarioApiDTO to an equivalent {@link ScenarioInfo}.
     *
     * @param name The name of the scenario.
     * @param dto The DTO to be converted.
     * @return The ScenarioInfo equivalent of the input DTO.
     */
    @Nonnull
    public static ScenarioInfo toScenarioInfo(final String name,
                                              @Nonnull final ScenarioApiDTO dto) {
        ScenarioInfo.Builder infoBuilder = ScenarioInfo.newBuilder();
        if (name != null) {
            infoBuilder.setName(name);
        }

        addTopologyChanges(dto.getTopologyChanges(), infoBuilder);
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
                                   @Nonnull final ScenarioInfo.Builder infoBuilder) {
        // build Topology changes
        if (topoChanges != null) {
            List<AddObjectApiDTO> addTopoChanges = topoChanges.getAddList();
            List<RemoveObjectApiDTO> removeTopoChanges = topoChanges.getRemoveList();
            List<ReplaceObjectApiDTO> replaceTopoChanges = topoChanges.getReplaceList();
            List<MigrateObjectApiDTO> migrateTopoChanges = topoChanges.getMigrateList();

            if (addTopoChanges != null) {
                addTopoChanges.forEach(change -> {
                    addTopologyAddition(change, infoBuilder);
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

    private static int projectionDay(@Nonnull final Integer projDay) {
        return projDay == null ? 0 : projDay;
    }

    private static Collection<Integer> projectionDays(@Nonnull final List<Integer> projDays) {
        return projDays == null ? Collections.emptyList() : projDays;
    }

    private static void addTopologyAddition(@Nonnull final AddObjectApiDTO change,
                                            @Nonnull final ScenarioInfo.Builder infoBuilder) {
        Preconditions.checkArgument(change.getTarget() != null,
                "Topology additions must contain a target");
        // Default count to 1
        int count = change.getCount() != null? change.getCount() : 1;

        ScenarioChange.Builder changeBuilder = ScenarioChange.newBuilder();
        infoBuilder.addChanges(changeBuilder.setTopologyAddition(
            TopologyAddition.newBuilder()
                .addAllChangeApplicationDays(projectionDays(change.getProjectionDays()))
                .setAdditionCount(count)
                .setEntityId(Long.parseLong(change.getTarget().getUuid()))
                .build()));
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

        changes.forEach(change -> {
            switch (change.getDetailsCase()) {
                case TOPOLOGY_ADDITION:
                    buildApiTopologyAddition(change.getTopologyAddition(),
                            outputChanges, serviceEntityMap);
                    break;
                case TOPOLOGY_REMOVAL:
                    buildApiTopologyRemoval(change.getTopologyRemoval(),
                            outputChanges, serviceEntityMap);
                    break;
                case TOPOLOGY_REPLACE:
                    buildApiTopologyReplace(change.getTopologyReplace(),
                            outputChanges, serviceEntityMap);
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
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        List<AddObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getAddList(),
                new ArrayList<>());
        AddObjectApiDTO changeApiDTO = new AddObjectApiDTO();
        changeApiDTO.setCount(addition.getAdditionCount());
        changeApiDTO.setTarget(dtoForId(addition.getEntityId(), serviceEntityMap));
        changeApiDTO.setProjectionDays(addition.getChangeApplicationDaysList());

        changeApiDTOs.add(changeApiDTO);
        outputChanges.setAddList(changeApiDTOs);
    }

    private static void buildApiTopologyRemoval(@Nonnull final TopologyRemoval removal,
                                        @Nonnull final TopologyChangesApiDTO outputChanges,
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        List<RemoveObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getRemoveList(),
                new ArrayList<>());
        RemoveObjectApiDTO changeApiDTO = new RemoveObjectApiDTO();
        changeApiDTO.setTarget(dtoForId(removal.getEntityId(), serviceEntityMap));
        changeApiDTO.setProjectionDay(removal.getChangeApplicationDay());

        changeApiDTOs.add(changeApiDTO);
        outputChanges.setRemoveList(changeApiDTOs);
    }

    private static void buildApiTopologyReplace(@Nonnull final TopologyReplace replace,
                                        @Nonnull final TopologyChangesApiDTO outputChanges,
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        List<ReplaceObjectApiDTO> changeApiDTOs = MoreObjects.firstNonNull(outputChanges.getReplaceList(),
                new ArrayList<>());
        ReplaceObjectApiDTO changeApiDTO = new ReplaceObjectApiDTO();
        changeApiDTO.setTarget(dtoForId(replace.getRemoveEntityId(), serviceEntityMap));
        changeApiDTO.setTemplate(dtoForId(replace.getAddTemplateId(), serviceEntityMap));
        changeApiDTO.setProjectionDay(replace.getChangeApplicationDay());

        changeApiDTOs.add(changeApiDTO);
        outputChanges.setReplaceList(changeApiDTOs);
    }

    // TODO: Handle more than just entities (ie templates and groups).
    private static BaseApiDTO dtoForId(long id, @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        BaseApiDTO entity = new BaseApiDTO();
        Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional.ofNullable(serviceEntityMap.get(id));
        entity.setClassName(serviceEntityApiDTO.map(ServiceEntityApiDTO::getClassName)
                .orElse(ServiceEntityMapper.toUIEntityType(EntityType.UNKNOWN.getNumber())));
        entity.setDisplayName(serviceEntityApiDTO.map(ServiceEntityApiDTO::getDisplayName)
                .orElse(ServiceEntityMapper.toUIEntityType(EntityType.UNKNOWN.getNumber())));
        entity.setUuid(Long.toString(id));

        return entity;
    }
}
