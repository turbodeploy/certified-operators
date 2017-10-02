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

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.ScenarioApiDTO;
import com.vmturbo.api.dto.ScenarioChangeApiDTO;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.dto.input.ScenarioApiInputDTO;
import com.vmturbo.api.enums.PlanChangeType.PlanModificationState;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.Builder;
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
     * Map a ScenarioApiInputDTO to an equivalent {@link ScenarioInfo}.
     *
     * @param name The name of the scenario.
     * @param dto The DTO to be converted.
     * @return The ScenarioInfo equivalent of the input DTO.
     */
    @Nonnull
    public static ScenarioInfo toScenarioInfo(final String name,
                                              @Nonnull final ScenarioApiInputDTO dto) {
        ScenarioInfo.Builder infoBuilder = ScenarioInfo.newBuilder();
        if (name != null) {
            infoBuilder.setName(name);
        }

        if (dto.getChanges() != null) {
            dto.getChanges().forEach(change -> {
                ScenarioChange.Builder changeBuilder = ScenarioChange.newBuilder();
                if (change.getDescription() != null) {
                    changeBuilder.setDescription(change.getDescription());
                }
                addChanges(change, changeBuilder, infoBuilder);
            });
        }

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

        dto.setChanges(buildApiChanges(scenario.getScenarioInfo().getChangesList()));

        return dto;
    }

    private static void addChanges(@Nonnull final ScenarioChangeApiDTO change,
                                   @Nonnull final Builder changeBuilder,
                                   @Nonnull final ScenarioInfo.Builder infoBuilder) {
        try {
            switch (PlanModificationState.valueOf(change.getType())) {
                case ADDED:
                    addTopologyAddition(change, changeBuilder, infoBuilder);
                    break;
                case REMOVED:
                    addTopologyRemoval(change, changeBuilder, infoBuilder);
                    break;
                case REPLACED:
                    addTopologyReplace(change, changeBuilder, infoBuilder);
                    break;
                default:
                    // All other cases unhandled at the moment.
                    logger.info("Skipping change of type " + change.getType());
                    break;
            }
        } catch (IllegalArgumentException e) {
            logger.info("Skipping change of type " + change.getType());
        } catch (RuntimeException e) {
            logger.error("Failed to parse change " + change);
        }
    }

    private static Collection<Integer> projectionDays(@Nonnull final ScenarioChangeApiDTO change) {
        return change.getProjectionDays() == null ?
            Collections.emptyList() :
            change.getProjectionDays();
    }

    private static void addTopologyAddition(@Nonnull final ScenarioChangeApiDTO change,
                                            @Nonnull final ScenarioChange.Builder changeBuilder,
                                            @Nonnull final ScenarioInfo.Builder infoBuilder) {
        if (change.getTargets().size() != 1) {
            throw new IllegalArgumentException(
                    "Topology additions must contain exactly one target. Change was: " + change);
        }

        infoBuilder.addChanges(changeBuilder.setTopologyAddition(
            TopologyAddition.newBuilder()
                .addAllChangeApplicationDays(projectionDays(change))
                .setAdditionCount(Integer.parseInt(change.getValue()))
                .setEntityId(Long.parseLong(change.getTargets().get(0).getUuid()))
                .build()));
    }

    private static void addTopologyRemoval(@Nonnull final ScenarioChangeApiDTO change,
                                           @Nonnull final ScenarioChange.Builder changeBuilder,
                                           @Nonnull final ScenarioInfo.Builder infoBuilder) {
        if (change.getTargets().size() != 1) {
            throw new IllegalArgumentException("Topology removals must contain exactly one target." +
                    " Change was: " + change);
        }

        infoBuilder.addChanges(changeBuilder.setTopologyRemoval(
            TopologyRemoval.newBuilder()
                .addAllChangeApplicationDays(projectionDays(change))
                .setEntityId(Long.parseLong(change.getTargets().get(0).getUuid()))
                .build()));
    }

    private static void addTopologyReplace(@Nonnull final ScenarioChangeApiDTO change,
                                           @Nonnull final ScenarioChange.Builder changeBuilder,
                                           @Nonnull final ScenarioInfo.Builder infoBuilder) {
        List<BaseApiDTO> targets = change.getTargets();
        if (targets.size() != 2) {
            throw new IllegalArgumentException("Topology replacements must contain exactly two " +
                    "targets. Change was: " + change);
        }

        infoBuilder.addChanges(changeBuilder.setTopologyReplace(
            TopologyReplace.newBuilder()
                .addAllChangeApplicationDays(projectionDays(change))
                .setAddTemplateId(Long.parseLong(targets.get(0).getUuid()))
                .setRemoveEntityId(Long.parseLong(targets.get(1).getUuid()))
                .build()));
    }

    @Nonnull
    private List<ScenarioChangeApiDTO> buildApiChanges(
            @Nonnull final List<ScenarioChange> changes) {
        List<ScenarioChangeApiDTO> outputChanges = new ArrayList<>(changes.size());

        // Get type information about entities involved in the scenario changes. We get it
        // in a single call to reduce the number of round-trips and total wait-time.
        //
        // Here we are retrieving the entire ServiceEntity DTO from the repository.
        // This shouldn't be an issue as long as scenarios don't contain absurd numbers of entities,
        // and as long as we're not retrieving detailed information about lots of scenarios.
        // If necessary we can optimize it by exposing an API call that returns only entity types.
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.getServiceEntitiesById(PlanDTOUtil.getInvolvedEntities(changes), false)
                .entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                // The .get() here is safe because we filtered out entries where the entity
                // information is not present.
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));

        // --- START HAX --- Tracking Issue: OM-14951
        // TODO (roman, Jan 20 2017): We need to extend the Plan Orchestrator with support
        // for scope and projection period changes, and do the appropriate conversion.
        // As part of the effort to get the plan UI functional, hard-coding the default
        // scope + projection changes used by the UI here.
        ScenarioChangeApiDTO scopeChange = new ScenarioChangeApiDTO();
        scopeChange.setType("SCOPE");
        BaseApiDTO scopeDto = new BaseApiDTO();
        scopeDto.setUuid("Market");
        scopeDto.setDisplayName("Global Environment");
        scopeDto.setClassName("Market");
        scopeChange.setScope(Collections.singletonList(scopeDto));

        ScenarioChangeApiDTO projectionChange = new ScenarioChangeApiDTO();
        projectionChange.setType("PROJECTION_PERIODS");
        projectionChange.setProjectionDays(Collections.singletonList(0));

        outputChanges.add(scopeChange);
        outputChanges.add(projectionChange);
        // --- END HAX ---

        changes.forEach(change -> {
            ScenarioChangeApiDTO changeApiDTO = new ScenarioChangeApiDTO();
            changeApiDTO.setDescription(change.getDescription());

            switch (change.getDetailsCase()) {
                case TOPOLOGY_ADDITION:
                    buildApiTopologyAddition(change.getTopologyAddition(),
                            changeApiDTO, serviceEntityMap);
                    break;
                case TOPOLOGY_REMOVAL:
                    buildApiTopologyRemoval(change.getTopologyRemoval(),
                            changeApiDTO, serviceEntityMap);
                    break;
                case TOPOLOGY_REPLACE:
                    buildApiTopologyReplace(change.getTopologyReplace(),
                            changeApiDTO, serviceEntityMap);
                    break;
                default:
                    // Do nothing
                    changeApiDTO = null;
                    logger.info("Unknown change type: " + change.getDetailsCase());
            }

            if (changeApiDTO != null) {
                outputChanges.add(changeApiDTO);
            }
        });

        return outputChanges;
    }

    private static void buildApiTopologyAddition(@Nonnull final TopologyAddition addition,
                                        @Nonnull final ScenarioChangeApiDTO changeApiDTO,
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        changeApiDTO.setType(PlanModificationState.ADDED.name());
        changeApiDTO.setValue(Integer.toString(addition.getAdditionCount()));
        changeApiDTO.setTargets(Collections.singletonList(
                dtoForId(addition.getEntityId(), serviceEntityMap)));
        changeApiDTO.setProjectionDays(addition.getChangeApplicationDaysList());
    }

    private static void buildApiTopologyRemoval(@Nonnull final TopologyRemoval removal,
                                        @Nonnull final ScenarioChangeApiDTO changeApiDTO,
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        changeApiDTO.setType(PlanModificationState.REMOVED.name());
        changeApiDTO.setTargets(Collections.singletonList(
                dtoForId(removal.getEntityId(), serviceEntityMap)));
        changeApiDTO.setProjectionDays(removal.getChangeApplicationDaysList());
    }

    private static void buildApiTopologyReplace(@Nonnull final TopologyReplace replace,
                                        @Nonnull final ScenarioChangeApiDTO changeApiDTO,
                                        @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        changeApiDTO.setType(PlanModificationState.REPLACED.name());
        changeApiDTO.setProjectionDays(replace.getChangeApplicationDaysList());
        changeApiDTO.setTargets(Arrays.asList(
            dtoForId(replace.getAddTemplateId(), serviceEntityMap),
            dtoForId(replace.getRemoveEntityId(), serviceEntityMap)
        ));
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
