package com.vmturbo.common.protobuf;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;

/**
 * Utilities for extracting information from plan-related protobufs
 * (in com.vmturbo.common.protobuf/.../protobuf/plan)
 */
public class PlanDTOUtil {

    /**
     * Return the OIDs of entities involved in list of {@link ScenarioChange}.
     *
     * @param scenarioChanges The target scenario changes.
     * @return The set of involved entity IDs. This does not include groups, templates, etc.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntities(
            @Nonnull final List<ScenarioChange> scenarioChanges) {
        return scenarioChanges.stream().
                flatMap(change -> PlanDTOUtil.getInvolvedEntities(change).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Return the OIDs of entities involved in a {@link ScenarioChange}.
     *
     * @param scenarioChange The target scenario change.
     * @return The set of involved entity IDs. This does not include groups, templates, etc.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntities(@Nonnull final ScenarioChange scenarioChange) {
        ImmutableSet.Builder<Long> entitiesBuilder = ImmutableSet.builder();
        if (scenarioChange.hasTopologyAddition()) {
            if (scenarioChange.getTopologyAddition().hasEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyAddition().getEntityId());
            }
        }

        if (scenarioChange.hasTopologyRemoval()) {
            if (scenarioChange.getTopologyRemoval().hasEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyRemoval().getEntityId());
            }
        }

        if (scenarioChange.hasTopologyReplace()) {
            if (scenarioChange.getTopologyReplace().hasRemoveEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyReplace().getRemoveEntityId());
            }
        }

        return entitiesBuilder.build();
    }

    public static Set<Long> getInvolvedTemplates(@Nonnull final List<ScenarioChange> scenarioChanges) {
        return scenarioChanges.stream()
            .flatMap(change -> PlanDTOUtil.getInvolvedTemplates(change).stream())
            .collect(Collectors.toSet());
    }

    /**
     * Return the OIDS of templates which are part of {@link ScenarioChange}. And the scenario change
     * could be Topology Addition or Topology Replace.
     *
     * @param scenarioChange The target scenario changes.
     * @return A set of involved template OIDs.
     */
    @Nonnull
    public static Set<Long> getInvolvedTemplates(@Nonnull final ScenarioChange scenarioChange) {
        ImmutableSet.Builder<Long> templatesBuilder = ImmutableSet.builder();
        if (scenarioChange.hasTopologyAddition()) {
            if (scenarioChange.getTopologyAddition().hasTemplateId()) {
                templatesBuilder.add(scenarioChange.getTopologyAddition().getTemplateId());
            }
        }

        if (scenarioChange.hasTopologyReplace()) {
            if (scenarioChange.getTopologyReplace().hasAddTemplateId()) {
                templatesBuilder.add(scenarioChange.getTopologyReplace().getAddTemplateId());
            }
        }
        return templatesBuilder.build();
    }
}
