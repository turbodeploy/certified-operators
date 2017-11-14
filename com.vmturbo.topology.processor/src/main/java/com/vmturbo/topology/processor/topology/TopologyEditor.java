package com.vmturbo.topology.processor.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

/**
 * The {@link TopologyEditor} is responsible for applying a set of changes (reflected
 * by {@link ScenarioChange} objects) to a topology.
 * <p>
 * Topology editing is an important phase of the plan lifecycle, since a key part of plans
 * is testing the addition/removal/replacement of entities.
 */
public class TopologyEditor {
    private final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final TemplateConverterFactory templateConverterFactory;

    TopologyEditor(@Nonnull final IdentityProvider identityProvider,
                   @Nonnull final TemplateConverterFactory templateConverterFactory) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.templateConverterFactory = Objects.requireNonNull(templateConverterFactory);
    }

    /**
     * Apply a set of changes to a topology. The method will edit the
     * input topology in-place.
     *
     * @param topology The entities in the topology, arranged by ID.
     * @param changes The list of changes to make. Some of these changes may not be topology-related.
     *                We ignore those.
     */
    public void editTopology(@Nonnull final Map<Long, TopologyEntityDTO.Builder> topology,
                             @Nonnull final List<ScenarioChange> changes) {

        final Map<Long, Long> entityAdditions = new HashMap<>();
        final Set<Long> entitiesToRemove = new HashSet<>();
        final Set<Long> entitiesToReplace = new HashSet<>();
        final Map<Long, Long> templateToAdd = new HashMap<>();

        changes.forEach(change -> {
            if (change.hasTopologyAddition()) {
                final TopologyAddition addition = change.getTopologyAddition();
                if (addition.hasEntityId()) {
                    addTopologyAdditionCount(entityAdditions, addition, addition.getEntityId());
                } else if (addition.hasTemplateId()) {
                    addTopologyAdditionCount(templateToAdd, addition, addition.getTemplateId());
                } else {
                    logger.warn("Unimplemented handling for topology addition with {}",
                            addition.getEntityOrTemplateOrGroupIdCase());
                }
            } else if (change.hasTopologyRemoval()) {
                final TopologyRemoval removal = change.getTopologyRemoval();
                if (removal.hasEntityId()) {
                    entitiesToRemove.add(removal.getEntityId());
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                            removal.getEntityOrGroupIdCase());
                }
            } else if (change.hasTopologyReplace()) {
                final TopologyReplace replace = change.getTopologyReplace();
                templateToAdd.put(replace.getAddTemplateId(),
                        templateToAdd.getOrDefault(replace.getAddTemplateId(), 0L) + 1);
                if (replace.hasRemoveEntityId()) {
                    entitiesToReplace.add(replace.getRemoveEntityId());
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                            replace.getEntityOrGroupRemovalIdCase());
                }

            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        });

        entityAdditions.forEach((oid, addCount) -> {
            TopologyEntityDTO.Builder entity = topology.get(oid);
            if (entity != null) {
                for (int i = 0; i < addCount; ++i) {
                    TopologyEntityDTO.Builder clone = clone(entity, identityProvider, i);
                    topology.put(clone.getOid(), clone);
                }
            }
        });

        entitiesToRemove.forEach(oid -> {
            TopologyEntityDTO.Builder entity = topology.get(oid);
            if (entity != null) {
                entity.setEntityState(EntityState.POWERED_OFF);
            }
        });

        entitiesToReplace.forEach(topology::remove);

        addTemplateTopologyEntities(templateToAdd).forEach(entity ->
                topology.put(entity.getOid(), entity));
    }

    /**
     * Create a clone of a topology entity, modifying some values, including
     * oid, display name, and unplacing the shopping lists.
     *
     * @param entity source topology entity
     * @param identityProvider used to generate an oid for the clone
     * @param cloneCounter used in the display name
     * @return the cloned entity
     */
    private static TopologyEntityDTO.Builder clone(TopologyEntityDTO.Builder entity,
                                                   @Nonnull final IdentityProvider identityProvider,
                                                   int cloneCounter) {
        final TopologyEntityDTO.Builder cloneBuilder = entity.clone()
                .clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProvider bought :
                entity.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = bought.getProviderId();
            cloneBuilder.addCommoditiesBoughtFromProviders(
                    bought.toBuilder().setProviderId(--noProvider).build());
            oldProvidersMap.put(noProvider, oldProvider);
        }
        Map<String, String> entityProperties =
                Maps.newHashMap(cloneBuilder.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put("oldProviders", new Gson().toJson(oldProvidersMap));
        }
        return cloneBuilder
                .setDisplayName(entity.getDisplayName() + " - Clone #" + cloneCounter)
                .setOid(identityProvider.getCloneId(entity))
                .putAllEntityPropertyMap(entityProperties);
    }

    /**
     * Add all addition topology entities which converted from templates
     *
     * @param templateAdditions contains all addition templates and the count need to add.
     */
    private Stream<TopologyEntityDTO.Builder> addTemplateTopologyEntities(
            @Nonnull Map<Long, Long> templateAdditions) {
        // Check if there are templates additions
        if (templateAdditions.isEmpty()) {
            return Stream.empty();
        } else {
            return templateConverterFactory.generateTopologyEntityFromTemplates(templateAdditions);
        }
    }

    private static void addTopologyAdditionCount(@Nonnull final Map<Long, Long> additionMap,
                                                 @Nonnull TopologyAddition addition,
                                                 long key) {
        final long additionCount =
                addition.hasAdditionCount() ? addition.getAdditionCount() : 1L;
        additionMap.put(key, additionMap.getOrDefault(key, 0L) + additionCount);
    }
}
