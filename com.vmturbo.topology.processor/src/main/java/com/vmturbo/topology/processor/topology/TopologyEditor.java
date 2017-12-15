package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private static final Set<Integer> UTILIZATION_LEVEL_TYPES = ImmutableSet
            .of(CommodityType.CPU_VALUE, CommodityType.MEM_VALUE);

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
        // Map key is template id, and value is the replaced topologyEntity.
        final Multimap<Long, TopologyEntityDTO> templateToReplacedEntity = ArrayListMultimap.create();

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
                    if (!topology.containsKey(replace.getRemoveEntityId())) {
                        logger.error("Can not find {} in current topology", replace.getRemoveEntityId());
                        throw TopologyEditorException.notFoundEntityException(replace.getRemoveEntityId());
                    }
                    templateToReplacedEntity.put(replace.getAddTemplateId(),
                        topology.get(replace.getRemoveEntityId()).build());
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                            replace.getEntityOrGroupRemovalIdCase());
                }

            } else if (change.hasPlanChanges()) {
                final UtilizationLevel utilizationLevel =
                        change.getPlanChanges().getUtilizationLevel();
                changeUtilization(topology, utilizationLevel.getPercentage());

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

        // when removing a entity, we should unplace its all consumers.
        handleReplacedEntities(entitiesToReplace, topology);

        addTemplateTopologyEntities(templateToAdd, templateToReplacedEntity)
            .forEach(entity ->
                topology.put(entity.getOid(), entity));
    }

    private void changeUtilization(@Nonnull Map<Long, TopologyEntityDTO.Builder> topology, int percentage) {
        final Predicate<TopologyEntityDTO.Builder> isVm =
                entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE;
        final Set<TopologyEntityDTO.Builder> topologyVms = topology.values().stream().filter(isVm)
                .collect(Collectors.toSet());
        for (TopologyEntityDTO.Builder vm : topologyVms) {
            final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
                    vm.getCommoditiesBoughtFromProvidersList();
            final List<CommoditiesBoughtFromProvider> increasedCommodities =
                    increaseProviderCommodities(topology, percentage, vm, commoditiesBoughtFromProviders);
            vm.clearCommoditiesBoughtFromProviders();
            vm.addAllCommoditiesBoughtFromProviders(increasedCommodities);
        }
    }

    @Nonnull
    private List<CommoditiesBoughtFromProvider> increaseProviderCommodities(
            @Nonnull Map<Long, Builder> topology, int percentage,
            @Nonnull Builder vm, List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders) {
        final ImmutableList.Builder<CommoditiesBoughtFromProvider> increasedProviderCommodities =
                ImmutableList.builder();
        for (CommoditiesBoughtFromProvider providerCommodities : commoditiesBoughtFromProviders) {
            List<CommodityBoughtDTO> increasedCommodities =
                    increaseComodities(topology, percentage, vm, providerCommodities);
            increasedProviderCommodities.add(CommoditiesBoughtFromProvider
                    .newBuilder(providerCommodities)
                    .clearCommodityBought()
                    .addAllCommodityBought(increasedCommodities)
                    .build());
        }
        return increasedProviderCommodities.build();
    }

    @Nonnull
    private List<CommodityBoughtDTO> increaseComodities(
            @Nonnull Map<Long, Builder> topology,
            int percentage, @Nonnull TopologyEntityDTO.Builder vm,
            @Nonnull CommoditiesBoughtFromProvider providerCommodities) {
        final ImmutableList.Builder<CommodityBoughtDTO> changedCommodities = ImmutableList.builder();
        for (CommodityBoughtDTO commodity : providerCommodities.getCommodityBoughtList()) {
            final int commodityType = commodity.getCommodityType().getType();
            if (UTILIZATION_LEVEL_TYPES.contains(commodityType)) {
                final double changedUtilization = increaseByPercent(commodity.getUsed(), percentage);
                changedCommodities.add(CommodityBoughtDTO.newBuilder(commodity)
                        .setUsed(changedUtilization).build());

                increaseCommoditySoldByProvider(topology, providerCommodities.getProviderId(),
                        vm.getOid(), commodityType, percentage);

            } else {
                changedCommodities.add(commodity);
            }
        }
        return changedCommodities.build();
    }

    private void increaseCommoditySoldByProvider(@Nonnull Map<Long, TopologyEntityDTO.Builder> topology,
            long providerId, long consumerId, int commodityType, int percentage) {
        final ImmutableList.Builder<CommoditySoldDTO> changedSoldCommodities =
                ImmutableList.builder();
        final TopologyEntityDTO.Builder provider = topology.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("Topology doesn't contain entity with id " + providerId);
        }
        for (CommoditySoldDTO sold : provider.getCommoditySoldListList()) {
            if (sold.getAccesses() == consumerId
                    && sold.getCommodityType().getType() == commodityType) {
                final CommoditySoldDTO increasedCommodity = CommoditySoldDTO.newBuilder(sold)
                        .setUsed(increaseByPercent(sold.getUsed(), percentage))
                        .build();
                changedSoldCommodities.add(increasedCommodity);
            } else {
                changedSoldCommodities.add(sold);
            }
        }
        provider.clearCommoditySoldList()
                .addAllCommoditySoldList(changedSoldCommodities.build());
    }

    private double increaseByPercent(double value, int percentage) {
        return value + value * percentage / 100;
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
     * It will iterates over entire topology and tries to find all consumers of replaced entities,
     *  And for all consumers, we should remove their provider id of Commodity bought to make them unplaced.
     *
     * @param entitiesToReplace a set of replaced entity oids.
     * @param topology the entities in the topology, arranged by ID.
     */
    private void handleReplacedEntities(@Nonnull Set<Long> entitiesToReplace,
                                        @Nonnull final Map<Long, TopologyEntityDTO.Builder> topology) {
        for (Long entityOid : topology.keySet()) {
            final TopologyEntityDTO.Builder topologyBuilder = topology.get(entityOid);
            if (isConsumerOfReplacedEntities(entitiesToReplace, topologyBuilder)) {
                final Map<Long, Long> oldProvidersMap = Maps.newHashMap();
                TopologyEntityDTO.Builder unplacedTopologyBuilder =
                    TopologyEntityDTO.newBuilder(topologyBuilder.build())
                        .clearCommoditiesBoughtFromProviders()
                        .addAllCommoditiesBoughtFromProviders(unplacedCommoditiesBoughtGroup(
                            topologyBuilder.getCommoditiesBoughtFromProvidersList(), entitiesToReplace,
                            oldProvidersMap));
                Map<String, String> entityProperties = Maps.newHashMap();
                if (!oldProvidersMap.isEmpty()) {
                    // TODO: OM-26631 - get rid of unstructured data and Gson
                    entityProperties.put("oldProviders", new Gson().toJson(oldProvidersMap));
                }
                unplacedTopologyBuilder.putAllEntityPropertyMap(entityProperties);
                topology.put(entityOid, unplacedTopologyBuilder);
            }
        }
        entitiesToReplace.forEach(topology::remove);
    }

    /**
     * Check if the topologyEntity is a consumer of replaced entities.
     *
     * @param entitiesToReplace a set of replaced entity oids.
     * @param topologyBuilder {@link TopologyEntityDTO.Builder}
     * @return a boolean.
     */
    private boolean isConsumerOfReplacedEntities(@Nonnull Set<Long> entitiesToReplace,
                                                 @Nonnull final TopologyEntityDTO.Builder topologyBuilder) {
        return topologyBuilder.getCommoditiesBoughtFromProvidersList().stream()
            .filter(CommoditiesBoughtFromProvider::hasProviderId)
            .map(CommoditiesBoughtFromProvider::getProviderId)
            .anyMatch(entitiesToReplace::contains);
    }

    /**
     * Remove all provider id of {@link CommoditiesBoughtFromProvider} if the provider id is one of
     * replaced entity oids. And generate a fake provider id and keep the mapping relationship from
     * fake provider id to original id to entityProperties map.
     *
     * @param commoditiesBoughtFromProviders a list of {@link CommoditiesBoughtFromProvider}
     * @param entitiesToReplace a set of replaced entity oids.
     * @param entityProperties a map contains mapping relationship from fake provider it to original
     *                         provder id.
     * @return a list of {@link CommoditiesBoughtFromProvider}.
     */
    private List<CommoditiesBoughtFromProvider> unplacedCommoditiesBoughtGroup(
        @Nonnull final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders,
        @Nonnull final Set<Long> entitiesToReplace,
        @Nonnull final Map<Long, Long> entityProperties) {
        final List<CommoditiesBoughtFromProvider> unplacedCommoditiesBoughtList = new ArrayList<>();
        long fakeProvider = 0;
        for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider : commoditiesBoughtFromProviders) {
            if (commoditiesBoughtFromProvider.hasProviderId() &&
                entitiesToReplace.contains(commoditiesBoughtFromProvider.getProviderId())) {
                final long oldProvider = commoditiesBoughtFromProvider.getProviderId();
                CommoditiesBoughtFromProvider unplacedCommodityBoughtFromProvider =
                    CommoditiesBoughtFromProvider.newBuilder(commoditiesBoughtFromProvider)
                        .clearProviderId()
                        .setProviderId(--fakeProvider)
                        .build();
                entityProperties.put(fakeProvider, oldProvider);
                unplacedCommoditiesBoughtList.add(unplacedCommodityBoughtFromProvider);
            }
            else {
                unplacedCommoditiesBoughtList.add(commoditiesBoughtFromProvider);
            }
        }
        return unplacedCommoditiesBoughtList;
    }

    /**
     * Add all addition or replaced topology entities which converted from templates
     *
     * @param templateAdditions a map which key is template id, value is the addition count.
     * @param templateToReplacedEntity a map which key is template id, value is a list of replaced entity.
     */
    private Stream<TopologyEntityDTO.Builder> addTemplateTopologyEntities(
        @Nonnull Map<Long, Long> templateAdditions,
        @Nonnull Multimap<Long, TopologyEntityDTO> templateToReplacedEntity) {
        // Check if there are templates additions or replaced
        if (templateAdditions.isEmpty() && templateToReplacedEntity.isEmpty()) {
            return Stream.empty();
        } else {
            return templateConverterFactory.generateTopologyEntityFromTemplates(templateAdditions,
                templateToReplacedEntity);
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
