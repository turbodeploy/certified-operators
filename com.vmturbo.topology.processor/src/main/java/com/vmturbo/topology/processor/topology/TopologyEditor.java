
package com.vmturbo.topology.processor.topology;

import java.util.Collections;
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

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Removed;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Replaced;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

/**
 * The {@link TopologyEditor} is responsible for applying a set of changes (reflected
 * by {@link ScenarioChange} objects) to a topology.
 *
 * <p>Topology editing is an important phase of the plan lifecycle, since a key part of plans
 * is testing the addition/removal/replacement of entities.
 */
public class TopologyEditor {
    private final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final TemplateConverterFactory templateConverterFactory;

    private final GroupServiceBlockingStub groupServiceClient;

    private static final Set<Integer> UTILIZATION_LEVEL_TYPES = ImmutableSet
            .of(CommodityType.CPU_VALUE, CommodityType.MEM_VALUE);

    TopologyEditor(@Nonnull final IdentityProvider identityProvider,
                   @Nonnull final TemplateConverterFactory templateConverterFactory,
                   @Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.templateConverterFactory = Objects.requireNonNull(templateConverterFactory);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * Apply a set of changes to a topology. The method will edit the
     * input topology in-place.
     *
     * @param topology The entities in the topology, arranged by ID.
     * @param changes The list of changes to make. Some of these changes may not be topology-related.
     *                We ignore those.
     * @param topologyInfo Information describing the topology and its context.
     * @param groupResolver The resolver to use when resolving group membership.
     */
    public void editTopology(@Nonnull final Map<Long, TopologyEntity.Builder> topology,
                             @Nonnull final List<ScenarioChange> changes,
                             @Nonnull final TopologyInfo topologyInfo,
                             @Nonnull final GroupResolver groupResolver) {

        // Set shopTogether to false for all entities if it's not a alleviate pressure plan,
        // so SNM is not performed by default.
        // 1. For full scope custom plan,
        //    since shopTogether is false for all entities, we will not perform SNM on any entities.
        // 2. For add workload plans (VM template),
        //    the new entities created later from VM template will shop together by default,
        //    which means we will perform SNM on those new entities.
        // 3. For add workload plans (VM copy),
        //    the new entities created later from VM copy have the same settings as the VM copy.
        //    Since shopTogether is false for VM copy, shopTogether is also false for new entities,
        //    which means we will not perform SNM on those new entities.
        // 4. For hardware refresh plans,
        //    shopTogether will be reset to true later for all consumers of an entity to be replaced,
        //    which means we will perform SNM on those consumers.
        // 5. For alleviate pressure plans,
        //    set shopTogether to true for all entities. This plan is a scoped plan, so entities
        //    not related to the hot or cold clusters will be discarded later.
        // Related story: OM-44989
        boolean isAlleviatePressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo);
        topology.forEach((oid, entity) ->
            entity.getEntityBuilder().getAnalysisSettingsBuilder()
                .setShopTogether(isAlleviatePressurePlan));

        final Map<Long, Long> entityAdditions = new HashMap<>();
        final Set<Long> entitiesToRemove = new HashSet<>();
        final Set<Long> entitiesToReplace = new HashSet<>();
        final Map<Long, Long> templateToAdd = new HashMap<>();
        // Map key is template id, and value is the replaced topologyEntity.
        final Multimap<Long, Long> templateToReplacedEntity =
            ArrayListMultimap.create();
        final Map<Long, Group> groupIdToGroupMap = getGroups(changes);
        final TopologyGraph<TopologyEntity> topologyGraph =
            TopologyEntityTopologyGraphCreator.newGraph(topology);

        changes.forEach(change -> {
            if (change.hasTopologyAddition()) {
                final TopologyAddition addition = change.getTopologyAddition();
                if (addition.hasEntityId()) {
                    addTopologyAdditionCount(entityAdditions, addition, addition.getEntityId());
                } else if (addition.hasTemplateId()) {
                    addTopologyAdditionCount(templateToAdd, addition, addition.getTemplateId());
                } else if (addition.hasGroupId()) {
                    groupResolver.resolve(groupIdToGroupMap.get(addition.getGroupId()), topologyGraph)
                        .forEach(entityId -> addTopologyAdditionCount(
                                entityAdditions, addition, entityId));
                } else {
                    logger.warn("Unimplemented handling for topology addition with {}",
                            addition.getAdditionTypeCase());
                }
            } else if (change.hasTopologyRemoval()) {
                final TopologyRemoval removal = change.getTopologyRemoval();
                Set<Long> entities = removal.hasEntityId()
                    ? Collections.singleton(removal.getEntityId())
                    : groupResolver.resolve(
                        groupIdToGroupMap.get(removal.getGroupId()),
                        topologyGraph);
                entities.forEach(id -> {
                    if (!topology.containsKey(id)) {
                        throwEntityNotFoundException(id);
                    }
                    entitiesToRemove.add(id);
                });
            } else if (change.hasTopologyReplace()) {
                final TopologyReplace replace = change.getTopologyReplace();
                Set<Long> entities = replace.hasRemoveEntityId()
                    ? Collections.singleton(replace.getRemoveEntityId())
                    : groupResolver.resolve(
                        groupIdToGroupMap.get(replace.getRemoveGroupId()),
                        topologyGraph);
                entities.forEach(id -> {
                    if (!topology.containsKey(id)) {
                        throwEntityNotFoundException(id);
                    }
                    entitiesToReplace.add(id);
                    templateToReplacedEntity.put(replace.getAddTemplateId(), id);
                });
            // only change utilization when plan changes have utilization level message.
            } else if (change.hasPlanChanges() && change.getPlanChanges().hasUtilizationLevel()) {
                final UtilizationLevel utilizationLevel =
                        change.getPlanChanges().getUtilizationLevel();
                changeUtilization(topology, utilizationLevel.getPercentage());

            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        });

        // entities added in this stage will have a plan origin pointed to the context id of this topology
        Origin entityOrigin = Origin.newBuilder()
            .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder()
                    .setPlanId(topologyInfo.getTopologyContextId()))
            .build();

        entityAdditions.forEach((oid, addCount) -> {
            TopologyEntity.Builder entity = topology.get(oid);
            if (entity != null) {
                for (int i = 0; i < addCount; ++i) {
                    // Create the new entity being added, but set the plan origin so these added
                    // entities aren't counted in plan "current" stats
                    TopologyEntityDTO.Builder clone =
                        clone(entity.getEntityBuilder(), identityProvider, i, topology)
                            .setOrigin(entityOrigin);
                    topology.put(clone.getOid(), TopologyEntity.newBuilder(clone));
                }
            }
        });

        // Prepare any entities that are getting removed as part of the plan, for removal from the
        // analysis topology. This process will unplace any current buyers of these entities
        // commodities, then mark the entities as "removed", so they can be removed from the Analysis
        // entities set.
        Edit removalEdit = Edit.newBuilder()
                .setRemoved(Removed.newBuilder()
                        .setPlanId(topologyInfo.getTopologyContextId()))
                .build();
        prepareEntitiesForRemoval(entitiesToRemove, topology, removalEdit);

        // Like we just did for "removed" entities, we will prepare any entities "replaced" as part
        // of a plan to be removed from the analysis topology. The steps are the same as with the
        // "removed" entities, except the Edit that is recorded on them is a Replacement, rather
        // than a Removal.
        Edit replacementEdit = Edit.newBuilder()
                .setReplaced(Replaced.newBuilder()
                        .setPlanId(topologyInfo.getTopologyContextId()))
                .build();
        prepareEntitiesForRemoval(entitiesToReplace, topology, replacementEdit);

        // Mark added entities with the Plan Origin so they aren't counted in "before" plan
        // stats
        addTemplateTopologyEntities(templateToAdd, templateToReplacedEntity, topology)
            .forEach(entity -> {
                        // entities added in plan are marked with a plan origin
                        entity.setOrigin(entityOrigin);
                        topology.put(entity.getOid(), TopologyEntity.newBuilder(entity));
                    });
    }

    /**
     * Marks an entity with an Edit attribute. When the entity is processed in the Market, entities
     * with "Removed" or "Replaced" edits on them will be removed from the set of entities being
     * analyzed.
     **/
    private void tagEntityWithEdit(Long oid,
                                   @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                   Edit edit) {
        TopologyEntity.Builder entity = topology.get(oid);
        if (entity != null) {
            entity.getEntityBuilder().setEdit(edit);
        }
    }

    private void changeUtilization(@Nonnull Map<Long, TopologyEntity.Builder> topology, int percentage) {
        final Predicate<TopologyEntity.Builder> isVm =
                entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE;
        final Set<TopologyEntity.Builder> topologyVms = topology.values().stream().filter(isVm)
                .collect(Collectors.toSet());
        for (TopologyEntity.Builder vm : topologyVms) {
            final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
                    vm.getEntityBuilder().getCommoditiesBoughtFromProvidersList();
            final List<CommoditiesBoughtFromProvider> increasedCommodities =
                    increaseProviderCommodities(topology, percentage, vm, commoditiesBoughtFromProviders);
            vm.getEntityBuilder().clearCommoditiesBoughtFromProviders();
            vm.getEntityBuilder().addAllCommoditiesBoughtFromProviders(increasedCommodities);
        }
    }

    @Nonnull
    private List<CommoditiesBoughtFromProvider> increaseProviderCommodities(
            @Nonnull Map<Long, TopologyEntity.Builder> topology, int percentage,
            @Nonnull TopologyEntity.Builder vm, List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders) {
        final ImmutableList.Builder<CommoditiesBoughtFromProvider> increasedProviderCommodities =
                ImmutableList.builder();
        for (CommoditiesBoughtFromProvider providerCommodities : commoditiesBoughtFromProviders) {
            List<CommodityBoughtDTO> increasedCommodities =
                    increaseCommodities(topology, percentage, vm.getEntityBuilder(), providerCommodities);
            increasedProviderCommodities.add(CommoditiesBoughtFromProvider
                    .newBuilder(providerCommodities)
                    .clearCommodityBought()
                    .addAllCommodityBought(increasedCommodities)
                    .build());
        }
        return increasedProviderCommodities.build();
    }

    @Nonnull
    private List<CommodityBoughtDTO> increaseCommodities(
        @Nonnull Map<Long, TopologyEntity.Builder> topology,
        int percentage, @Nonnull TopologyEntityDTO.Builder vm,
        @Nonnull CommoditiesBoughtFromProvider providerCommodities) {
        final ImmutableList.Builder<CommodityBoughtDTO> changedCommodities = ImmutableList.builder();
        for (CommodityBoughtDTO commodity : providerCommodities.getCommodityBoughtList()) {
            final int commodityType = commodity.getCommodityType().getType();
            if (UTILIZATION_LEVEL_TYPES.contains(commodityType)) {
                final double changedUtilization = increaseByPercent(commodity.getUsed(), percentage);
                changedCommodities.add(CommodityBoughtDTO.newBuilder(commodity)
                        .setUsed(changedUtilization).build());
                // increase provider's commodity sold utilization only when it has provider
                if (providerCommodities.hasProviderId()) {
                    increaseCommoditySoldByProvider(topology, providerCommodities.getProviderId(),
                            vm.getOid(), commodityType, percentage);
                }
            } else {
                changedCommodities.add(commodity);
            }
        }
        return changedCommodities.build();
    }

    private void increaseCommoditySoldByProvider(@Nonnull Map<Long, TopologyEntity.Builder> topology,
            long providerId, long consumerId, int commodityType, int percentage) {
        final ImmutableList.Builder<CommoditySoldDTO> changedSoldCommodities =
                ImmutableList.builder();
        final TopologyEntity.Builder provider = topology.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("Topology doesn't contain entity with id " + providerId);
        }
        for (CommoditySoldDTO sold : provider.getEntityBuilder().getCommoditySoldListList()) {
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
        provider.getEntityBuilder()
            .clearCommoditySoldList()
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
     * @param topology to which entities belong to
     * @return the cloned entity
     */
    private static TopologyEntityDTO.Builder clone(TopologyEntityDTO.Builder entity,
                                                   @Nonnull final IdentityProvider identityProvider,
                                                   int cloneCounter,
                                                   Map<Long, TopologyEntity.Builder> topology) {
        final TopologyEntityDTO.Builder cloneBuilder = entity.clone()
                .clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProvider bought :
                entity.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = bought.getProviderId();
            CommoditiesBoughtFromProvider.Builder clonedProvider =
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(--noProvider)
                    .setMovable(true)
                    .setProviderEntityType(bought.getProviderEntityType());
            // In legacy opsmgr, during topology addition, all constraints are
            // implicitly ignored. We do the same thing here.
            // A Commodity has a constraint if it has a key in its CommodityType.
            bought.getCommodityBoughtList().forEach(commodityBought -> {
                if (!commodityBought.getCommodityType().hasKey()) {
                    clonedProvider.addCommodityBought(commodityBought);
                }
            });
            cloneBuilder.addCommoditiesBoughtFromProviders(clonedProvider.build());
            oldProvidersMap.put(noProvider, oldProvider);
        }

        long cloneId = identityProvider.getCloneId(entity);
        cloneBuilder.getCommoditySoldListBuilderList().stream()
             // for each commodity sold for clone, update utilization to 0
            .map(commSold -> {
                commSold.setUsed(0d);
                commSold.setPeak(0d);
                return commSold;
            })
            .filter(commSold -> AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType()))
            .forEach(bicliqueCommSold -> {
                // Set commodity sold for storage/host in case of a DSPM/DATASTORE commodity.
                // This will make sure we have an edge for biclique creation between newly cloned host
                // to original storages or newly cloned storage to original hosts.
                TopologyEntity.Builder connectedEntity = topology.get(bicliqueCommSold.getAccesses());
                if (connectedEntity != null) {
                    int commType =
                            bicliqueCommSold.getCommodityType().getType() == CommodityType.DSPM_ACCESS_VALUE
                            ? CommodityType.DATASTORE_VALUE : CommodityType.DSPM_ACCESS_VALUE;
                    connectedEntity.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setKey("CommodityInClone::" + commType + "::" + cloneId)
                            .setType(commType))
                        .setAccesses(cloneId)
                        .build());
                }
            });

        Map<String, String> entityProperties =
                Maps.newHashMap(cloneBuilder.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put("oldProviders", new Gson().toJson(oldProvidersMap));
        }
        return cloneBuilder
                .setDisplayName(entity.getDisplayName() + " - Clone #" + cloneCounter)
                .setOid(cloneId)
                .putAllEntityPropertyMap(entityProperties);
    }

    /**
     * The entities being removed (or replaced) will be marked with an Edit property
     * that flags these entities for removal in the Market.
     *
     * @param entitiesToRemove a set of replaced entity oids.
     * @param topology the entities in the topology, arranged by ID.
     * @param edit the Edit to assign to the entity, marking it as removed/replaced
     */
    private void prepareEntitiesForRemoval(@Nonnull Set<Long> entitiesToRemove,
                                           @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                           @Nonnull Edit edit) {
        /**
         * Mark the entities as edited. The entities remain in the topology and all related entities
         * maintain their relationships to the entities marked for removal. The marked entities will
         * actually be removed in the market component prior to analysis.
         *
         * It is important to retain the entities in the topology and the relationships to those entities
         * for correctness. For example, consider the scoping algorithm which occurs after these
         * edits take place. If we were to run a plan on a cluster (ie the scope seeds are the hosts in
         * the cluster) and then replace those hosts, if we removed the hosts now, the scoping algorithm
         * would be unable to scope from the seeds. Further, even if we retained the hosts but unplaced
         * the VMs buying from those hosts, the scoping algorithm would be unable to traverse from
         * the hosts to their consumers for scoping purposes because no entities buy from the hosts being
         * removed. Another example of why we should not unplace due to removal at this time is because
         * if a group applies to the entities consuming from or providing to the entities being removed,
         * that group would be invalidated by removing the relationship to the entity being removed. As
         * a result, policy and setting application would do the wrong thing.
         */
        entitiesToRemove.forEach(entityOid -> tagEntityWithEdit(entityOid, topology, edit));
    }

    /**
     * Add all addition or replaced topology entities which converted from templates.
     *
     * @param templateAdditions a map which key is template id, value is the addition count.
     * @param templateToReplacedEntity a map which key is template id, value is a list of replaced entity.
     * @param topology The entities in the topology, arranged by ID.
     *
     * @return a stream of builders of entities created from the specified templates
     */
    private Stream<TopologyEntityDTO.Builder> addTemplateTopologyEntities(
        @Nonnull Map<Long, Long> templateAdditions,
        @Nonnull Multimap<Long, Long> templateToReplacedEntity,
        @Nonnull Map<Long, TopologyEntity.Builder> topology) {
        // Check if there are templates additions or replaced
        if (templateAdditions.isEmpty() && templateToReplacedEntity.isEmpty()) {
            return Stream.empty();
        } else {
            return templateConverterFactory.generateTopologyEntityFromTemplates(templateAdditions,
                templateToReplacedEntity, topology);
        }
    }

    private static void addTopologyAdditionCount(@Nonnull final Map<Long, Long> additionMap,
                                                 @Nonnull TopologyAddition addition,
                                                 long key) {
        final long additionCount =
                addition.hasAdditionCount() ? addition.getAdditionCount() : 1L;
        additionMap.put(key, additionMap.getOrDefault(key, 0L) + additionCount);
    }


    private Map<Long, Group> getGroups(List<ScenarioChange> changes) {
        final Set<Long> groupIds = PlanDTOUtil.getInvolvedGroups(changes);
        final Map<Long, Group> groupIdToGroupMap = new HashMap<>();

        if (!groupIds.isEmpty()) {
            final GetGroupsRequest request =
                    GetGroupsRequest.newBuilder()
                            .addAllId(groupIds)
                            .setResolveClusterSearchFilters(true)
                            .build();

            groupServiceClient.getGroups(request)
                    .forEachRemaining(
                            group -> groupIdToGroupMap.put(group.getId(), group)
                    );
        }

        return groupIdToGroupMap;
    }

    private void throwEntityNotFoundException(long id) {
        logger.error("Cannot find entity: {} in current topology", id);
        throw TopologyEditorException.notFoundEntityException(id);
    }
}
