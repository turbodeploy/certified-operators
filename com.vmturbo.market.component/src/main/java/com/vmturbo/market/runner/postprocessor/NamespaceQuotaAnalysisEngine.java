package com.vmturbo.market.runner.postprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.DoubleMath;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Analysis engine to generate namespace resource quota resizing actions based on corresponding
 * container resizing actions and update projected namespace commodity capacity accordingly.
 *
 * <P>Namespace quota resizing actions are generated to accommodate containers with resources congested
 * to assure performance of container workloads.
 *
 * <p>Namespace quota new capacity = capacity changes of container resizing actions + namespace quota current usage,
 * only if quota new capacity is larger than current capacity. The container resizing actions we want to
 * account for are different in real time and plan:
 * - in plan we take all container resizing actions into consideration to simulate the result;
 * - in real time we take only container resizing up actions.
 */
public class NamespaceQuotaAnalysisEngine {

    private static final Logger logger = LogManager.getLogger();

    static final double COMPARISON_EPSILON = 1e-2;

    /**
     * Map from container resource commodity to corresponding namespace quota commodity. Since we only
     * generate namespace quota resizing up action, we care about only the map of container limit
     * commodities to namespace limit quota here because request commodities cannot be resized up.
     */
    private static final Map<Integer, Integer> resourceToQuotaCommodityMap = ImmutableMap.of(
        CommodityType.VCPU_VALUE, CommodityType.VCPU_LIMIT_QUOTA_VALUE,
        CommodityType.VMEM_VALUE, CommodityType.VMEM_LIMIT_QUOTA_VALUE);

    /**
     * Set of entity types which resell quota commodities to container to facilitate the supply chain
     * traversal from container to namespace.
     */
    private static final Set<Integer> quotaResellerEntityTypes = ImmutableSet.of(
        EntityType.CONTAINER_POD_VALUE,
        EntityType.WORKLOAD_CONTROLLER_VALUE);

    /**
     * Execute namespace quota analysis engine.
     *
     * @param topologyInfo      Given {@link TopologyInfo} which determines current topology is real
     *                          time or plan.
     * @param originalEntities  Map of entity OID to original TopologyEntityDTO.
     * @param projectedEntities Map of entity OID to projectedTopologyEntityDTO.
     * @param actions           List of actions generated from previous analysis.
     * @return List of generated namespace resizing actions.
     */
    @Nonnull
    public List<Action> execute(@Nonnull final TopologyInfo topologyInfo,
                                @Nonnull final Map<Long, TopologyEntityDTO> originalEntities,
                                @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
                                @Nonnull final List<Action> actions) {
        final StopWatch stopWatch = StopWatch.createStarted();
        final String logPrefix = String.format("%s topology [ID=%d, context=%d]:",
            topologyInfo.getTopologyType(), topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());

        final Map<Long, Map<Integer, Float>> nsToCommCapacityChangeMap =
            getNSToCommCapacityChangeMap(logPrefix, actions, projectedEntities, topologyInfo.getTopologyType() == TopologyType.PLAN);
        final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult =
            generateNamespaceResizeActions(logPrefix, nsToCommCapacityChangeMap, originalEntities, projectedEntities);
        stopWatch.stop();
        logger.info("{} Finished generating {} resource quota resizing actions on {} namespaces in {} ms.",
            logPrefix, namespaceQuotaAnalysisResult.getNamespaceQuotaResizeActions().size(),
            namespaceQuotaAnalysisResult.getNamespacesWithActions().size(), stopWatch.getTime(TimeUnit.MILLISECONDS));
        return namespaceQuotaAnalysisResult.getNamespaceQuotaResizeActions();
    }

    /**
     * Get map of namespace to capacity changes of corresponding commodities based on container
     * resizing actions.
     *
     * @param logPrefix         Given log prefix.
     * @param actions           List of actions generated from previous analysis.
     * @param projectedEntities Map of projected topology entities.
     * @param isPlan            True if current topology is plan.
     * @return Map of namespace OID to map of capacity changes of corresponding commodities.
     */
    private Map<Long, Map<Integer, Float>> getNSToCommCapacityChangeMap(@Nonnull final String logPrefix,
                                                                        @Nonnull final List<Action> actions,
                                                                        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
                                                                        final boolean isPlan) {
        final Map<Long, Map<Integer, Float>> nsToCommCapacityChangeMap = new HashMap<>();
        // Cache to store container to namespace OID map.
        final Map<Long, Long> containerToNamespaceMap = new HashMap<>();
        actions.stream().filter(action -> action.getInfo().hasResize())
            .map(action -> action.getInfo().getResize())
            // Only take container resizing actions on certain resource commodities into account.
            .filter(resize -> resize.getTarget().getType() == EntityType.CONTAINER_VALUE
                && resourceToQuotaCommodityMap.containsKey(resize.getCommodityType().getType()))
            .forEach(resize -> {
                final long containerOID = resize.getTarget().getId();
                final ProjectedTopologyEntity container = projectedEntities.get(containerOID);
                if (container == null) {
                    logger.error("{} Container {} is not found in projectedEntities map", logPrefix, containerOID);
                    return;
                }
                final long namespaceOID = containerToNamespaceMap.computeIfAbsent(containerOID,
                    k -> getNamespaceOID(container, projectedEntities));
                if (namespaceOID == -1) {
                    logger.error("{} Container {} has no namespace", logPrefix, containerOID);
                    return;
                }
                // In plan we take all container resizing actions into consideration to simulate the
                // result;
                // in real time we take only container resizing up actions.
                final float capacityChange = resize.getNewCapacity() - resize.getOldCapacity();
                final Integer mappedQuotaCommodity = resourceToQuotaCommodityMap.get(resize.getCommodityType().getType());
                if (mappedQuotaCommodity != null && (isPlan || capacityChange > 0)) {
                    Map<Integer, Float> commCapacityChangeMap =
                        nsToCommCapacityChangeMap.computeIfAbsent(namespaceOID, k -> new HashMap<>());
                    commCapacityChangeMap.compute(mappedQuotaCommodity, (k, v) -> v == null ? capacityChange : v + capacityChange);
                }
            });
        return nsToCommCapacityChangeMap;
    }

    /**
     * Perform BFS starting from container to find namespace OID. Return -1 if namespace is not found.
     *
     * @param container         Given container entity from which namespace provider will be retrieved.
     * @param projectedEntities Given map of projected entity OID to projectedTopologyEntity.
     * @return Namespace OID from given container entity. -1 if namespace is not found.
     */
    private long getNamespaceOID(@Nonnull final ProjectedTopologyEntity container,
                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
        Queue<ProjectedTopologyEntity> queue = new LinkedList<>();
        queue.offer(container);
        while (!queue.isEmpty()) {
            ProjectedTopologyEntity entity = queue.poll();
            for (CommoditiesBoughtFromProvider comm : entity.getEntity().getCommoditiesBoughtFromProvidersList()) {
                if (!comm.hasProviderId()) {
                    continue;
                }
                if (comm.hasProviderEntityType() && comm.getProviderEntityType() == EntityType.NAMESPACE_VALUE) {
                    return comm.getProviderId();
                }
                ProjectedTopologyEntity provider = projectedEntities.get(comm.getProviderId());
                if (provider != null) {
                    if (provider.getEntity().getEntityType() == EntityType.NAMESPACE_VALUE) {
                        return provider.getEntity().getOid();
                    } else if (quotaResellerEntityTypes.contains(provider.getEntity().getEntityType())) {
                        queue.offer(provider);
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Generate namespace resizing up actions based on commodity capacity changes from container resizing
     * actions. If the increase of container capacity plus namespace current quota usage is larger than
     * current quota capacity, generated quota resizing up action and update projected commodity capacity
     * on this namespace.
     *
     * @param logPrefix                 Given log prefix.
     * @param nsToCommCapacityChangeMap Map of namespace OID to map of capacity changes of corresponding commodities.
     * @param originalEntities          Map of entity OID to original topologyEntityDTO.
     * @param projectedEntities         Map of entity OID to projectedTopologyEntity.
     * @return {@link NamespaceQuotaAnalysisResult}.
     */
    private NamespaceQuotaAnalysisResult generateNamespaceResizeActions(@Nonnull final String logPrefix,
                                                                        @Nonnull final Map<Long, Map<Integer, Float>> nsToCommCapacityChangeMap,
                                                                        @Nonnull final Map<Long, TopologyEntityDTO> originalEntities,
                                                                        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
        final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult = new NamespaceQuotaAnalysisResult();
        nsToCommCapacityChangeMap.forEach((nsOID, commCapacityChangeMap) -> {
            // nsOID is guaranteed to exist in nsToCommCapacityChangeMap based on previous logic.
            ProjectedTopologyEntity.Builder projNSEntityBuilder = projectedEntities.get(nsOID).toBuilder();
            TopologyEntityDTO origNS = originalEntities.get(nsOID);
            if (origNS == null) {
                logger.error("{} Namespace {} is not found in original entities map", logPrefix, nsOID);
                return;
            }
            projNSEntityBuilder.getEntityBuilder().getCommoditySoldListBuilderList()
                .forEach(projectedComm -> {
                    final int commodityType = projectedComm.getCommodityType().getType();
                    final Float capacityChange = commCapacityChangeMap.get(commodityType);
                    if (capacityChange != null) {
                        origNS.getCommoditySoldListList().stream()
                            .filter(c -> c.getCommodityType().getType() == commodityType)
                            .findAny()
                            .ifPresent(origComm -> {
                                float newCapacity = (float)origComm.getUsed() + capacityChange;
                                // Generate namespace resize action only if new capacity is larger than current
                                // capacity.
                                if (DoubleMath.fuzzyCompare(newCapacity, origComm.getCapacity(), COMPARISON_EPSILON) > 0) {
                                    Action namespaceResizeAction =
                                        generateNamespaceResizeAction(origNS,
                                            projectedComm.getCommodityType(), (float)origComm.getCapacity(),
                                            newCapacity);
                                    namespaceQuotaAnalysisResult.addNamespaceResizeAction(namespaceResizeAction);
                                    // Update namespace commodity capacity. Note that commodity capacity should
                                    // be updated after generating resize action
                                    projectedComm.setCapacity(newCapacity);
                                }
                            });
                    }
                });
            // Update projected namespace entity in projectedEntities map.
            projectedEntities.put(nsOID, projNSEntityBuilder.build());
        });
        return namespaceQuotaAnalysisResult;
    }

    private Action generateNamespaceResizeAction(@Nonnull final TopologyEntityDTO namespace,
                                                 final TopologyDTO.CommodityType commodityType,
                                                 final float oldCapacity, final float newCapacity) {
        final Explanation.Builder expBuilder = Explanation.newBuilder();
        ResizeExplanation.Builder resizeExplanation = ResizeExplanation.newBuilder()
            // startUtilization and endUtilization are deprecated but required fields.
            .setDeprecatedStartUtilization(oldCapacity)
            .setDeprecatedEndUtilization(newCapacity);
        expBuilder.setResize(resizeExplanation);

        final Action.Builder action = Action.newBuilder()
            // Assign a unique ID to each generated action.
            .setId(IdentityGenerator.next())
            // importance is deprecated but required field.
            .setDeprecatedImportance(-1.0d)
            .setExplanation(expBuilder.build())
            // TODO Set namespace resource quota resizing executable false for now.
            // Will set it to true after supporting action execution in kubeturbo probe side
            .setExecutable(false);

        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(namespace.getOid())
                .setType(namespace.getEntityType())
                .setEnvironmentType(namespace.getEnvironmentType())
                .build())
            .setNewCapacity(newCapacity)
            .setOldCapacity(oldCapacity)
            .setCommodityType(commodityType)
            .setCommodityAttribute(CommodityAttribute.CAPACITY);

        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        infoBuilder.setResize(resizeBuilder.build());

        action.setInfo(infoBuilder);
        return action.build();
    }

    /**
     * Wrapper class for namespace quota analysis result, including list of generated namespace quota
     * resizing actions and set of namespaces with actions.
     */
    private static class NamespaceQuotaAnalysisResult {
        /**
         * Set of namespaces with resize actions.
         */
        private final Set<Long> namespacesWithActions;
        /**
         * List of namespace quota resize actions.
         */
        private final List<Action> namespaceQuotaResizeActions;

        private NamespaceQuotaAnalysisResult() {
            this.namespacesWithActions = new HashSet<>();
            this.namespaceQuotaResizeActions = new ArrayList<>();
        }

        private void addNamespaceResizeAction(@Nonnull final Action nsResizeAction) {
            namespaceQuotaResizeActions.add(nsResizeAction);
            namespacesWithActions.add(nsResizeAction.getInfo().getResize().getTarget().getId());
        }

        private Set<Long> getNamespacesWithActions() {
            return namespacesWithActions;
        }

        private List<Action> getNamespaceQuotaResizeActions() {
            return namespaceQuotaResizeActions;
        }
    }
}
