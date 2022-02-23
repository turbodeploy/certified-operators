package com.vmturbo.topology.processor.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * A utility class to process taints in a k8s cluster.
 */
public class K8sTaintProcessingUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String TAINT_PREFIX = "[k8s taint]";

    private K8sTaintProcessingUtil() {}

    /**
     * Collect taints from all nodes in a k8s cluster.
     *
     * @param scope The plan scope that includes the seed id of a container cluster
     * @param topology The topology map
     * @return The resulting taint collection
     */
    @Nonnull
    public static Set<String> collectTaints(@Nullable PlanScope scope,
                                            @Nonnull final Map<Long, Builder> topology) {
        // Locate the container cluster
        final TopologyEntity.Builder containerCluster = Optional.ofNullable(scope)
                .map(PlanScope::getScopeEntriesList)
                .flatMap(scopeEntries -> scopeEntries.stream()
                        .filter(scopeEntry -> scopeEntry.getClassName()
                                .equals(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.apiStr()))
                        .findAny())
                .map(PlanScopeEntry::getScopeObjectOid)
                .map(topology::get)
                .orElse(null);
        if (containerCluster == null) {
            logger.warn("Failed to locate the container cluster to collect taints");
            return Collections.emptySet();
        }
        // Iterate through all nodes in the cluster, and collect taints from these nodes into a set
        return containerCluster.getAggregatedEntities().stream()
                .filter(entity -> EntityType.VIRTUAL_MACHINE_VALUE == entity.getEntityType())
                .map(TopologyEntity::getTopologyEntityImpl)
                .map(K8sTaintProcessingUtil::collectTaintsFromNode)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Collect all taints from a node.
     * Taints on a node are identified as tags with [k8s taint] in the tag key.
     *
     * <p>Example 1:
     *
     * <p>Tag: [k8s taint] NoSchedule: node-role.kubernetes.io/master
     *
     * <p>Result: (node-role.kubernetes.io/master=:NoSchedule)
     *
     * <p>Example 2:
     *
     * <p>Tag: [k8s taint] NoSchedule: key1=value1
     *
     * <p>Result: (key1=value1:NoSchedule)
     *
     * <p>Example 3:
     *
     * <p>Tag: [k8s taint] NoSchedule: key2=value2,key4=value4
     *
     * <p>Result: (key2=value2:NoSchedule, key4=value4:NoSchedule)
     *
     * @param node The node to collect the taints from
     * @return A set that contains all the taints on the given node
     */
    private static Set<String> collectTaintsFromNode(@Nonnull final TopologyEntityImpl node) {
        final Set<String> taintsOnNode = new HashSet<>();
        node.getTags().getTagsMap().forEach((key, valueList) -> {
            if (!key.contains(TAINT_PREFIX)) {
                return;
            }
            final String taintEffect = key.replace(TAINT_PREFIX, "").trim();
            valueList.getValuesList().forEach(value -> {
                final String taintKeyValue = value.contains("=") ? value : value + "=";
                taintsOnNode.add(taintKeyValue + ":" + taintEffect);
            });
        });
        return taintsOnNode;
    }
}
