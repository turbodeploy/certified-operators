package com.vmturbo.topology.processor.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A utility class to process commodities in a k8s cluster.
 */
public final class K8sProcessingUtil {

    private static final String TAINT_PREFIX = "[k8s taint]";
    private static final String LABEL_PREFIX = "[k8s label]";

    private static final Map<String, CommodityType> prefixMap =
        new HashMap<String, CommodityType>() {{
            put(TAINT_PREFIX, CommodityType.TAINT);
            put(LABEL_PREFIX, CommodityType.LABEL);
        }};

    private K8sProcessingUtil() {}

    /**
     * Helper method to detect which prefix (if any) existing on the supplied value.
     *
     * @param value the value
     * @return the prefix
     */
    @Nullable
    private static String getPrefix(@Nonnull final String value) {
        if (value.contains(TAINT_PREFIX)) {
            return TAINT_PREFIX;
        } else if (value.contains(LABEL_PREFIX)) {
            return LABEL_PREFIX;
        } else {
            return null;
        }
    }

    /**
     * Helper method to generate a node commodity.
     *
     * @param commodityType the type of commodity
     * @param key the key of the commodity
     * @param value the value of the commodity
     * @return the commodity value
     */
    @Nullable
    private static String generateNodeCommodity(
        @Nonnull final CommodityType commodityType,
        @Nonnull final String key,
        @Nonnull final String value) {
        switch (commodityType) {
            case TAINT:
                final String taintValue = value.contains("=") ? value : value + "=";
                return taintValue + ":" + key;
            case LABEL:
                final String labelKey = key.contains("=") ? key : key + "=";
                return labelKey + value;
            default:
                return null;
        }
    }

    /**
     * Collect commodities from all nodes in a k8s cluster.
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
     * <p>Example 3:
     *
     * <p>Tag: [k8s label] key1=value1
     *
     * <p>Result: (key1=value1)
     *
     * @param containerCluster The container cluster of this plan
     * @return The resulting taint collection
     */
    @Nonnull
    public static Map<CommodityType, Set<String>> collectNodeCommodities(
        @Nonnull TopologyEntity.Builder containerCluster) {
        final Map<CommodityType, Set<String>> nodeCommodities = new HashMap<>();
        // Iterate through all nodes in the cluster, and collect commodities from these nodes
        containerCluster.getAggregatedEntities()
            .stream()
            .filter(entity -> EntityType.VIRTUAL_MACHINE_VALUE == entity.getEntityType())
            .map(TopologyEntity::getTopologyEntityImpl)
            .forEach(node -> node.getTags()
                .getTagsMap()
                .forEach((key, valueList) -> {
                    final String prefix = getPrefix(key);
                    if (prefix == null) {
                        return;
                    }
                    if (!prefixMap.containsKey(prefix)) {
                        return;
                    }
                    final CommodityType commodityType = prefixMap.get(prefix);
                    final String commodityKey = key.replace(prefix, "").trim();
                    valueList.getValuesList().forEach(value -> {
                        final String nodeCommodity = generateNodeCommodity(
                            commodityType, commodityKey, value);
                        if (nodeCommodity != null) {
                            nodeCommodities.computeIfAbsent(commodityType, k -> new HashSet<>())
                                    .add(nodeCommodity);
                        }
                    });
                }));

        return nodeCommodities;
    }
}
