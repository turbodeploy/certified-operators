package com.vmturbo.extractor.export;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseGraphEntity.DiscoveredTargetId;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Extract target info for an entity or group.
 */
public class TargetsExtractor {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyGraph<SupplyChainEntity> graph;

    private final ThinTargetCache targetCache;

    /**
     * Constructor for {@link TargetsExtractor}.
*
     * @param targetCache cache for all targets in the system
     * @param graph topology graph containing all entities
     */
    public TargetsExtractor(@Nonnull final ThinTargetCache targetCache,
                            @Nullable final TopologyGraph<SupplyChainEntity> graph) {
        this.targetCache = targetCache;
        this.graph = graph;
    }

    /**
     * Extract targets info for given entity.
     *
     * @param entityOid oid of the entity to get targets for
     * @return list of {@link Target}s
     */
    @Nullable
    public List<Target> extractTargets(long entityOid) {
        if (graph == null) {
            logger.debug("Topology graph is not available, skipping extracting targets for entity {}", entityOid);
            return null;
        }
        Optional<SupplyChainEntity> entity = graph.getEntity(entityOid);
        if (!entity.isPresent()) {
            return null;
        }
        List<DiscoveredTargetId> discoveredTargetIds = entity.get().getDiscoveredTargetIds();
        if (discoveredTargetIds.isEmpty()) {
            return null;
        }
        return createTargets(discoveredTargetIds.stream()
                .map(target -> Pair.of(target.getTargetId(), target.getVendorId())));
    }

    /**
     * Extract targets info for given entity.
     *
     * @param entity {@link TopologyEntityDTO}
     * @return list of {@link Target}s
     */
    @Nullable
    public List<Target> extractTargets(@Nonnull TopologyEntityDTO entity) {
        Map<Long, PerTargetEntityInformation> discoveredTargetDataMap =
                entity.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap();
        if (discoveredTargetDataMap.isEmpty()) {
            return null;
        }
        return createTargets(discoveredTargetDataMap.entrySet().stream().map(entry ->
                Pair.of(entry.getKey(),
                        entry.getValue().hasVendorId() ? entry.getValue().getVendorId() : null)));
    }

    /**
     * Extract targets info for given group.
     *
     * @param group {@link Grouping}
     * @return list of {@link Target}s
     */
    @Nullable
    public List<Target> extractTargets(@Nonnull Grouping group) {
        List<Long> targets = group.getOrigin().getDiscovered().getDiscoveringTargetIdList();
        if (targets.isEmpty()) {
            return null;
        }
        return createTargets(targets.stream().map(target -> Pair.of(target, null)));
    }

    private List<Target> createTargets(Stream<Pair<Long, String>> targets) {
        return targets
                // sort by target id to avoid hash changes in reporting
                .sorted(Comparator.comparingLong(Pair::getLeft))
                .map(entry -> createTarget(entry.getLeft(), entry.getRight()))
                .collect(Collectors.toList());
    }

    private Target createTarget(@Nonnull Long targetOid, @Nullable String entityVendorId) {
        final Target target = new Target();
        target.setOid(targetOid);
        targetCache.getTargetInfo(targetOid).ifPresent(targetInfo -> {
            target.setName(targetInfo.displayName());
            target.setType(targetInfo.probeInfo().type());
            //todo: uicategory?
            target.setCategory(targetInfo.probeInfo().category());
            target.setEntityVendorId(entityVendorId);
        });
        return target;
    }
}
