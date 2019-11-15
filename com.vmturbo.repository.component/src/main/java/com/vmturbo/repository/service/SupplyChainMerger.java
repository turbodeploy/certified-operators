package com.vmturbo.repository.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.SetOnce;

/**
 * A class to merge all individual instances of {@link MergedSupplyChainNode} we've derived
 * from each starting Entity OID listed in the request. This class accepts and accumulates
 * individual {@link SingleSourceSupplyChain} instances, and then calculates the merged
 * supply chains when requested.
 */
public class SupplyChainMerger {
    private static final Logger logger = LogManager.getLogger();

    private final List<SingleSourceSupplyChain> supplyChains = new ArrayList<>();

    /**
     * Accumulate a {@link SingleSourceSupplyChain} for later merging.
     *
     * @param supplyChain an {@link SingleSourceSupplyChain} to record for later merging
     */
    void addSingleSourceSupplyChain(@Nonnull final SingleSourceSupplyChain supplyChain) {
        supplyChains.add(Objects.requireNonNull(supplyChain));
    }

    /**
     * Calculate the merged {@link SupplyChainNode}s, by Entity Type, for all the
     * {@link SingleSourceSupplyChain} instances we've accumulated.
     *
     * @return a single {@link MergedSupplyChain} containing all the individual
     * {@link MergedSupplyChainNode}s derived here
     */
    MergedSupplyChain merge() {
        final Map<String, MergedSupplyChainNode> nodesByType = new HashMap<>();
        final List<String> errors = new ArrayList<>();
        final Set<Long> notFound = new HashSet<>();

        supplyChains.forEach(supplyChain -> {
            if (supplyChain.getError().isPresent()) {
                if (supplyChain.getError().get() instanceof NoSuchElementException) {
                    notFound.addAll(supplyChain.getStartingVertexOid());
                } else {
                    errors.add(supplyChain.getError().get().getMessage());
                }
            } else {
                supplyChain.getSupplyChainNodes().forEach(node -> mergeNode(nodesByType, node));
            }
        });

        return new MergedSupplyChain(nodesByType.values().stream()
                .map(MergedSupplyChainNode::toSupplyChainNode)
                .collect(Collectors.toList()), notFound, errors);
    }

    /**
     * Merge a new SupplyChainNode into the curent set of {@link MergedSupplyChainNode}s.
     * Look up the previous MergedSupplyChainNode for the entity type of this one, create
     * a new MergedSupplyChainNode if necessary, and then merge the given {@link SupplyChainNode}
     * into it.
     *
     * @param nodesByType a map from Entity Type to the {@link MergedSupplyChainNode}
     *                    for that Entity Type.
     * @param supplyChainNode a new {@link SupplyChainNode} to be merged into the existing data
     */
    private void mergeNode(@Nonnull final Map<String, MergedSupplyChainNode> nodesByType,
                           @Nonnull final SupplyChainNode supplyChainNode) {
        final MergedSupplyChainNode mergedNode =
            nodesByType.computeIfAbsent(supplyChainNode.getEntityType(),
                type -> new MergedSupplyChainNode(supplyChainNode));

        mergedNode.mergeWith(supplyChainNode);
    }


    /**
     * This class is used to accumulate supply chain information related to a single
     * starting vertex OID. It maintains the starting vertex OID, a list of
     * {@link SupplyChainNode}s generated during the graph walk, and (optionally) an
     * error message if an error is found during the walking process.
     */
    public static class SingleSourceSupplyChain {

        private final List<SupplyChainNode> supplyChainNodes = new ArrayList<>();
        private final Set<Long> startingVertexOid;
        private final SetOnce<Throwable> error = new SetOnce<>();

        public SingleSourceSupplyChain(final Set<Long> startingVertexOid) {
            this.startingVertexOid = startingVertexOid;
        }

        /**
         * Remember a {@link SupplyChainNode} for later.
         *
         * @param node the {@link SupplyChainNode} to be remembered
         */
        void addSupplyChainNode(@Nonnull final SupplyChainNode node) {
            supplyChainNodes.add(Objects.requireNonNull(node));
        }

        /**
         * Return the list of {@link SupplyChainNode} elements that have been remembered
         *
         * @return a list of all of the {@link SupplyChainNode}s that have been stored
         */
        List<SupplyChainNode> getSupplyChainNodes() {
            return supplyChainNodes;
        }

        /**
         * Remove SupplyChainNodes for the given set of entity types. For example: we get supply
         * chain starting from BusinessAccount, but we don't want to show BusinessAccount node in
         * the supply chain, then we need to remove the BusinessAccount node and modify related nodes.
         *
         * @param repoEntityTypes the set of entity types {@link UIEntityType} to remove SupplyChainNode for
         */
        void removeSupplyChainNodes(@Nonnull Set<UIEntityType> repoEntityTypes) {
            final Set<String> entityTypes = repoEntityTypes.stream()
                .map(UIEntityType::apiStr)
                .collect(Collectors.toSet());

            final List<SupplyChainNode> newSupplyChainNodes = supplyChainNodes.stream()
                // e.g. remove BusinessAccount node
                .filter(supplyChainNode -> !entityTypes.contains(supplyChainNode.getEntityType()))
                .map(supplyChainNode -> {
                    final SupplyChainNode.Builder nodeBuilder = supplyChainNode.toBuilder();
                    // e.g. remove BusinessAccount from nodes providers list
                    nodeBuilder.clearConnectedProviderTypes();
                    nodeBuilder.addAllConnectedProviderTypes(
                        supplyChainNode.getConnectedProviderTypesList().stream()
                            .filter(type -> !entityTypes.contains(type))
                            .collect(Collectors.toList()));
                    // e.g. remove BusinessAccount from nodes consumers list
                    nodeBuilder.clearConnectedConsumerTypes();
                    nodeBuilder.addAllConnectedConsumerTypes(
                        supplyChainNode.getConnectedConsumerTypesList().stream()
                            .filter(type -> !entityTypes.contains(type))
                            .collect(Collectors.toList()));
                    return nodeBuilder.build();
                }).collect(Collectors.toList());
            supplyChainNodes.clear();
            supplyChainNodes.addAll(newSupplyChainNodes);
        }

        /**
         * Remember an error message related to this Supply Chain generation process.
         *
         * @param error a String describing the error that has occurred
         */
        void setError(@Nonnull final Throwable error) {
            this.error.trySetValue(error);
        }

        /**
         * Fetch an error message that has been saved, or Optional.empty() if none
         *
         * @return the error message that has been saved, or Optional.empty() if none
         */
        public Optional<Throwable> getError() {
            return error.getValue();
        }

        @Nonnull
        public Set<Long> getStartingVertexOid() {
            return startingVertexOid;
        }
    }

    /**
     * A class to used to hold the individual {@link SupplyChainNode}s created by merging
     * the supply chain information for each individual supply chain. It also
     */
    public static class MergedSupplyChain {
        private final Collection<SupplyChainNode> supplyChainNodes;
        private final Set<Long> notFoundIds;
        private final List<String> errors;

        MergedSupplyChain(@Nonnull final Collection<SupplyChainNode> supplyChainNodes,
                          @Nonnull final Set<Long> notFoundIds,
                          @Nonnull final List<String> errors) {
            this.supplyChainNodes = supplyChainNodes;
            this.notFoundIds = notFoundIds;
            this.errors = errors;
        }

        /**
         * Get the final merged supply chain.
         * @param entityTypesToIncludeList The entity types to include in the supply chain.
         *                                 Empty means include all entity types.
         * @return The {@link SupplyChain} protobuf describing the merged supply chain.
         * @throws MergedSupplyChainException If there were error smerging the individual supply
         * chains into the final version.
         */
        @Nonnull
        public SupplyChain getSupplyChain(@Nonnull final List<String> entityTypesToIncludeList)
                throws MergedSupplyChainException {
            if (!errors.isEmpty()) {
                throw new MergedSupplyChainException(errors);
            }

            final SupplyChain.Builder retBuilder = SupplyChain.newBuilder()
                .addAllMissingStartingEntities(notFoundIds);

            supplyChainNodes.stream()
                // It's okay to use contains on a list here, because the list will be small.
                .filter(node -> entityTypesToIncludeList.isEmpty() || entityTypesToIncludeList.contains(node.getEntityType()))
                .forEach(retBuilder::addSupplyChainNodes);

            return retBuilder.build();
        }
    }

    /**
     * Exception thrown if merging supply chains encounters errors.
     */
    public static class MergedSupplyChainException extends Exception {
        MergedSupplyChainException(@Nonnull final List<String> errors) {
            super(String.join(", ", errors));
        }
    }

    /**
     * A class to handle a single merged {@link SupplyChainNode}. This class handles the merging
     * task, where a new individual SupplyChainNode is merged into the aggregate here. Note that,
     * as for the {@link SupplyChainNode} which will eventually be returned to the caller,
     * this MergedSupplyChainNode holds the information pertaining to a single EntityType.
     */
    public static class MergedSupplyChainNode {
        private final Map<Integer, Set<Long>> membersByState;
        private final Set<String> connectedConsumerTypes;
        private final Set<String> connectedProviderTypes;
        private Optional<Integer> supplyChainDepth;
        private final String entityType;

        /**
         * Create an instance of the class to aggregate multiple individual {@link SupplyChainNode}s
         * for the same Entity Type. Initialize this instance based on an initial individual
         * SupplyChainNode.
         *
         * @param supplyChainNode first the initial {@link SupplyChainNode} to aggregate; initialize
         *                        the internal variables for capturing the merged information later
         */
        MergedSupplyChainNode(@Nonnull final SupplyChainNode supplyChainNode) {
            this.connectedConsumerTypes = new HashSet<>(supplyChainNode.getConnectedConsumerTypesCount());
            this.connectedProviderTypes = new HashSet<>(supplyChainNode.getConnectedProviderTypesCount());
            this.supplyChainDepth = supplyChainNode.hasSupplyChainDepth() ?
                Optional.of(supplyChainNode.getSupplyChainDepth()) :
                Optional.empty();
            this.entityType = supplyChainNode.getEntityType();
            this.membersByState = new HashMap<>(supplyChainNode.getMembersByStateCount());
            supplyChainNode.getMembersByStateMap().forEach((state, membersForState) ->
                membersByState.put(state, new HashSet<>(membersForState.getMemberOidsList())));
        }

        /**
         * Aggregate information from an input {@link SupplyChainNode} into the supply chain
         * information already seen. We add to the memberOids, connectedConsumerTypes,
         * connectedProviderTypes, all without replacement. We track the supplyChainDepth,
         * and log a warning if the new supplyChainDepth does not match the original.
         *
         * @param supplyChainNode the new individual {@link SupplyChainNode} to aggregate into the
         *                        information saved from the previous SupplyChainNodes
         */
        void mergeWith(@Nonnull final SupplyChainNode supplyChainNode) {
            // Handle node depth
            if (supplyChainNode.hasSupplyChainDepth()) {
                if (supplyChainDepth.isPresent() &&
                    supplyChainDepth.get() != supplyChainNode.getSupplyChainDepth()) {
                    // if there's a mismatch, log the occurance and use the original
                    logger.warn("Mismatched supply chain depth on entity type {} when merging supply chains. " +
                            "Old depth {}, new depth {}",
                        supplyChainNode.getEntityType(),
                        supplyChainDepth.get(),
                        supplyChainNode.getSupplyChainDepth());
                } else {
                    this.supplyChainDepth = Optional.of(supplyChainNode.getSupplyChainDepth());
                }
            }

            // Merge by taking the union of known members and connected types with the new ones.
            supplyChainNode.getMembersByStateMap().forEach((state, membersForState) -> {
                final Set<Long> allMembersForState = membersByState.computeIfAbsent(state,
                    k -> new HashSet<>(membersForState.getMemberOidsCount()));
                allMembersForState.addAll(membersForState.getMemberOidsList());
            });
            connectedConsumerTypes.addAll(supplyChainNode.getConnectedConsumerTypesList());
            connectedProviderTypes.addAll(supplyChainNode.getConnectedProviderTypesList());
        }

        /**
         * Create an instance of {@link SupplyChainNode} to return to the caller based on the
         * values accumulated over all the individual instances of SupplyChainNode.
         *
         * @return a new {@link SupplyChainNode} initialized from the information accumulated from
         * all the individual SupplyChainNodes
         */
        SupplyChainNode toSupplyChainNode() {
            final SupplyChainNode.Builder builder = SupplyChainNode.newBuilder()
                .setEntityType(entityType)
                .addAllConnectedConsumerTypes(connectedConsumerTypes)
                .addAllConnectedProviderTypes(connectedProviderTypes);
            membersByState.forEach((state, membersForState) -> {
                builder.putMembersByState(state, MemberList.newBuilder()
                    .addAllMemberOids(membersForState)
                    .build());
            });
            supplyChainDepth.ifPresent(builder::setSupplyChainDepth);

            return builder.build();
        }
    }

}
