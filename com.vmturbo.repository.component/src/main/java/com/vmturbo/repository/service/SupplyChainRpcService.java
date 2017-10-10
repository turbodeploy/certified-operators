package com.vmturbo.repository.service;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * A gRPC service for retrieving supply chain information.
 */
public class SupplyChainRpcService extends SupplyChainServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private final SupplyChainService supplyChainService;

    private final GraphDBService graphDBService;

    private static final DataMetricSummary GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_global_supply_chain_duration_seconds")
        .withHelp("Duration in seconds it takes repository to retrieve global supply chain.")
        .build()
        .register();
    private static final DataMetricSummary SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_single_source_supply_chain_duration_seconds")
        .withHelp("Duration in seconds it takes repository to retrieve single source supply chain.")
        .build()
        .register();

    public SupplyChainRpcService(@Nonnull final GraphDBService graphDBService,
                                 @Nonnull final SupplyChainService supplyChainService) {
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.supplyChainService = Objects.requireNonNull(supplyChainService);
    }

    /**
     * Fetch supply chain information as determined by the given {@link SupplyChainRequest}.
     * The request may be to calculate either the supply chain for an individual ServiceEntity OID,
     * a merged supply chain derived from a starting list of ServiceEntityOIDs, or
     * a request for supply chain information for the entire topology.
     *
     * The supply chain information includes, organized by Entity Type:
     * <ul>
     *     <li>the entity type
     *     <li>the depth in the dependency tree of the Service Entities of this type
     *     <li>the list of entity types that provide resources to ServiceEntities of this type
     *     <li>the list of entity types that consume resources from ServiceEntities of this type
     *     <li>the OIDs of the ServiceEntities of this type in the supplychain
     * </ul>
     *
     * @param request the request indicating the OIDs for the service entities from which
     *                the supply chain should be calculated; and the topology context ID
     *                identifying from which topology, either the Live Topology or a Plan Topology,
     *                the supply chain information should be drawn, and optional entityType filter
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     *                         returned
     */
    @Override
    public void getSupplyChain(SupplyChainRequest request,
                               StreamObserver<SupplyChainNode> responseObserver) {
        final Optional<Long> contextId = request.hasContextId() ?
            Optional.of(request.getContextId()) : Optional.empty();

        if (request.getStartingEntityOidCount() > 0) {
            getMultiSourceSupplyChain(request.getStartingEntityOidList(),
                    request.getEntityTypesToIncludeList(), contextId,
                    responseObserver);
        } else {
            getGlobalSupplyChain(request.getEntityTypesToIncludeList(), contextId, responseObserver);
        }
    }

    /**
     * Get the global supply chain. While technically not a supply chain, return a stream of the
     * same supply chain information ({@link SupplyChainNode} calculated over all the
     * ServiceEntities in the given topology context. If requested, restrict the supply chain
     * information to entities from a given list of entityTypes.
     *
     * @param entityTypesToIncludeList if given and non-empty, then restrict supply chain nodes
     *                                 returned to the entityTypes listed here
     * @param contextId the unique identifier for the topology context from which the supply chain
     *                  information should be derived
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getGlobalSupplyChain(@Nullable List<String> entityTypesToIncludeList,
                                      @Nonnull final Optional<Long> contextId,
                                      @Nonnull final StreamObserver<SupplyChainNode> responseObserver) {
        GLOBAL_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            supplyChainService.getGlobalSupplyChain(contextId.map(Object::toString))
                .subscribe(supplyChainNodes -> {
                    supplyChainNodes.values().stream()
                            // if entityTypes are to be limited, restrict to SupplyChainNode types in the list
                            .filter(supplyChainNode -> CollectionUtils.isEmpty(entityTypesToIncludeList)
                                    || entityTypesToIncludeList.contains(supplyChainNode.getEntityType()))
                            .forEach(responseObserver::onNext);
                    responseObserver.onCompleted();
                }, error -> responseObserver.onError(Status.INTERNAL.withDescription(
                    error.getMessage()).asException()));
        });
    }

    /**
     * Fetch the supply chain for each element in a list of starting ServiceEntity OIDs.
     * The result is a stream of {@link SupplyChainNode} elements, one per entity type
     * in the supply chain. If requested, restrict the supply chain information to entities from
     * a given list of entityTypes.
     *
     * The SupplyChainNodes returned represent the result of merging, without duplication,
     * the supply chains derived from each of the starting Vertex OIDs. The elements merged
     * into each SupplyChainNode are: connected_provider_types, connected_consumer_types,
     * and member_oids.
     *  @param startingVertexOids the list of the ServiceEntity OIDs to start with, generating the
     *                           supply
     * @param entityTypesToIncludeList if given and not empty, restrict the supply chain nodes
     *                                 to be returned to entityTypes in this list
     * @param contextId the unique identifier for the topology context from which the supply chain
     *                  information should be derived
     * @param responseObserver the gRPC response stream onto which each resulting SupplyChainNode is
     */
    private void getMultiSourceSupplyChain(@Nonnull final List<Long> startingVertexOids,
                                           @Nullable final List<String> entityTypesToIncludeList,
                                           @Nonnull final Optional<Long> contextId,
                                           @Nonnull final StreamObserver<SupplyChainNode> responseObserver) {
        final SupplyChainMerger supplyChainMerger = new SupplyChainMerger();

        startingVertexOids.stream()
            .map(oid -> getSingleSourceSupplyChain(oid, contextId))
            .forEach(supplyChainMerger::addSingleSourceSupplyChain);

        final MergedSupplyChain supplyChain = supplyChainMerger.merge();
        if (supplyChain.errors.isEmpty()) {
            supplyChain.getSupplyChainNodes().stream()
                    // if entityTypes are to be limited, restrict to SupplyChainNode types in the list
                    .filter(supplyChainNode -> CollectionUtils.isEmpty(entityTypesToIncludeList)
                            || entityTypesToIncludeList.contains(supplyChainNode.getEntityType()))
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.INTERNAL.withDescription(
                supplyChain.errors.stream()
                    .collect(Collectors.joining(", "))).asException());
        }
    }

    /**
     * Get the supply chain local to a specific starting node by walking the graph topology beginning
     * with the starting node. The search is done within the topology corresponding to the given
     * topology context ID, which might be the Live Topology or a Plan Topology.
     */
    private SingleSourceSupplyChain getSingleSourceSupplyChain(@Nonnull final Long startingVertexOid,
                                     @Nonnull final Optional<Long> contextId) {
        logger.debug("Getting a supply chain starting from {} in topology {}",
            startingVertexOid, contextId.map(Object::toString).orElse("DEFAULT"));
        final SingleSourceSupplyChain singleSourceSupplyChain = new SingleSourceSupplyChain();

        SINGLE_SOURCE_SUPPLY_CHAIN_DURATION_SUMMARY.startTimer().time(() -> {
            Either<String, Stream<SupplyChainNode>> supplyChain = graphDBService.getSupplyChain(
                contextId.map(Object::toString), startingVertexOid.toString());

            Match(supplyChain).of(
                Case(Right($()), v -> {
                    v.forEach(singleSourceSupplyChain::addSupplyChainNode);
                    return null;
                }),
                Case(Left($()), err -> {
                    singleSourceSupplyChain.setError(err);
                    return null;
                }));
        });
        return singleSourceSupplyChain;
    }

    /**
     * This class is used to accumulate supply chain information related to a single
     * starting vertex OID. It maintains the starting vertex OID, a list of
     * {@link SupplyChainNode}s generated during the graph walk, and (optionally) an
     * error message if an error is found during the walking process.
     */
    private static class SingleSourceSupplyChain {

        private final List<SupplyChainNode> supplyChainNodes = new ArrayList<>();
        private Optional<String> error = Optional.empty();

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
         * Remember an error message related to this Supply Chain generation process.
         *
         * @param error a String describing the error that has occurred
         */
        void setError(@Nonnull final String error) {
            this.error = Optional.of(error);
        }

        /**
         * Fetch an error message that has been saved, or Optional.empty() if none
         *
         * @return the error message that has been saved, or Optional.empty() if none
         */
        public Optional<String> getError() {
            return error;
        }
    }

    /**
     * A class to used to hold the individual {@link SupplyChainNode}s created by merging
     * the supply chain information for each individual supply chain. It also
     */
    private static class MergedSupplyChain {
        private final Collection<SupplyChainNode> supplyChainNodes;
        private final List<String> errors;

        MergedSupplyChain(@Nonnull final Collection<SupplyChainNode> supplyChainNodes,
                                 @Nonnull final List<String> errors) {
            this.supplyChainNodes = supplyChainNodes;
            this.errors = errors;
        }

        Collection<SupplyChainNode> getSupplyChainNodes() {
            return supplyChainNodes;
        }

        public List<String> getErrors() {
            return errors;
        }
    }

    /**
     * A class to handle a single merged {@link SupplyChainNode}. This class handles the merging
     * task, where a new individual SupplyChainNode is merged into the aggregate here. Note that,
     * as for the {@link SupplyChainNode} which will eventually be returned to the caller,
     * this MergedSupplyChainNode holds the information pertaining to a single EntityType.
     */
    private static class MergedSupplyChainNode {
        private final Set<Long> memberOids;
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
            this.memberOids = new HashSet<>(supplyChainNode.getMemberOidsCount());
            this.connectedConsumerTypes = new HashSet<>(supplyChainNode.getConnectedConsumerTypesCount());
            this.connectedProviderTypes = new HashSet<>(supplyChainNode.getConnectedProviderTypesCount());
            this.supplyChainDepth = supplyChainNode.hasSupplyChainDepth() ?
                Optional.of(supplyChainNode.getSupplyChainDepth()) :
                Optional.empty();
            this.entityType = supplyChainNode.getEntityType();
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
            memberOids.addAll(supplyChainNode.getMemberOidsList());
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
                .addAllMemberOids(memberOids)
                .addAllConnectedConsumerTypes(connectedConsumerTypes)
                .addAllConnectedProviderTypes(connectedProviderTypes);
            supplyChainDepth.ifPresent(builder::setSupplyChainDepth);

            return builder.build();
        }
    }

    /**
     * A class to merge all individual instances of {@link MergedSupplyChainNode} we've derived
     * from each starting Entity OID listed in the request. This class accepts and accumulates
     * individual {@link SingleSourceSupplyChain} instances, and then calculates the merged
     * supply chains when requested.
     */
    private static class SupplyChainMerger {
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

            supplyChains.forEach(supplyChain -> {
                if (supplyChain.getError().isPresent()) {
                    errors.add(supplyChain.getError().get());
                } else {
                    supplyChain.getSupplyChainNodes().forEach(node -> mergeNode(nodesByType, node));
                }
            });

            return new MergedSupplyChain(nodesByType.values().stream()
                    .map(MergedSupplyChainNode::toSupplyChainNode)
                    .collect(Collectors.toList()), errors);
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
    }
}
