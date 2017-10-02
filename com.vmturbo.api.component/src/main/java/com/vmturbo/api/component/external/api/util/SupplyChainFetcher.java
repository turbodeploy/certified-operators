package com.vmturbo.api.component.external.api.util;

import java.net.NoRouteToHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.dto.SupplychainApiDTO;
import com.vmturbo.api.dto.SupplychainEntryDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceStub;

/**
 * A helper for fetching a supply chain as SupplyChainNodes are retrieved.
 * As SupplyChainNodes arrive, fetch the severities for those nodes.
 * These calls are currently synchronous but should probably be made asynchronous as an optimization.
 *
 * Completes the future input to the constructor when the supply chain is ready.
 */
public class SupplyChainFetcher {

    private final SupplyChainServiceStub supplyChainRpcService;
    private final EntitySeverityServiceBlockingStub severityRpcService;
    private final RepositoryApi repositoryApi;
    private final Duration supplyChainFetcherTimeoutSeconds;

    private static final Collection<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableList.of(
            "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    public SupplyChainFetcher(@Nonnull final Channel supplyChainChannel,
                              @Nonnull final Channel entitySeverityChannel,
                              @Nonnull final RepositoryApi repositoryApi,
                              @Nonnull final Duration supplyChainFetcherTimeoutSeconds) {
        Objects.requireNonNull(supplyChainChannel);
        Objects.requireNonNull(entitySeverityChannel);

        // create a non-blocking stub to query the supply chain from the Repository component
        this.supplyChainRpcService = SupplyChainServiceGrpc.newStub(supplyChainChannel);

        this.severityRpcService = EntitySeverityServiceGrpc.newBlockingStub(entitySeverityChannel);
        this.repositoryApi = repositoryApi;
        this.supplyChainFetcherTimeoutSeconds = supplyChainFetcherTimeoutSeconds;
    }

    public Builder newBuilder() {
        return new Builder();
    }

    /**
     * Define a Builder class to simplify creating a {@link SupplyChainFetchOperation}.
     *
     * None of the parameters are required.
     */
    public class Builder {

        // all fields are optional; see the setter for each field for a description
        private List<String> seedUuids;
        private long topologyContextId;
        private List<String> entityTypes;
        private EnvironmentType environmentType;
        private SupplyChainDetailType supplyChainDetailType;
        private Boolean includeHealthSummary = false;

        /**
         * The seed UUID to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param seedUuid a single UUID to serve as the seed for the supplychain generation
         * @return the flow-style Builder for this SupplyChainOperation
         */
        Builder seedUuid(String seedUuid) {
            this.seedUuids = Lists.newArrayList(seedUuid);
            return this;
        }

        /**
         * The seed UUIDs to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param uuids a list of uuids, each of which will be the seed of a supplychain; the result
         *              is the union of the supplychains from each seed
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder seedUuid(List<String> uuids) {
            this.seedUuids = uuids;
            return this;
        }

        /**
         * the topologyContext in which to perform the supplychain lookup - default is the Live Topology
         * @param topologyContextId the topologyContextId on which the supplychain operations should
         *                          be performed - default is the Live Topology
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder topologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return this;
        }

        /**
         * A list of service entity types to include in the answer - default is all entity types.
         * 'null' or the empty list indicates no filtering; all entity types will be included.
         *
         * @param entityTypes a list of the entity types to be included in the result
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder entityTypes(List<String> entityTypes) {
            this.entityTypes = entityTypes;
            return this;
        }

        /**
         * Limit the response to service entities in this environment e.g. ON_PREM, CLOUD, HYBRID
         * - default is all environments.
         *
         * NOTE:  this setting is not currently supported in XL
         *
         * @param environmentType what environment to limit the responses to
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder environmentType(EnvironmentType environmentType) {
            this.environmentType = environmentType;
            return this;
        }

        /**
         * Specify the level of service entity detail to include in the result
         * - default is no detail.
         *
         * NOTE:  this setting is not currently supported in XL.
         *
         * @param supplyChainDetailType what level of detail to include in the supplychain result
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder supplyChainDetailType(SupplyChainDetailType supplyChainDetailType) {
            this.supplyChainDetailType = supplyChainDetailType;
            return this;
        }

        /**
         * Should the 'health summary' be populated in the result - default is no.
         *
         * If the 'health summary' is not included, then return the full details of
         * all the ServiceEntities in the supplychain.
         *
         * @param includeHealthSummary should the healthSummary be included in the supplychain result
         * @return the flow-style Builder for this SupplyChainOperation
         */
        public Builder includeHealthSummary(boolean includeHealthSummary) {
            this.includeHealthSummary = includeHealthSummary;
            return this;
        }

        /**
         * Create a new instance of the internal class {@link SupplyChainFetchOperation}
         * to handle the multiple async steps requied to fetch the supply chain.
         *
         * @return an {@link SupplychainApiDTO} containing the form of the supplychain as requested
         * @throws OperationFailedException either the call to the SupplyChainRpcService or the
         * SeverityRpcService may fail.
         */
        public SupplychainApiDTO fetch() throws OperationFailedException {
            try {
                return new SupplyChainFetchOperation(
                        topologyContextId,
                        seedUuids,
                        entityTypes,
                        environmentType,
                        supplyChainDetailType,
                        includeHealthSummary,
                        supplyChainRpcService,
                        severityRpcService,
                        repositoryApi,
                        supplyChainFetcherTimeoutSeconds).fetch();
            } catch (TimeoutException |ExecutionException | InterruptedException e) {
                throw new OperationFailedException("Error fetching supply chain: " + toString() +
                        ": " + e.getMessage());
            }
        }
    }

    /**
     * Internal Class to handle a single SupplyChain fetch operation. Processes the stream of
     * {@link SupplyChainNode} values returned from the SupplyChain RpcService. Also handles
     * fetching the health status if required.
     *
     * If there are more than one seed UUID, then the supply chains from each are merged
     * by ServiceEntity. Note that the list of OIDs for each entity type are merged without
     * duplication. In order to avoid duplication, the OIDs from each supplychain are compiled
     * in the 'shadowOidMap' internally. This 'shadowOidMap' is also used to calculate the count
     * of entities of each type when populating the result {@link SupplychainApiDTO}.
     */
    private static class SupplyChainFetchOperation implements StreamObserver<SupplyChainNode> {

        private final Logger logger = LogManager.getLogger();

        private final Long topologyContextId;
        private final List<String> seedUuids;
        private final List<String> entityTypes;
        private final EnvironmentType environmentType;
        private final SupplyChainDetailType supplyChainDetailType;
        private final Boolean includeHealthSummary;
        private final SupplyChainServiceStub supplyChainRpcService;
        private final EntitySeverityServiceBlockingStub severityRpcService;

        private final CompletableFuture<SupplychainApiDTO> supplyChainFuture;

        private final Duration supplyChainFetcherTimeoutSeconds;

        private final SupplychainApiDTO supplychainApiDTO;
        private final RepositoryApi repositoryApi;

        // map from EntityType to ServiceEntity OIDs of the given type in the supply chain
        private final Multimap<String, Long> shadowOidMap;

        private boolean actionOrchestratorAvailable;


        private SupplyChainFetchOperation(@Nullable Long topologyContextId,
                                          @Nullable List<String> seedUuids,
                                          @Nullable List<String> entityTypes,
                                          @Nullable EnvironmentType environmentType,
                                          @Nullable SupplyChainDetailType supplyChainDetailType,
                                          @Nullable Boolean includeHealthSummary,
                                          @Nullable SupplyChainServiceStub supplyChainRpcService,
                                          @Nullable EntitySeverityServiceBlockingStub severityRpcService,
                                          @Nullable RepositoryApi repositoryApi,
                                          @Nullable Duration supplyChainFetcherTimeoutSeconds) {
            this.topologyContextId = topologyContextId;
            this.seedUuids = seedUuids;
            this.entityTypes = entityTypes;
            this.environmentType = environmentType;
            this.supplyChainDetailType = supplyChainDetailType;
            this.includeHealthSummary = includeHealthSummary;


            this.supplyChainRpcService = supplyChainRpcService;
            this.severityRpcService = severityRpcService;
            this.repositoryApi = repositoryApi;

            this.supplyChainFetcherTimeoutSeconds = supplyChainFetcherTimeoutSeconds;

            // initialize a future for waiting on the severity calculation
            this.supplyChainFuture = new CompletableFuture<>();

            // prepare the "answer" DTO
            supplychainApiDTO = new SupplychainApiDTO();
            supplychainApiDTO.setSeMap(new HashMap<>());

            actionOrchestratorAvailable = true;

            shadowOidMap = HashMultimap.create();
        }

        /**
         * Fetch the requested supply chain in a blocking fashion, waiting at most the duration
         * of the timeout. Use the option 'includeHealthSummary' during the result processing
         * to determine what sort of results to return - health information (from the
         * {@link EntitySeverityServiceGrpc}) vs. the individual ServiceEntities.
         *
         * TODO:  The supply chain request here does not make use of the 'entityTypes' parameter
         * that the caller sets to restrict the returned {@link SupplychainEntryDTO}s. We should
         * add the 'entityTypes' filter to the SupplyChainRequest.
         *
         * @return The {@link SupplychainApiDTO} populated with the supply chain search results.
         */
        SupplychainApiDTO fetch() throws InterruptedException, ExecutionException, TimeoutException {
            final SupplyChainRequest.Builder requestBuilder = SupplyChainRequest.newBuilder();

            // If global, do not specify a starting vertex
            if (!isGlobalSupplyChainSearch()) {
                requestBuilder.addAllStartingEntityOid(seedUuids.stream()
                        .map(Long::valueOf)
                        .collect(Collectors.toList()));
            }

            SupplyChainRequest request = requestBuilder.build();

            supplyChainRpcService.getSupplyChain(request, this);

            return supplyChainFuture.get(supplyChainFetcherTimeoutSeconds.getSeconds(),
                    TimeUnit.SECONDS);
        }

        /**
         * Detect whether this is a global or scoped supplychain search. If there are no seeds,
         * or a single seed in the list GLOBAL_SCOPE_SUPPLY_CHAIN (e.g. "Market"),
         * then this is a global supplychain search.
         *
         * @return true iff there are either no seed uuids, or a single seed UUID
         * in GLOBAL_SCOPE_SUPPLY_CHAIN
         */
        private boolean isGlobalSupplyChainSearch() {
            return seedUuids == null || (seedUuids.size() == 1
                    && GLOBAL_SCOPE_SUPPLY_CHAIN.contains(seedUuids.get(0)));
        }

        /**
         * Handle on supplychain response from the SupplyChain service. Tabulate the OIDs;
         * if requested, fetch the entity details from the Repository; also fetch the
         * Severity information.
         *
         * @param supplyChainNode the supplychain returned from the SupplyChainService for a single
         *                        seed UUID
         */
        @Override
        public void onNext(SupplyChainNode supplyChainNode) {
            final List<Long> memberOidsList = supplyChainNode.getMemberOidsList();
            final MultiEntityRequest severityRequest = MultiEntityRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .addAllEntityIds(memberOidsList)
                    .build();

            // remember these OIDs (without duplication)
            shadowOidMap.putAll(supplyChainNode.getEntityType(), memberOidsList);

            // fetch service entities, if requested
            Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS = Collections.emptyMap();
            if (supplyChainDetailType != null) {
                // fetch a map from member OID to optional<ServiceEntityApiDTO>, where the
                // optional is empty if the OID was not found
                Map<Long, Optional<ServiceEntityApiDTO>> serviceEntitiesFromRepository =
                        repositoryApi.getServiceEntitiesById(new HashSet<>(memberOidsList),
                                false);
                // ignore the unknown OIDs for now...perhaps should complain in the future
                serviceEntityApiDTOS = serviceEntitiesFromRepository.entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(entry -> Long.toString(entry.getKey()),
                                entry -> entry.getValue().get()));
            }

            // fetch severities
            Optional<Map<Severity, Long>> severities = Optional.empty();
            if (includeHealthSummary) {
                try {
                    logger.debug("Collecting severities for {}", supplyChainNode.getEntityType());

                    // If we have already determined the AO is unavailable, avoid lots of other calls to the AO that
                    // will likely almost certainly fail and delay the response to the client.
                    if (actionOrchestratorAvailable) {
                        final Iterable<EntitySeverity> severitiesIter =
                                () -> severityRpcService.getEntitySeverities(severityRequest);

                        severities = Optional.of(StreamSupport.stream(severitiesIter.spliterator(), false)
                                // If no severity is provided by the AO, default to normal
                                .map(entitySeverity -> entitySeverity.hasSeverity() ? entitySeverity.getSeverity() : Severity.NORMAL)
                                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
                    }
                } catch (RuntimeException e) {
                    logger.error("Error when fetching severities: ", e);
                    if (e.getCause() != null && (e.getCause() instanceof NoRouteToHostException)) {
                        actionOrchestratorAvailable = false;
                    }
                }
            }
            // add the results from this {@link SupplyChainNode} to the aggregate
            // {@link SupplychainApiDTO} result
            compileSupplyChainNode(supplyChainNode, severities, serviceEntityApiDTOS);
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Error fetching supply chain: ", throwable);
            supplyChainFuture.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            supplyChainFuture.complete(supplychainApiDTO);
        }

        /**
         * Compile the supply chain node and its associated severities into the {@link SupplychainApiDTO}
         * to be built by the fetcher.
         *
         @param node The {@link SupplyChainNode} to be compiled in.
          * @param severityMap A map showing how many entities in the node map to various kinds of severities.
         *@param serviceEntityApiDTOS if requested (see includeHealthSummary) them map from OID to
         *                             {@link ServiceEntityApiDTO} to return
         */
        private synchronized void compileSupplyChainNode(
                @Nonnull final SupplyChainNode node,
                @Nonnull final Optional<Map<Severity, Long>> severityMap,
                @Nonnull Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS) {
            logger.debug("Compiling results for {}", node.getEntityType());

            SupplychainEntryDTO supplyChainEntry = supplychainApiDTO.getSeMap().get(node.getEntityType());

            // add these SE's into the SupplyChainEntryDTO being built.
            if (supplyChainEntry == null) {
                // first SupplychainEntryDTO for this entity type; create one and just store the values
                supplyChainEntry = new SupplychainEntryDTO();
                supplychainApiDTO.getSeMap().put(node.getEntityType(), supplyChainEntry);
                supplyChainEntry.setConnectedConsumerTypes(new HashSet<>(node.getConnectedConsumerTypesList()));
                supplyChainEntry.setConnectedProviderTypes(new HashSet<>(node.getConnectedProviderTypesList()));
                supplyChainEntry.setDepth(node.getSupplyChainDepth());
                supplyChainEntry.setEntitiesCount(node.getMemberOidsCount());
                supplyChainEntry.setInstances(serviceEntityApiDTOS);

                // Set health summary if we were able to retrieve severities.
                if (severityMap.isPresent()) {
                    Map<Severity, Long> severities = severityMap.get();
                    // Entities with no actions are not provided a severity
                    final Map<String, Integer> healthSummary = severities.entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> ActionDTOUtil.getSeverityName(entry.getKey()),
                                    entry -> entry.getValue().intValue()));
                    supplyChainEntry.setHealthSummary(healthSummary);
                }
            }
        }


        /**
         * Define a verbose toString() method that includes the important
         * fields for this class - for advanced debugging.
         *
         * @return a debugging string with current values for the important fields.
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyContextId", topologyContextId)
                    .add("seedUuids", seedUuids)
                    .add("entityTypes", entityTypes)
                    .add("environmentType", environmentType)
                    .add("supplyChainDetailType", supplyChainDetailType)
                    .add("includeHealthSummary", includeHealthSummary)
                    .add("supplyChainFetcherTimeoutSeconds", supplyChainFetcherTimeoutSeconds)
                    .add("supplyChainFuture", supplyChainFuture)
                    .add("supplychainApiDTO", supplychainApiDTO)
                    .add("actionOrchestratorAvailable", actionOrchestratorAvailable)
                    .toString();
        }
    }
}
