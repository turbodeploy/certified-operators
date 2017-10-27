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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
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
    private final GroupExpander groupExpander;
    private final Duration supplyChainFetcherTimeoutSeconds;

    public SupplyChainFetcher(@Nonnull final Channel supplyChainChannel,
                              @Nonnull final Channel entitySeverityChannel,
                              @Nonnull final RepositoryApi repositoryApi,
                              @Nonnull final GroupExpander groupExpander,
                              @Nonnull final Duration supplyChainFetcherTimeoutSeconds) {
        Objects.requireNonNull(supplyChainChannel);
        Objects.requireNonNull(entitySeverityChannel);

        // create a non-blocking stub to query the supply chain from the Repository component
        this.supplyChainRpcService = SupplyChainServiceGrpc.newStub(supplyChainChannel);

        this.severityRpcService = EntitySeverityServiceGrpc.newBlockingStub(entitySeverityChannel);
        this.repositoryApi = repositoryApi;
        this.groupExpander = groupExpander;
        this.supplyChainFetcherTimeoutSeconds = supplyChainFetcherTimeoutSeconds;
    }

    public OperationBuilder newOperation() {
        return new OperationBuilder();
    }

    /**
     * Define a OperationBuilder class to simplify creating a {@link SupplyChainFetchOperation}.
     *
     * None of the parameters are required.
     */
    public class OperationBuilder {

        // all fields are optional; see the setter for each field for a description
        private long topologyContextId;
        private final Set<String> seedUuids = Sets.newHashSet();
        private final List<String> entityTypes = Lists.newLinkedList();
        private EnvironmentType environmentType;
        private SupplyChainDetailType supplyChainDetailType;
        private Boolean includeHealthSummary = false;

        /**
         * The seed UUID to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param seedUuid a single UUID to serve as the seed for the supplychain generation
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @SuppressWarnings("SameParameterValue")
        OperationBuilder addSeedUuid(@Nonnull final String seedUuid) {
            seedUuids.add(seedUuid);
            return this;
        }

        /**
         * The seed UUIDs to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param uuids a list of uuids, each of which will be the seed of a supplychain; the result
         *              is the union of the supplychains from each seed
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder addSeedUuids(@Nullable Collection<String> uuids) {
            if (uuids != null) {
                this.seedUuids.addAll(uuids);
            }
            return this;
        }

        /**
         * the topologyContext in which to perform the supplychain lookup - default is the Live Topology
         * @param topologyContextId the topologyContextId on which the supplychain operations should
         *                          be performed - default is the Live Topology
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder topologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return this;
        }

        /**
         * A list of service entity types to include in the answer - default is all entity types.
         * 'null' or the empty list indicates no filtering; all entity types will be included.
         *
         * @param entityTypes a list of the entity types to be included in the result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder entityTypes(@Nullable List<String> entityTypes) {
            if (entityTypes != null) {
                this.entityTypes.addAll(entityTypes);
            }
            return this;
        }

        /**
         * Limit the response to service entities in this environment e.g. ON_PREM, CLOUD, HYBRID
         * - default is all environments.
         *
         * NOTE:  this setting is not currently supported in XL
         *
         * @param environmentType what environment to limit the responses to
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder environmentType(@Nullable EnvironmentType environmentType) {
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
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder supplyChainDetailType(@Nullable SupplyChainDetailType supplyChainDetailType) {
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
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public OperationBuilder includeHealthSummary(boolean includeHealthSummary) {
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
                        groupExpander, supplyChainFetcherTimeoutSeconds).fetch();
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
        private final Set<String> seedUuids;
        private final List<String> entityTypes;
        private final EnvironmentType environmentType;
        private final SupplyChainDetailType supplyChainDetailType;
        private final Boolean includeHealthSummary;
        private final SupplyChainServiceStub supplyChainRpcService;
        private final EntitySeverityServiceBlockingStub severityRpcService;
        private final GroupExpander groupExpander;

        private final CompletableFuture<SupplychainApiDTO> supplyChainFuture;

        private final Duration supplyChainFetcherTimeoutSeconds;

        private final SupplychainApiDTO supplychainApiDTO;
        private final RepositoryApi repositoryApi;

        private boolean actionOrchestratorAvailable;


        private SupplyChainFetchOperation(@Nullable Long topologyContextId,
                                          @Nullable Set<String> seedUuids,
                                          @Nullable List<String> entityTypes,
                                          @Nullable EnvironmentType environmentType,
                                          @Nullable SupplyChainDetailType supplyChainDetailType,
                                          boolean includeHealthSummary,
                                          @Nonnull SupplyChainServiceStub supplyChainRpcService,
                                          @Nonnull EntitySeverityServiceBlockingStub severityRpcService,
                                          @Nonnull RepositoryApi repositoryApi,
                                          @Nonnull GroupExpander groupExpander,
                                          @Nonnull Duration supplyChainFetcherTimeoutSeconds) {
            this.topologyContextId = topologyContextId;
            this.seedUuids = seedUuids;
            this.entityTypes = entityTypes;
            this.environmentType = environmentType;
            this.supplyChainDetailType = supplyChainDetailType;
            this.includeHealthSummary = includeHealthSummary;


            this.supplyChainRpcService = supplyChainRpcService;
            this.severityRpcService = severityRpcService;
            this.repositoryApi = repositoryApi;
            this.groupExpander = groupExpander;

            this.supplyChainFetcherTimeoutSeconds = supplyChainFetcherTimeoutSeconds;

            // initialize a future for waiting on the severity calculation
            this.supplyChainFuture = new CompletableFuture<>();

            // prepare the "answer" DTO
            supplychainApiDTO = new SupplychainApiDTO();
            supplychainApiDTO.setSeMap(new HashMap<>());

            actionOrchestratorAvailable = true;
        }

        /**
         * Fetch the requested supply chain in a blocking fashion, waiting at most the duration
         * of the timeout. Use the option 'includeHealthSummary' during the result processing
         * to determine what sort of results to return - health information (from the
         * {@link EntitySeverityServiceGrpc}) vs. the individual ServiceEntities.
         *
         * Make use of the 'entityTypes' parameter, if specified, to to restrict the returned
         * {@link SupplychainEntryDTO}s.
         *
         * @return The {@link SupplychainApiDTO} populated with the supply chain search results.
         */
        SupplychainApiDTO fetch() throws InterruptedException, ExecutionException, TimeoutException {

            final SupplyChainRequest.Builder requestBuilder = SupplyChainRequest.newBuilder();

            // if list of seed uuids has limited scope,then expand it; if global scope, don't expand
            if (UuidMapper.hasLimitedScope(seedUuids)) {
                // expand any groups in the input list of seeds
                Set<String> expandedUuids = groupExpander.expandUuids(seedUuids).stream()
                        .map(l -> Long.toString(l))
                        .collect(Collectors.toSet());
                // empty expanded list?  If so, return immediately
                if (expandedUuids.isEmpty()) {
                    SupplychainApiDTO emptySupplyChain = new SupplychainApiDTO();
                    emptySupplyChain.setSeMap(Collections.emptyMap());
                    return emptySupplyChain;
                }
                // otherwise add the expanded list of seed uuids to the request
                requestBuilder.addAllStartingEntityOid(expandedUuids.stream()
                        .map(Long::valueOf)
                        .collect(Collectors.toList()));
            }

            // If entityTypes is specified, include that in the request
            if (CollectionUtils.isNotEmpty(entityTypes)) {
                requestBuilder.addAllEntityTypesToInclude(entityTypes);
            }

            SupplyChainRequest request = requestBuilder.build();

            supplyChainRpcService.getSupplyChain(request, this);

            return supplyChainFuture.get(supplyChainFetcherTimeoutSeconds.getSeconds(),
                    TimeUnit.SECONDS);
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

            // fetch service entities, if requested
            final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS = new HashMap<>();
            if (supplyChainDetailType != null) {
                // fetch a map from member OID to optional<ServiceEntityApiDTO>, where the
                // optional is empty if the OID was not found; include severities
                Map<Long, Optional<ServiceEntityApiDTO>> serviceEntitiesFromRepository =
                        repositoryApi.getServiceEntitiesById(new HashSet<>(memberOidsList));
                // ignore the unknown OIDs for now...perhaps should complain in the future
                serviceEntityApiDTOS.putAll(serviceEntitiesFromRepository.entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(entry -> Long.toString(entry.getKey()),
                                entry -> entry.getValue().get())));
            }

            final Map<Severity, Long> severities = new HashMap<>();
            if (includeHealthSummary || supplyChainDetailType != null) {
                // fetch severities, either to include in a health summary or to decorate SE's
                try {
                    logger.debug("Collecting severities for {}", supplyChainNode.getEntityType());

                    // If we have already determined the AO is unavailable, avoid lots of other calls to the AO that
                    // will likely almost certainly fail and delay the response to the client.
                    if (actionOrchestratorAvailable) {
                        severityRpcService.getEntitySeverities(severityRequest)
                                .forEachRemaining(entitySeverity -> {
                                    // If no severity is provided by the AO, default to normal
                                    Severity effectiveSeverity = entitySeverity.hasSeverity()
                                            ? entitySeverity.getSeverity()
                                            : Severity.NORMAL;
                                    // if the SE is being collected, update the severity
                                    if (supplyChainDetailType != null) {
                                        final String oidString = Long.toString(entitySeverity.getEntityId());
                                        if (serviceEntityApiDTOS.containsKey(oidString)) {
                                            serviceEntityApiDTOS
                                                    // fetch the ServiceEntityApiDTO for this ID
                                                    .get(oidString)
                                                    // update the severity
                                                    .setSeverity(effectiveSeverity.name());
                                        }
                                    }
                                    // if healthSummary is being created, increment the count
                                    if (includeHealthSummary) {
                                        severities.put(entitySeverity.getSeverity(), severities
                                                .getOrDefault(entitySeverity.getSeverity(), 0L) + 1L);
                                    }
                                });
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
                @Nonnull final Map<Severity, Long> severityMap,
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
                final Map<String, Integer> healthSummary = severityMap.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> ActionDTOUtil.getSeverityName(entry.getKey()),
                                entry -> entry.getValue().intValue()));
                supplyChainEntry.setHealthSummary(healthSummary);
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
