package com.vmturbo.api.component.external.api.util;

import java.net.NoRouteToHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.grpc.Channel;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;

/**
 * A factory class for various {@link SupplychainFetcher}s.
 */
public class SupplyChainFetcherFactory {

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final EntitySeverityServiceBlockingStub severityRpcService;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final long realtimeTopologyContextId;

    public SupplyChainFetcherFactory(@Nonnull final Channel supplyChainChannel,
                                     @Nonnull final Channel entitySeverityChannel,
                                     @Nonnull final RepositoryApi repositoryApi,
                                     @Nonnull final GroupExpander groupExpander,
                                     final long realtimeTopologyContextId) {
        Objects.requireNonNull(supplyChainChannel);
        Objects.requireNonNull(entitySeverityChannel);

        this.supplyChainRpcService = SupplyChainServiceGrpc.newBlockingStub(supplyChainChannel)
                .withInterceptors(new JwtClientInterceptor());

        this.severityRpcService = EntitySeverityServiceGrpc.newBlockingStub(entitySeverityChannel);
        this.repositoryApi = repositoryApi;
        this.groupExpander = groupExpander;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Create a new {@link SupplyChainNodeFetcherBuilder} to retrieve raw {@link SupplyChainNode}s
     * matching various criteria. Users of this method can customize the builder and then call
     * {@link SupplyChainNodeFetcherBuilder#fetch()} to synchronously get the data.
     *
     * @return The {@link SupplyChainNodeFetcherBuilder}.
     */
    @Nonnull
    public SupplyChainNodeFetcherBuilder newNodeFetcher() {
        return new SupplyChainNodeFetcherBuilder();
    }

    /**
     * Create a new {@link SupplychainApiDTOFetcherBuilder} to retrieve a {@link SupplychainApiDTO}
     * containing the supply chain matching various criteria. Users of this method can customize
     * the builder and then call {@link SupplychainApiDTOFetcherBuilder#fetch()} to synchronously
     * get the data.
     *
     * @return The {@link SupplychainApiDTOFetcherBuilder}.
     */
    @Nonnull
    public SupplychainApiDTOFetcherBuilder newApiDtoFetcher() {
        return new SupplychainApiDTOFetcherBuilder();
    }

    /**
     * A builder for a {@link SupplychainNodeFetcher} that returns the raw
     * {@link SupplyChainNode}s, arranged by entity type.
     */
    public class SupplyChainNodeFetcherBuilder extends
            SupplyChainFetcherBuilder<SupplyChainNodeFetcherBuilder, Map<String, SupplyChainNode>> {

        @Override
        public Map<String, SupplyChainNode> fetch() throws OperationFailedException {
            try {
                return new SupplychainNodeFetcher(
                        topologyContextId,
                        seedUuids,
                        entityTypes,
                        environmentType,
                        supplyChainRpcService,
                        groupExpander,
                        enforceUserScope).fetch();
            } catch (InterruptedException|ExecutionException|TimeoutException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }

        @Override
        public Set<Long> fetchEntityIds() throws OperationFailedException {
            try {
                return
                    new SupplychainNodeFetcher(
                            topologyContextId, seedUuids, entityTypes, environmentType,
                            supplyChainRpcService, groupExpander, enforceUserScope)
                        .fetchEntityIds();
            } catch (InterruptedException|ExecutionException|TimeoutException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }
    }

    /**
     * A builder for a {@link SupplychainApiDTOFetcher} that returns a
     * {@link SupplychainApiDTO} representing the supply chain.
     */
    public class SupplychainApiDTOFetcherBuilder extends SupplyChainFetcherBuilder<SupplychainApiDTOFetcherBuilder, SupplychainApiDTO> {
        protected EntityDetailType entityDetailType;
        protected Boolean includeHealthSummary = false;

        /**
         * Specify the level of service entity detail to include in the result
         * - default is no detail.
         *
         * NOTE:  this setting is not currently supported in XL.
         *
         * @param entityDetailType what level of detail to include in the supplychain result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public SupplychainApiDTOFetcherBuilder entityDetailType(
                @Nullable final EntityDetailType entityDetailType) {
            this.entityDetailType = entityDetailType;
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
        @Nonnull
        public SupplychainApiDTOFetcherBuilder includeHealthSummary(
                final boolean includeHealthSummary) {
            this.includeHealthSummary = includeHealthSummary;
            return this;
        }

        @Override
        @Nonnull
        public SupplychainApiDTO fetch() throws OperationFailedException, InterruptedException {
            try {
                final SupplychainApiDTO dto = new SupplychainApiDTOFetcher(topologyContextId,
                    seedUuids, entityTypes, environmentType, entityDetailType, includeHealthSummary,
                    supplyChainRpcService, severityRpcService, repositoryApi, groupExpander,
                    enforceUserScope).fetch();
                return dto;
            } catch (ExecutionException | TimeoutException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }

        @Override
        @Nonnull
        public Set<Long> fetchEntityIds() throws OperationFailedException, InterruptedException {
            try {
                return
                    new SupplychainApiDTOFetcher(
                            topologyContextId, seedUuids, entityTypes, environmentType,
                            entityDetailType, includeHealthSummary, supplyChainRpcService,
                            severityRpcService, repositoryApi, groupExpander, enforceUserScope)
                        .fetchEntityIds();
            } catch (ExecutionException | TimeoutException e) {
                throw new OperationFailedException("Failed to fetch supply chain! Error: "
                        + e.getMessage());
            }
        }
    }

    /**
     * A builder class to simplify creating a {@link SupplychainFetcher}.
     *
     * None of the parameters are required.
     *
     * @param <B> The builder subtype, used to allow method chaining with common setter methods.
     * @param <T> The return type of the {@link SupplychainFetcher} the builder builds.
     *            For now, the builder doesn't actually return the operation itself, but returns
     *            the result of running the operation. In the future, if we want to allow running
     *            the operation asynchronously while the calling code does other things we should
     *            return the operation itself, and this parameter would be different.
     */
    private abstract class SupplyChainFetcherBuilder<B extends SupplyChainFetcherBuilder<B, T>, T> {

        // all fields are optional; see the setter for each field for a description
        protected long topologyContextId = realtimeTopologyContextId;

        protected final Set<String> seedUuids = Sets.newHashSet();

        protected final Set<String> entityTypes = Sets.newHashSet();

        protected boolean enforceUserScope = true;

        protected Optional<EnvironmentTypeEnum.EnvironmentType> environmentType = Optional.empty();

        /**
         * Synchronously fetch the supply chain with the parameters specified in the builder.
         *
         * @return The return type of the {@link SupplychainFetcher} being built.
         * @throws OperationFailedException If any of the calls/processing required for the fetch
         *                                  operation fail.
         * @throws InterruptedException If the thread is interrupted while waiting for the operation.
         */
        public abstract T fetch() throws OperationFailedException, InterruptedException;

        /**
         * Synchronously fetch the supply chain with the parameters specified in the builder
         * and return the ids of all the contained entities
         *
         * @return The set of ids of all contained entities.
         * @throws OperationFailedException If any of the calls/processing required for the fetch
         *                                  operation fail.
         * @throws InterruptedException If the thread is interrupted while waiting for the operation.
         */
        public abstract Set<Long> fetchEntityIds() throws OperationFailedException, InterruptedException;

        /**
         * The seed UUID to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param seedUuid a single UUID to serve as the seed for the supplychain generation
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @SuppressWarnings("SameParameterValue")
        public B addSeedUuid(@Nonnull final String seedUuid) {
            seedUuids.add(seedUuid);
            return (B)this;
        }

        /**
         * The seed UUIDs to start the supply chain generation from; may be SE, Group, Cluster.
         * The default is the entire topology.
         *
         * @param uuids a list of uuids, each of which will be the seed of a supplychain; the result
         *              is the union of the supplychains from each seed
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B addSeedUuids(@Nullable Collection<String> uuids) {
            if (uuids != null) {
                this.seedUuids.addAll(uuids);
            }
            return (B)this;
        }

        /**
         * the topologyContext in which to perform the supplychain lookup - default is the Live Topology
         * @param topologyContextId the topologyContextId on which the supplychain operations should
         *                          be performed - default is the Live Topology
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B topologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return (B)this;
        }

        /**
         * A list of service entity types to include in the answer - default is all entity types.
         * 'null' or the empty list indicates no filtering; all entity types will be included.
         *
         * @param entityTypes a list of the entity types to be included in the result
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        public B entityTypes(@Nullable List<String> entityTypes) {
            if (entityTypes != null) {
                this.entityTypes.addAll(entityTypes);
            }
            return (B)this;
        }

        /**
         * Limit the response to service entities in this environment e.g. ON_PREM, CLOUD, HYBRID
         * - default is all environments.
         *
         * @param environmentType what environment to limit the responses to
         * @return the flow-style OperationBuilder for this SupplyChainFetcher
         */
        @Nonnull
        public B apiEnvironmentType(@Nullable final com.vmturbo.api.enums.EnvironmentType environmentType) {
            // If the desired environment type is "HYBRID", we're looking for cloud OR on-prem,
            // which is the same as looking for all.
            if (environmentType != null) {
                this.environmentType = UIEnvironmentType.fromString(
                    environmentType.name()).toEnvType();
            }
            return (B)this;
        }

        @Nonnull
        public B environmentType(@Nullable final EnvironmentType environmentType) {
            this.environmentType = Optional.ofNullable(environmentType);
            return (B)this;
        }

        /**
         * Whether the results should be confined to the user access scope or not. If true (default)
         * and the requesting user has an access scope assigned, hen the supply chain will only
         * contain entities that are part of the user's entity access scope. If false, then all
         * entities in the supply chain will be returned.
         *
         * @param enforceUserScope
         * @return
         */
        @Nonnull
        public B enforceUserScope(final boolean enforceUserScope) {
            this.enforceUserScope = enforceUserScope;
            return (B)this;
        }

    }

    /**
     * Internal Class to handle a single SupplyChain fetch operation. Processes the {@link SupplyChain}
     * returned from the supply chain RPC service, and hands the nodes off to sub-classes for
     * processing.
     */
    private abstract static class SupplychainFetcher<T> {

        protected final Logger logger = LogManager.getLogger(getClass());

        private final long topologyContextId;

        protected final Set<String> seedUuids;

        private final Set<String> entityTypes;

        private final Optional<EnvironmentTypeEnum.EnvironmentType> environmentType;

        private final SupplyChainServiceBlockingStub supplyChainRpcService;

        private final GroupExpander groupExpander;

        protected final boolean enforceUserScope;

        private SupplychainFetcher(final long topologyContextId,
                                   @Nullable final Set<String> seedUuids,
                                   @Nullable final Set<String> entityTypes,
                                   @Nonnull final Optional<EnvironmentTypeEnum.EnvironmentType> environmentType,
                                   @Nonnull SupplyChainServiceBlockingStub supplyChainRpcService,
                                   @Nonnull GroupExpander groupExpander,
                                   final boolean enforceUserScope) {
            this.topologyContextId = topologyContextId;
            this.seedUuids = seedUuids;
            this.entityTypes = entityTypes;
            this.environmentType = environmentType;
            this.supplyChainRpcService = supplyChainRpcService;
            this.groupExpander = groupExpander;
            this.enforceUserScope = enforceUserScope;

        }

        public abstract T processSupplyChain(final List<SupplyChainNode> supplyChainNodes);

        public final T fetch() throws InterruptedException, ExecutionException, TimeoutException {
            return processSupplyChain(fetchSupplyChainNodes());
        }

        /**
         * Fetch the requested supply chain using {@link #fetch()} and then return the ids
         * of all the entities in the supply chain.
         *
         * @return the set of ids of all the entities in the supply chain.
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws TimeoutException
         */
        public final Set<Long> fetchEntityIds()
                throws InterruptedException, ExecutionException, TimeoutException {
            return
                fetchSupplyChainNodes().stream()
                    .map(SupplyChainNode::getMembersByStateMap)
                    .map(Map::values)
                    .flatMap(memberList ->
                        memberList.stream().map(MemberList::getMemberOidsList).flatMap(List::stream))
                    .collect(Collectors.toSet());
        }

        /**
         * Fetch the requested supply chain in a blocking fashion, waiting at most the duration
         * of the timeout.
         *
         * @return The {@link SupplychainApiDTO} populated with the supply chain search results.
         */
        final List<SupplyChainNode> fetchSupplyChainNodes()
                throws InterruptedException, ExecutionException, TimeoutException {

            final GetSupplyChainRequest.Builder requestBuilder = GetSupplyChainRequest.newBuilder();

            // if list of seed uuids has limited scope,then expand it; if global scope, don't expand
            if (UuidMapper.hasLimitedScope(seedUuids)) {
                // START Mad(ish) Hax.
                // Handle a very particular special case where we are asking for the supply chain
                // of a group, restricted to the entity type of the group (e.g. give me the supply
                // chain of Group 1 of PhysicalMachines, containing only PhysicalMachine nodes).
                // The request is, essentially, asking for the members of the group, so we don't need
                // to do any supply chain queries.
                //
                // The reason this even happens is because some information (e.g. grouped severities
                // for supply chain stats, or aspects for entities) is only available via the
                // supply chain API. In the long term there should be a better API to retrieve this
                // (e.g. some sort of "entity counts" API for grouped severities,
                //       and/or options on the /search API for aspects)
                if (seedUuids.size() == 1 && CollectionUtils.size(entityTypes) == 1) {
                    final String groupUuid = seedUuids.iterator().next();
                    final String desiredEntityType = entityTypes.iterator().next();
                    final Optional<Group> group = groupExpander.getGroup(groupUuid);
                    if (group.isPresent()) {
                        final String groupType = UIEntityType.fromType(
                                GroupProtoUtil.getEntityType(group.get())).apiStr();

                        if (groupType.equals(desiredEntityType)) {
                            return Collections.singletonList(SupplyChainNode.newBuilder()
                                .setEntityType(groupType)
                                .putMembersByState(EntityState.POWERED_ON_VALUE,
                                    MemberList.newBuilder()
                                        .addAllMemberOids(groupExpander.expandUuid(groupUuid))
                                        .build())
                                .build());
                        }
                    }
                }
                // END Mad(ish) Hax.

                // expand any groups in the input list of seeds
                Set<String> expandedUuids = groupExpander.expandUuids(seedUuids).stream()
                        .map(l -> Long.toString(l))
                        .collect(Collectors.toSet());
                // empty expanded list?  If so, return immediately
                if (expandedUuids.isEmpty()) {
                    return Collections.emptyList();
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

            environmentType.ifPresent(requestBuilder::setEnvironmentType);

            requestBuilder.setEnforceUserScope(enforceUserScope);

            GetSupplyChainRequest request = requestBuilder.build();

            final GetSupplyChainResponse response = supplyChainRpcService.getSupplyChain(request);
            if (!response.getSupplyChain().getMissingStartingEntitiesList().isEmpty()) {
                logger.warn("{} of {} seed entities were not found for the supply chain: {}.",
                    response.getSupplyChain().getMissingStartingEntitiesCount(),
                    CollectionUtils.size(seedUuids),
                    response.getSupplyChain().getMissingStartingEntitiesList());
            }
            return response.getSupplyChain().getSupplyChainNodesList();
        }

        protected long getTopologyContextId() {
            return topologyContextId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyContextId", topologyContextId)
                    .add("seedUuids", seedUuids)
                    .add("entityTypes", entityTypes)
                    .add("environmentType", environmentType)
                    .toString();
        }

    }

    /**
     * A {@link SupplychainFetcher} that returns the {@link SupplyChainNode}s retrieved from
     * the repository, arranged by entity type.
     *
     * The main use case for this fetcher is if you just want the members of a particular set
     * of entity types in the supply chain of some scope. If you want any related information
     * it's worth considering {@link SupplychainApiDTOFetcher}.
     */
    private static class SupplychainNodeFetcher extends SupplychainFetcher<Map<String, SupplyChainNode>> {

        private SupplychainNodeFetcher(final long topologyContextId,
                                       @Nullable final Set<String> seedUuids,
                                       @Nullable final Set<String> entityTypes,
                                       @Nonnull final Optional<EnvironmentType> environmentType,
                                       @Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                       @Nonnull final GroupExpander groupExpander,
                                       final boolean enforceUserScope) {
            super(topologyContextId, seedUuids, entityTypes,
                    environmentType, supplyChainRpcService, groupExpander, enforceUserScope);
        }

        @Nonnull
        public Map<String, SupplyChainNode> processSupplyChain(
                @Nonnull final List<SupplyChainNode> supplyChainNodes) {
            return supplyChainNodes.stream()
                .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));
        }
    }

    /**
     * A {@link SupplychainFetcher} that returns a {@link SupplychainApiDTO} ready for API/UI
     * consumption.
     *
     * Also handles fetching the health status if required.
     *
     * If there are more than one seed UUID, then the supply chains from each are merged
     * by ServiceEntity. Note that the list of OIDs for each entity type are merged without
     * duplication. In order to avoid duplication, the OIDs from each supplychain are compiled
     * in the 'shadowOidMap' internally. This 'shadowOidMap' is also used to calculate the count
     * of entities of each type when populating the result {@link SupplychainApiDTO}.
     */
    private static class SupplychainApiDTOFetcher extends SupplychainFetcher<SupplychainApiDTO> {

        private final EntityDetailType entityDetailType;

        private final EntitySeverityServiceBlockingStub severityRpcService;

        private final Boolean includeHealthSummary;

        private final RepositoryApi repositoryApi;

        private boolean actionOrchestratorAvailable;

        private SupplychainApiDTOFetcher(final long topologyContextId,
                                         @Nullable final Set<String> seedUuids,
                                         @Nullable final Set<String> entityTypes,
                                         @Nonnull final Optional<EnvironmentType> environmentType,
                                         @Nullable final EntityDetailType entityDetailType,
                                         final boolean includeHealthSummary,
                                         @Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                         @Nonnull final EntitySeverityServiceBlockingStub severityRpcService,
                                         @Nonnull final RepositoryApi repositoryApi,
                                         @Nonnull final GroupExpander groupExpander,
                                         final boolean enforceUserScope) {
            super(topologyContextId, seedUuids, entityTypes, environmentType, supplyChainRpcService,
                    groupExpander, enforceUserScope);
            this.entityDetailType = entityDetailType;
            this.includeHealthSummary = includeHealthSummary;
            this.severityRpcService = Objects.requireNonNull(severityRpcService);
            this.repositoryApi = Objects.requireNonNull(repositoryApi);

            actionOrchestratorAvailable = true;
        }

        /**
         * Handle on supplychain response from the SupplyChain service. Tabulate the OIDs;
         * if requested, fetch the entity details from the Repository; also fetch the
         * Severity information.
         *
         * Use the option 'includeHealthSummary' during the result processing
         * to determine what sort of results to return - health information (from the
         * {@link EntitySeverityServiceGrpc}) vs. the individual ServiceEntities.
         */
        @Override
        public SupplychainApiDTO processSupplyChain(@Nonnull final List<SupplyChainNode> supplyChainNodes) {
            final SupplychainApiDTO resultApiDTO = new SupplychainApiDTO();
            resultApiDTO.setSeMap(new HashMap<>());

            for (SupplyChainNode supplyChainNode : supplyChainNodes) {
                final Set<Long> memberOidsList = RepositoryDTOUtil.getAllMemberOids(supplyChainNode);

                // fetch service entities, if requested
                final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS = new HashMap<>();
                if (entityDetailType != null) {
                    // fetch a map from member OID to optional<ServiceEntityApiDTO>, where the
                    // optional is empty if the OID was not found; include severities
                    Map<Long, Optional<ServiceEntityApiDTO>> serviceEntitiesFromRepository =
                        repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(
                            memberOidsList).build());

                    // ignore the unknown OIDs for now...perhaps should complain in the future
                    serviceEntityApiDTOS.putAll(serviceEntitiesFromRepository.entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(entry -> Long.toString(entry.getKey()),
                            entry -> entry.getValue().get())));
                }

                final Map<Severity, Long> severities = new HashMap<>();
                if (includeHealthSummary || entityDetailType != null) {
                    // fetch severities, either to include in a health summary or to decorate SE's
                    try {
                        logger.debug("Collecting severities for {}", supplyChainNode.getEntityType());

                        // If we have already determined the AO is unavailable, avoid lots of other calls to the AO that
                        // will likely almost certainly fail and delay the response to the client.
                        if (actionOrchestratorAvailable) {
                            final MultiEntityRequest severityRequest = MultiEntityRequest.newBuilder()
                                .setTopologyContextId(getTopologyContextId())
                                .addAllEntityIds(memberOidsList)
                                .build();
                            if (entityDetailType != null) {
                                fetchEntitySeverities(severityRequest, serviceEntityApiDTOS, severities);
                            } else {
                                fetchSeverityCounts(severityRequest, severities);
                            }
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
                compileSupplyChainNode(supplyChainNode, severities, serviceEntityApiDTOS, resultApiDTO);
            }

            return resultApiDTO;
        }

        private void fetchSeverityCounts(@Nonnull final MultiEntityRequest severityCountRequest,
                                         @Nonnull final Map<Severity, Long> severities) {
            Preconditions.checkArgument(includeHealthSummary);

            final SeverityCountsResponse response =
                severityRpcService.getSeverityCounts(severityCountRequest);
            response.getCountsList().forEach(severityCount -> {
                final Severity severity = severityCount.getSeverity();
                final long currentCount = severities.getOrDefault(severity, 0L);
                severities.put(severity, currentCount + severityCount.getEntityCount());
            });

            final long currentNormalCount = severities.getOrDefault(Severity.NORMAL, 0L);
            severities.put(Severity.NORMAL,
                currentNormalCount + response.getUnknownEntityCount());
        }

        private void fetchEntitySeverities(@Nonnull final MultiEntityRequest entitySeverityRequest,
                                           @Nonnull final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS,
                                           @Nonnull final Map<Severity, Long> severities) {
            Objects.requireNonNull(entityDetailType);

            severityRpcService.getEntitySeverities(entitySeverityRequest).getEntitySeverityList()
                .forEach(entitySeverity -> {
                    // If no severity is provided by the AO, default to normal
                    Severity effectiveSeverity = entitySeverity.hasSeverity()
                        ? entitySeverity.getSeverity()
                        : Severity.NORMAL;
                    // if the SE is being collected, update the severity
                    final String oidString = Long.toString(entitySeverity.getEntityId());
                    if (serviceEntityApiDTOS.containsKey(oidString)) {
                        serviceEntityApiDTOS
                            // fetch the ServiceEntityApiDTO for this ID
                            .get(oidString)
                                // update the severity
                            .setSeverity(effectiveSeverity.name());
                    }

                    // if healthSummary is being created, increment the count
                    if (includeHealthSummary) {
                        severities.put(entitySeverity.getSeverity(), severities
                            .getOrDefault(entitySeverity.getSeverity(), 0L) + 1L);
                    }
                });
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
                @Nonnull final Map<String, ServiceEntityApiDTO> serviceEntityApiDTOS,
                @Nonnull final SupplychainApiDTO resultApiDTO) {
            logger.debug("Compiling results for {}", node.getEntityType());

            // This is thread-safe because we're doing it in a synchronized method.
            resultApiDTO.getSeMap().computeIfAbsent(node.getEntityType(), entityType -> {
                // first SupplychainEntryDTO for this entity type; create one and just store the values
                final SupplychainEntryDTO supplyChainEntry = new SupplychainEntryDTO();
                supplyChainEntry.setConnectedConsumerTypes(new HashSet<>(node.getConnectedConsumerTypesList()));
                supplyChainEntry.setConnectedProviderTypes(new HashSet<>(node.getConnectedProviderTypesList()));
                supplyChainEntry.setDepth(node.getSupplyChainDepth());
                supplyChainEntry.setInstances(serviceEntityApiDTOS);

                // Set health summary if we were able to retrieve severities.
                final Map<String, Integer> healthSummary = severityMap.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> ActionDTOUtil.getSeverityName(entry.getKey()),
                                entry -> entry.getValue().intValue()));
                supplyChainEntry.setHealthSummary(healthSummary);

                // Compile the entities count from the members-by-state map, since
                // the member OIDs field is deprecated.
                int entitiesCount = 0;
                final Map<String, Integer> stateSummary = new HashMap<>();
                for (final Entry<Integer, MemberList> entry : node.getMembersByStateMap().entrySet()) {
                    entitiesCount += entry.getValue().getMemberOidsCount();
                    final UIEntityState uiState =
                            UIEntityState.fromEntityState(EntityState.forNumber(entry.getKey()));
                    stateSummary.compute(uiState.apiStr(),
                        (k, existingValue) -> {
                            if (existingValue != null) {
                                logger.warn("Multiple states in supply chain node for entity type " +
                                        "{} map to API state {}", node.getEntityType(), k);
                                return existingValue + entry.getValue().getMemberOidsCount();
                            } else {
                                return entry.getValue().getMemberOidsCount();
                            }
                    });
                }
                supplyChainEntry.setStateSummary(stateSummary);
                supplyChainEntry.setEntitiesCount(entitiesCount);
                return supplyChainEntry;
            });
        }

        @Override
        public String toString() {
            return super.toString() + "\n" + MoreObjects.toStringHelper(this)
                    .add("entityDetailType", entityDetailType)
                    .add("includeHealthSummary", includeHealthSummary)
                    .add("actionOrchestratorAvailable", actionOrchestratorAvailable)
                    .toString();
        }
    }
}
