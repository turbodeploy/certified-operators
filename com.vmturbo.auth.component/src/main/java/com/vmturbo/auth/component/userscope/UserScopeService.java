package com.vmturbo.auth.component.userscope;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.scoping.AccessScopeCacheKey;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.userscope.UserScope.CurrentUserEntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeContents;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeResponse;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.AllOids;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.NoOids;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.OidArray;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceImplBase;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * This services manages calculation of the "user scope" entity set. (see "Scoped Users" in the wiki
 * for how this is supposed to work in both classic and XL). A user is "scoped" if they are assigned
 * a group or set of groups as their "scope". The entity members of these "scope groups" are used to
 * seed a supply chain calculation, where all entities in the supply chain for the group members is
 * the resulting "user scope" -- meaning, the user can see and interact with all of the entities in
 * the supply chain supporting their scope groups.
 *
 * The UserScopeService manages the supply chain calculation for this process, and basically just
 * makes calls to the group service (to get the members of the scope groups) and the repository
 * service (for the supply chain calculation), and adds a caching layer on top of it.
 *
 * This could also have been built as a generic supply chain service caching layer, though at this
 * point that may be adding extra complexity where any performance boost may not be needed. We will
 * build this as a purpose-specific cache, since the "scoped user" cases are expected to be a
 * minority of use cases compared to regular, non-scoped users. In the future, if we do build
 * caching into group / supply chain services, we may be able to remove this service, or at least
 * remove the caching layer in it.
 */
public class UserScopeService extends UserScopeServiceImplBase implements RepositoryListener {
    private static final Logger logger = LogManager.getLogger();

    // Some metrics tracking the behavior of the user scope service
    // GRPC invocations are already tracking by the monitoring interceptor, so we'll only add
    // metrics specific to this service here.
    private static final DataMetricGauge USER_SCOPE_CACHE_ENTRIES = DataMetricGauge.builder()
            .withName("user_scope_cache_total_entries")
            .withHelp("# of user scope entries currently in the cache.")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_TOTAL_OIDS = DataMetricGauge.builder()
            .withName("user_scope_cache_total_oids")
            .withHelp("total # of oids across all cache entries (scope members only, does not include group ids and group member ids).")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_TOTAL_GROUP_IDS = DataMetricGauge.builder()
            .withName("user_scope_cachee_total_group_ids")
            .withHelp("total # of group ids across all cache entries - duplicates allowed.")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_TOTAL_ESTIMATED_BYTES = DataMetricGauge.builder()
            .withName("user_scope_cache_total_estimated_bytes")
            .withHelp("total estimated cache size (in bytes).")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_LARGEST_OID_SET = DataMetricGauge.builder()
            .withName("user_scope_cache_largest_oid_set")
            .withHelp("number of oids in the largest oid set in memory.")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_LARGEST_GROUP_SET = DataMetricGauge.builder()
            .withName("user_scope_cache_largest_group_set")
            .withHelp("number of ids in the largest user scope group set in memory.")
            .build()
            .register();

    private static final DataMetricGauge USER_SCOPE_CACHE_LARGEST_ENTRY_ESTIMATED_BYTES = DataMetricGauge.builder()
            .withName("user_scope_cache_largest_entry_estimated_bytes")
            .withHelp("estimated byte size of the largest entry in the user scope cache.")
            .build()
            .register();

    private final GroupServiceBlockingStub groupServiceStub;

    private final SupplyChainServiceBlockingStub supplyChainServiceStub;

    private final SearchServiceBlockingStub searchServiceStub;

    private final Clock clock;

    private boolean cacheEnabled = true;

    // cache of EntityAccessScopeContents objects. Synchronized map for now, but can be made
    // concurrent if performance is a problem.
    private final Map<AccessScopeCacheKey, AccessScopeDataCacheEntry> accessScopeContentsForGroups
            = Collections.synchronizedMap(new HashMap<>());

    public UserScopeService(GroupServiceBlockingStub groupServiceStub,
                            SupplyChainServiceBlockingStub supplyChainServiceStub,
                            SearchServiceBlockingStub searchServiceStub,
                            Clock clock) {
        this.groupServiceStub = groupServiceStub;
        this.supplyChainServiceStub = supplyChainServiceStub;
        this.searchServiceStub = searchServiceStub;
        this.clock = clock;
    }

    /**
     * Enables or disables the user scope cache.
     *
     * @param newValue if true, the cache will be enabled. If false, it will be disabled.
     * @return the previous setting value.
     */
    public boolean setCacheEnabled(boolean newValue) {
        logger.info("UserScopeService - setting cache enabled to {}", newValue);
        boolean wasEnabled = cacheEnabled;
        cacheEnabled = newValue;
        if (!cacheEnabled) {
            clearCache("Cache is being disabled.");
        }
        return wasEnabled;
    }

    @Override
    public void getCurrentUserEntityAccessScopeMembers(final CurrentUserEntityAccessScopeRequest request,
                                                       final StreamObserver<EntityAccessScopeResponse> responseObserver) {
        List<Long> scopeGroups = UserScopeUtils.getUserScopeGroups();

        internalGetEntityAccessScopeMembers(scopeGroups,
                UserScopeUtils.isUserShared(),
                request.hasCurrentScopeHash() ? Optional.of(request.getCurrentScopeHash()) : Optional.empty(),
                responseObserver);
    }

    @Override
    public void getEntityAccessScopeMembers(final EntityAccessScopeRequest request,
                                            final StreamObserver<EntityAccessScopeResponse> responseObserver) {

        internalGetEntityAccessScopeMembers(request.getGroupIdList(),
                request.hasIncludeInfrastructureEntities() ? (!request.getIncludeInfrastructureEntities()) : false,
                request.hasCurrentScopeHash() ? Optional.of(request.getCurrentScopeHash()) : Optional.empty(),
                responseObserver);
    }

    private void internalGetEntityAccessScopeMembers(List<Long> scopeGroupOids, boolean excludeInfrastructureEntities, Optional<Integer> currentHash,
                                                     final StreamObserver<EntityAccessScopeResponse> responseObserver) {
        // set up the response builder
        EntityAccessScopeResponse.Builder responseBuilder = EntityAccessScopeResponse.newBuilder();

        // if the scope group list is empty, return a "default config" response, e.g. no seed oids,
        // and "all oids" in the accessible oids set.
        if (scopeGroupOids.size() == 0) {
            logger.debug("No groups in scope -- returning full access scope.");
            EntityAccessScopeContents.Builder contentsBuilder = EntityAccessScopeContents.newBuilder();
            contentsBuilder.setSeedOids(OidSetDTO.newBuilder()
                    .setNoOids(NoOids.getDefaultInstance()))
                    .setAccessibleOids(OidSetDTO.newBuilder()
                            .setAllOids(AllOids.getDefaultInstance()));
            responseBuilder.setEntityAccessScopeContents(contentsBuilder);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        // at this point, we have a user with scope needing resolution.
        // if we don't have service stubs to use -- throw an error
        if (groupServiceStub == null || supplyChainServiceStub == null) {
            logger.error("Group service and supply chain service clients unavailable. Cannot compute scope.");
            responseObserver.onError(new IllegalStateException("Cannot compute user scope without references to "
                    +"Group service and Supply Chain services."));
            return;
        }

        // first, check if the data is already cached.
        EntityAccessScopeContents contents;
        if (cacheEnabled) {
            // get the response from cache
            int prevSize = accessScopeContentsForGroups.size();
            contents = accessScopeContentsForGroups.computeIfAbsent(
                    new AccessScopeCacheKey(scopeGroupOids, excludeInfrastructureEntities),
                    key -> calculateScope(key.getScopeGroupOids(), excludeInfrastructureEntities)).contents;
            // if the cache size has changed, update the metrics
            if (accessScopeContentsForGroups.size() != prevSize) {
                updateCacheMetrics();
            }
        } else {
            // always calculate the scope if the cache is disabled.
            logger.debug("UserScopeService cache is disabled.");
            contents = calculateScope(scopeGroupOids, excludeInfrastructureEntities).contents;
        }

        // if the request included a cached hash, compare them -- if they are the same, send
        // back a "data unchanged" response w/updated expiration time. This will avoid retransmission
        // of the whole data set in a scenario where the receiver already has it in memory.
        if (currentHash.isPresent()) {
            if (currentHash.get() == contents.getHash()) {
                logger.debug("Requester scope contents hash is still good -- sending confirmation.");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
        }
        // otherwise, send the full data set.
        responseBuilder.setEntityAccessScopeContents(contents);
        logger.debug("Sending entity scope contents for {} groups", scopeGroupOids.size());
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * Given a list of scope group id's, calculate the scope via the group and supply chain services.
     *
     * @param scopeGroupOids the groups to calculate the supply chain scope for.
     * @return a {@link AccessScopeDataCacheEntry} object containing the scope data.
     */
    private synchronized AccessScopeDataCacheEntry calculateScope(Collection<Long> scopeGroupOids,
                                                                  boolean excludeInfrastructureEntities) {
        // double check if the cache entry exists in case the current thread was blocking on entry
        // into this function and the previous caller populated the cache in the meantime.
        AccessScopeDataCacheEntry doubleCheckEntry = accessScopeContentsForGroups.get(
                new AccessScopeCacheKey(scopeGroupOids, excludeInfrastructureEntities));
        if (doubleCheckEntry != null) {
            logger.debug("UserScopeService found a cached access scope on the second try, using it.");
            return doubleCheckEntry;
        }

        // at this point, we'll need to expand the scope groups and get the supply chain for the
        // set of group members.
        Instant startTime = clock.instant();
        EntityAccessScopeContents.Builder contentsBuilder = EntityAccessScopeContents.newBuilder();

        // get the scope groups
        logger.debug("User has scope with {} groups. Will create an access scope.", scopeGroupOids.size());
        // turn the user's scopes into the entity set they have access to.
        // We will do this by:
        // 1) Getting the members of all the groups in the user's access scope if it's a group, or
        //    the entity itself if it's an entity
        // 2) Getting the supply chain for all of the entities in the set created in step (1)
        // 3) The resulting set of entities will become the user's entity access scope.
        Set<Long> scopeGroupEntityOids = new HashSet<>();
        for (Long scopeOid : scopeGroupOids) {
            // check if this scope is an entity or group
            SearchEntityOidsResponse searchEntityOidsResponse = searchServiceStub.searchEntityOids(
                    SearchEntityOidsRequest.newBuilder().addEntityOid(scopeOid).build());
            if (searchEntityOidsResponse.getEntitiesCount() == 1 &&
                    searchEntityOidsResponse.getEntities(0) == scopeOid) {
                // this is an entity, for example: DataCenter
                logger.debug("Adding entity {} to user scope", scopeOid);
                scopeGroupEntityOids.add(scopeOid);
            } else {
                // this is a group, for example: host cluster. try to get its members
                GetMembersRequest getGroupMembersReq = GetMembersRequest.newBuilder()
                        .setId(scopeOid)
                        .setEnforceUserScope(false) // disable this for the purposes of calculating scope
                        .setExpandNestedGroups(true)
                        .setExpectPresent(false)
                        .build();
                GetMembersResponse groupMembersResponse = groupServiceStub.getMembers(getGroupMembersReq);
                List<Long> members = groupMembersResponse.getMembers().getIdsList();
                logger.debug("Adding {} members from group {} to user scope", members.size(), scopeOid);
                scopeGroupEntityOids.addAll(members);
            }
        }
        Instant groupFetchTime = clock.instant();
        logger.debug("fetching {} groups for user scope took {} ms", scopeGroupOids.size(),
                Duration.between(startTime, groupFetchTime).toMillis());

        // now, use the set of entities to make a supply chain request.
        final GetSupplyChainRequest.Builder supplyChainRequestBuilder = GetSupplyChainRequest.newBuilder()
            .setScope(SupplyChainScope.newBuilder()
                .addAllStartingEntityOid(scopeGroupEntityOids))
                .setFilterForDisplay(false);

        // for "shared" users, we should only include specific entity types.
        if (excludeInfrastructureEntities) {
            logger.debug("Adding filter for shared entity types: {}", UserScopeUtils.SHARED_USER_ENTITY_TYPES);
            supplyChainRequestBuilder.getScopeBuilder().addAllEntityTypesToInclude(UserScopeUtils.SHARED_USER_ENTITY_TYPES);
        }
        final GetSupplyChainRequest supplyChainRequest = supplyChainRequestBuilder.build();

        Set<Long> accessibleEntities = new HashSet<>();
        // this is a streaming response
        final SupplyChain response = supplyChainServiceStub.getSupplyChain(supplyChainRequest)
            .getSupplyChain();
        final int missingEntitiesCnt = response.getMissingStartingEntitiesCount();
        if (missingEntitiesCnt > 0) {
            logger.warn("Related entities for {} (of {}) scope members were not found. Missing members: {}",
                missingEntitiesCnt, scopeGroupEntityOids.size(),
                response.getMissingStartingEntitiesList());
        }

        response.getSupplyChainNodesList().stream()
                .forEach(node -> {
                    // add all of these members into both the type-specific oid set as well as the
                    // global set of accessible oids
                    OidSetDTO.Builder typeSetBuilder = OidSetDTO.newBuilder();
                    node.getMembersByStateMap().values().stream()
                            .forEach(memberList -> {
                                typeSetBuilder.getArrayBuilder().addAllOids(memberList.getMemberOidsList());
                                accessibleEntities.addAll(memberList.getMemberOidsList());
                            });
                    contentsBuilder.putAccessibleOidsByEntityType(node.getEntityType(), typeSetBuilder.build());
                });

        // convert the set to a primitive array, which we will both cache and send back to the caller
        OidArray.Builder accessibleOidArrayBuilder = OidArray.newBuilder();
        int x = 0;
        for (Long oid : accessibleEntities) {
            accessibleOidArrayBuilder.addOids(oid);
        }
        contentsBuilder.setAccessibleOids(OidSetDTO.newBuilder().setArray(accessibleOidArrayBuilder));
        Instant scopeMembersFetchTime = clock.instant();
        logger.debug("fetching supply chain of {} oids for user scope took {} ms", accessibleEntities.size(),
                Duration.between(groupFetchTime, scopeMembersFetchTime).toMillis());


        // put the set of scope group member oids into the response as well -- these may be useful.
        OidArray.Builder scopeGroupMemberOidsBuilder = OidArray.newBuilder();
        for (Long oid : scopeGroupEntityOids) {
            scopeGroupMemberOidsBuilder.addOids(oid);
        }
        contentsBuilder.setSeedOids(OidSetDTO.newBuilder().setArray(scopeGroupMemberOidsBuilder));
        // the hash is only based on the accessible oids
        contentsBuilder.setHash(accessibleEntities.hashCode());

        return new AccessScopeDataCacheEntry(contentsBuilder.build());
    }

    // update the cache metrics
    private void updateCacheMetrics() {
        USER_SCOPE_CACHE_ENTRIES.setData(Double.valueOf(accessScopeContentsForGroups.size()));

        // calculate some stats about the stuff in cache
        int totalOids = 0;
        int totalGroups = 0;
        int totalEstimatedSize = 0;
        int largestNumGroups = 0;
        int largestSetSize = 0;
        int largestEstimatedSetSize = 0;
        for (Map.Entry<AccessScopeCacheKey, AccessScopeDataCacheEntry> entry : accessScopeContentsForGroups.entrySet()) {
            int numGroups = entry.getKey().getScopeGroupOids().size();
            totalGroups += numGroups;
            if (numGroups > largestNumGroups) {
                largestNumGroups = numGroups;
            }

            EntityAccessScopeContents contents = entry.getValue().contents;
            if (contents.getAccessibleOids().hasArray()) {
                int numOidsInScope = contents.getAccessibleOids().getArray().getOidsCount();
                totalOids += numOidsInScope;
                if (totalOids > largestSetSize) {
                    largestSetSize = totalOids;
                }
            }

            int estimatedSize = entry.getValue().estimatedSizeBytes();
            totalEstimatedSize += estimatedSize;
            if (estimatedSize > largestEstimatedSetSize) {
                largestEstimatedSetSize = estimatedSize;
            }
        }

        USER_SCOPE_CACHE_TOTAL_OIDS.setData(Double.valueOf(totalOids));
        USER_SCOPE_CACHE_TOTAL_GROUP_IDS.setData(Double.valueOf(totalGroups));
        USER_SCOPE_CACHE_TOTAL_ESTIMATED_BYTES.setData(Double.valueOf(totalEstimatedSize));
        USER_SCOPE_CACHE_LARGEST_OID_SET.setData(Double.valueOf(largestSetSize));
        USER_SCOPE_CACHE_LARGEST_GROUP_SET.setData(Double.valueOf(largestNumGroups));
        USER_SCOPE_CACHE_LARGEST_ENTRY_ESTIMATED_BYTES.setData(Double.valueOf(largestEstimatedSetSize));
    }

    private void clearCache(String reason) {
        if (accessScopeContentsForGroups.size() > 0) {
            logger.info("{} -- clearing {} cached access scope contents.", reason,
                    accessScopeContentsForGroups.size());
            accessScopeContentsForGroups.clear();

            updateCacheMetrics();
        }
    }

    @Override
    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId) {
        clearCache("New source topology available");
    }

    @Override
    public void onSourceTopologyFailure(final long topologyId, final long topologyContextId, @Nonnull final String failureDescription) {
        // not invalidating the cache here, since the repository will probably return the same
        // answers anyways.
    }

    /**
     * Holder for cache values.
     */
    @Immutable
    private static class AccessScopeDataCacheEntry {
        // these numbers were derived from some simple tests. They are not exact.
        static private final int ESTIMATED_BASELINE_SCOPE_CONTENT_SIZE_BYTES = 392;
        static private final int ESTIMATED_BYTES_PER_OID = 30;

        private final EntityAccessScopeContents contents;
        private final int estimatedSizeInBytes;

        public AccessScopeDataCacheEntry(EntityAccessScopeContents scopeContents) {
            contents = scopeContents;

            // the estimated byte size is a rough approximation based on some measurements of the
            // protobuf object sizes in memory.
            // baseline protobuf user scope object w/no oids
            int estimatedBytes = ESTIMATED_BASELINE_SCOPE_CONTENT_SIZE_BYTES;
            // add in an estimate of bytes per oid
            if (scopeContents.getSeedOids().hasArray()) {
                estimatedBytes += scopeContents.getSeedOids().getArray().getOidsCount() * ESTIMATED_BYTES_PER_OID;
            }
            if (scopeContents.getAccessibleOids().hasArray()) {
                // we are going to roughly double the estimate based on total accessible oids since
                // we've added the per-entity-type map. This is an under-approximation.
                estimatedBytes += (2 * scopeContents.getAccessibleOids().getArray().getOidsCount() * ESTIMATED_BYTES_PER_OID);
            }
            estimatedSizeInBytes = estimatedBytes;
        }

        /**
         * Return the estimated size of this entry, in bytes.
         *
         * @return
         */
        public int estimatedSizeBytes() {
            return estimatedSizeInBytes;
        }
    }
}
