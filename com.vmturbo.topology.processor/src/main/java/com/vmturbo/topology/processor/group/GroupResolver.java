package com.vmturbo.topology.processor.group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 *  Class to resolve members of groups by searching in the topologyGraph.
 *  The GroupResolver caches already resolved groups so that subsequent requests
 *  for the same groupId will return the cached value.
 *
 *  <p/>Typical usage of GroupResolver is to create an instance of it and pass it
 *  along through the different pipelines involved in the topology
 *  transformation before the topology broadcast.
 *
 */
@NotThreadSafe
public class GroupResolver {
    private static final Logger logger = LogManager.getLogger();

    private final SearchResolver<TopologyEntity> searchResolver;

    private final GroupServiceGrpc.GroupServiceBlockingStub groupServiceClient;

    /**
     * Cache for storing resolved groups.
     * Mapping from GroupID->Set(EntityIDs)
     */
    private final Map<Long, ResolvedGroup> groupResolverCache;

    private final GroupResolverSearchFilterResolver searchFilterResolver;

    /**
     * Create a new group resolver instance. Each instance will cache resolved groups.
     * The intention is for one resolver to be created per broadcast, and shared across broadcast.
     *
     * @param searchResolver The {@link SearchResolver} used to resolve dynamic groups.
     * @param groupServiceClient To access group service to resolve nested groups.
     * @param searchFilterResolver To resolve the group property filters.
     */
    public GroupResolver(@Nonnull final SearchResolver<TopologyEntity> searchResolver,
                         @Nullable final GroupServiceBlockingStub groupServiceClient,
                         @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver) {
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.groupServiceClient = groupServiceClient;
        this.groupResolverCache = new HashMap<>();
        this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
    }

    /**
     * Resolve the members of a group defined by certain criteria.
     *
     * @param group The definition of the group whose members should be resolved.
     * @param graph The topology graph on which to perform the search.
     * @return OIDs of the members of the input group.
     * @throws GroupResolutionException when a dynamic group cannot be resolved.
     */
    public ResolvedGroup resolve(@Nonnull final Grouping group,
                                 @Nonnull final TopologyGraph<TopologyEntity> graph)
            throws GroupResolutionException {
        Preconditions.checkArgument(group.hasId(), "Missing groupId");

        final ResolvedGroup cachedGroup = groupResolverCache.get(group.getId());
        if (cachedGroup == null) {
            ResolvedGroup members = resolveMembers(group, graph);
            groupResolverCache.put(group.getId(), members);
            return members;
        } else {
            return cachedGroup;
        }
    }

    @Nonnull
    private Map<ApiEntityType, Set<Long>> resolve(
            @Nonnull final Set<Long> groupIds,
            @Nonnull final TopologyGraph<TopologyEntity> graph) throws GroupResolutionException {
        final Set<Long> nonCachedGroupIds = groupIds.stream()
            .filter(id -> !groupResolverCache.containsKey(id))
            .collect(Collectors.toSet());
        final Map<ApiEntityType, Set<Long>> ret = new HashMap<>();
        if (!nonCachedGroupIds.isEmpty()) {
            logger.debug("Fetching non-cached groups {}", nonCachedGroupIds);
            final List<Grouping> nonCachedGroups = new ArrayList<>(nonCachedGroupIds.size());
            groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                    .addAllId(nonCachedGroupIds))
                .build()).forEachRemaining(nonCachedGroups::add);
            for (Grouping nonCachedGroup : nonCachedGroups) {
                // A recursive call. We don't expect deep levels of recursion here.
                ResolvedGroup group = resolve(nonCachedGroup, graph);
                logger.debug("Successfully resolved and cached group {} with {} members.",
                    nonCachedGroup::getId, () -> group.getAllEntities().size());
            }
        }

        groupIds.forEach(groupId -> {
            ResolvedGroup cachedGroup = groupResolverCache.get(groupId);
            if (cachedGroup != null) {
                cachedGroup.getEntitiesByType().forEach((type, entities) -> {
                    ret.computeIfAbsent(type, k -> new HashSet<>(entities.size())).addAll(entities);
                });
            } else {
                // This shouldn't happen because we take the groups that are missing from the cache
                // and resolve them earlier in this method, and we never remove entries from the
                // cache.
                logger.error("Group {} unexpectedly missing from cache.", groupId);
            }
        });
        return ret;
    }

    /**
     * Helper method which does the actual group resolution.
     *
     * @param group The definition of the group whose members should be resolved.
     * @param graph The topology graph on which to perform the search.
     * @return OIDs of the members of the input group.
     * @throws GroupResolutionException when a dynamic group cannot be resolved.
     */
    private ResolvedGroup resolveMembers(@Nonnull final Grouping group,
                                         @Nonnull final TopologyGraph<TopologyEntity> graph)
            throws GroupResolutionException {
        final Map<ApiEntityType, Set<Long>> result = new HashMap<>();
        final Set<Long> groups = new HashSet<>();
        switch (group.getDefinition().getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                for (StaticMembersByType members : group.getDefinition().getStaticGroupMembers().getMembersByTypeList()) {
                    if (members.getType().hasGroup()) {
                        groups.addAll(members.getMembersList());
                    } else {
                        result.computeIfAbsent(ApiEntityType.fromType(members.getType().getEntity()),
                            k -> new HashSet<>(members.getMembersCount())).addAll(members.getMembersList());
                    }
                }
                break;
            case ENTITY_FILTERS:
                for (EntityFilter entityFilter: group.getDefinition().getEntityFilters().getEntityFilterList()) {
                    try {
                        List<SearchParameters> finalSearchParameters = entityFilter
                                .getSearchParametersCollection().getSearchParametersList().stream()
                                .map(searchFilterResolver::resolveExternalFilters)
                                .collect(Collectors.toList());
                        final Set<Long> members = searchResolver.search(SearchQuery.newBuilder()
                            .addAllSearchParameters(finalSearchParameters)
                            .setLogicalOperator(entityFilter.getLogicalOperator())
                            .build(), graph)
                            .map(TopologyEntity::getOid)
                            .collect(Collectors.toSet());
                        result.computeIfAbsent(ApiEntityType.fromType(entityFilter.getEntityType()), k -> new HashSet<>())
                            .addAll(members);
                    } catch (RuntimeException e) {
                        throw new GroupResolutionException(group, e);
                    }
                }
                break;
            case GROUP_FILTERS:
                try {
                    GroupDTO.GetMembersResponse response = groupServiceClient
                        .getMembers(GroupDTO.GetMembersRequest.newBuilder()
                            .setExpectPresent(true)
                            .addId(group.getId()).build()).next();
                    groups.addAll(response.getMemberIdList());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus() == Status.NOT_FOUND) {
                        // If the group no longer exists - e.g. it got deleted between the start
                        // of the pipeline and now - it's safe to treat it as an empty group.
                        logger.warn("Group {} no longer exists: {}", group.getId(), e.getMessage());
                    } else {
                        // For any other error we will throw an exception.
                        throw new GroupResolutionException(group, e);
                    }
                }
                break;
            default:
                logger.error("Unsupported or unset group member selection criteria for `{}`", group);
                break;
        }

        if (!groups.isEmpty()) {
            logger.debug("Resolution for group {} requires resolving nested groups: {}", group.getId(), groups);
            resolve(groups, graph).forEach((type, oids) -> {
                result.computeIfAbsent(type, k -> new HashSet<>(oids.size())).addAll(oids);
            });
        }
        return new ResolvedGroup(group, result);
    }

    /**
     * Get the size of the group resolver cache.
     *
     * @return the size of the group resolver cache.
     */
    public int getCacheSize() {
        return groupResolverCache.size();
    }

    public GroupServiceGrpc.GroupServiceBlockingStub getGroupServiceClient() {
        return groupServiceClient;
    }
}
