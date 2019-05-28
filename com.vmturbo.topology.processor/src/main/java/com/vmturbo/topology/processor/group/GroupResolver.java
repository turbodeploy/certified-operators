package com.vmturbo.topology.processor.group;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.FilterTypeCase;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 *  Class to resolve members of groups by searching in the topologyGraph.
 *  The GroupResolver caches already resolved groups so that subsequent requests
 *  for the same groupId will return the cached value.
 *
 *  Typical usage of GroupResolver is to create an instance of it and pass it
 *  along through the different pipelines involved in the topology
 *  transformation before the topology broadcast.
 *
 */
@NotThreadSafe
public class GroupResolver {
    private static final Logger logger = LogManager.getLogger();

    private final SearchResolver<TopologyEntity> searchResolver;

    /**
     * Cache for storing resolved groups.
     * Mapping from GroupID->Set(EntityIDs)
     */
    private final Map<Long, Set<Long>> groupResolverCache;

    /**
     * Create a GroupResolver.
     */
    public GroupResolver(@Nonnull final SearchResolver<TopologyEntity> searchResolver) {
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.groupResolverCache = new HashMap<>();
    }

    /**
     * Resolve the members of a group defined by certain criteria.
     *
     * @param group The definition of the group whose members should be resolved.
     * @param graph The topology graph on which to perform the search.
     * @return OIDs of the members of the input group.
     * @throws GroupResolutionException when a dynamic group cannot be resolved.
     */
    public Set<Long> resolve(@Nonnull final Group group,
                             @Nonnull final TopologyGraph<TopologyEntity> graph)
            throws GroupResolutionException {

        Preconditions.checkArgument(group.hasId(), "Missing groupId");

        if (groupResolverCache.containsKey(group.getId())) {
            return groupResolverCache.get(group.getId());
        }

        Set<Long> members = resolveMembers(group, graph);
        groupResolverCache.put(group.getId(), members);
        return members;
    }

    /**
     * Helper method which does the actual group resolution.
     *
     * @param group The definition of the group whose members should be resolved.
     * @param graph The topology graph on which to perform the search.
     * @return OIDs of the members of the input group.
     * @throws GroupResolutionException when a dynamic group cannot be resolved.
     */
    private Set<Long> resolveMembers(@Nonnull final Group group,
                                     @Nonnull final TopologyGraph<TopologyEntity> graph)
            throws GroupResolutionException {

        Optional<Set<Long>> groupMembers = Optional.empty();
        switch (group.getType()) {
            case CLUSTER:
                groupMembers = Optional.of(GroupProtoUtil.getClusterMembers(group));
                break;
            case GROUP:
                switch (group.getGroup().getSelectionCriteriaCase()) {
                    case STATIC_GROUP_MEMBERS:
                        groupMembers = Optional.of(resolveStaticGroup(group));
                        break;
                    case SEARCH_PARAMETERS_COLLECTION:
                        List<SearchParameters> searchParametersList =
                                group.getGroup().getSearchParametersCollection().getSearchParametersList();
                        for (SearchParameters searchParameters : searchParametersList) {
                            final Set<Long> resolvedMembers = resolveDynamicGroup(group.getId(),
                                    GroupProtoUtil.getEntityType(group), searchParameters, graph);
                            // need to save the first resolve result in order to perform intersection
                            groupMembers = groupMembers.isPresent() ? groupMembers.map(groupSet -> {
                                groupSet.retainAll(resolvedMembers);
                                return groupSet;
                            }) : Optional.of(resolvedMembers);
                        }
                        break;
                    default:
                        throw new GroupResolutionException("Unknown selection criteria type: " +
                                group.getGroup().getSelectionCriteriaCase());
                }
                break;
        }

        return groupMembers.orElse(Collections.emptySet());
    }

    /**
     * Resolve the members of the static group.
     *
     * @param group Group whose static memebers have to be resolved
     * @return A collection of OIDs of the members of the group
     */
    @Nonnull
    @VisibleForTesting
    Set<Long> resolveStaticGroup(final Group group) {
        return new HashSet<>(group.getGroup().getStaticGroupMembers().getStaticMemberOidsList());
    }

    /**
     * Resolve the members of a group defined by certain criteria.
     *
     * @param groupId The ID of the group to resolveDynamicGroup.
     * @param groupEntityType The entity type for this group.
     * @param search The search criteria to use in resolving the group's members.
     * @param graph The topology graph on which to perform the search.
     * @return A collection of OIDs for the group members that match the {@link SearchParameters}.
     * @throws GroupResolutionException when trying to
     *               resolve the group throws a {@link RuntimeException}.
     */
    @Nonnull
    @VisibleForTesting
    Set<Long> resolveDynamicGroup(final long groupId,
                                  final int groupEntityType,
                                  @Nonnull final SearchParameters search,
                                  @Nonnull final TopologyGraph<TopologyEntity> graph) throws GroupResolutionException {
        try {
            long resolutionStartTime = System.currentTimeMillis();

            final Set<Long> members = searchResolver.search(search, graph)
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());

            final long duration = System.currentTimeMillis() - resolutionStartTime;
            final long numTraversalFilters = search.getSearchFilterList().stream()
                .filter(f -> f.getFilterTypeCase() == FilterTypeCase.TRAVERSAL_FILTER)
                .count();
            final long numPropertyFilters = search.getSearchFilterList().size() - numTraversalFilters + 1;

            final String entityTypeName = Optional.ofNullable(EntityType.forNumber(groupEntityType))
                    .map(EntityType::name)
                    .orElse("Unknown");
            logger.debug("Dynamic group {} ({}P, {}T) resolved to {} {} in {} ms .",
                groupId, numPropertyFilters, numTraversalFilters, members.size(), entityTypeName, duration);

            return members;
        } catch (RuntimeException e) {
            throw new GroupResolutionException(e);
        }
    }
}
