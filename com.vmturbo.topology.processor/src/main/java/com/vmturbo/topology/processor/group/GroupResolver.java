package com.vmturbo.topology.processor.group;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.FilterTypeCase;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

public class GroupResolver {

    private final TopologyFilterFactory filterFactory;
    private static final Logger logger = LogManager.getLogger();

    public GroupResolver(@Nonnull final TopologyFilterFactory filterFactory) {
        this.filterFactory = Objects.requireNonNull(filterFactory);
    }

    /**
     * Resolve the members of a policy grouping. If the policy grouping uses a {@link Group}
     * then delegate to {@link #resolve(Group, TopologyGraph)}, otherwise (the grouping
     * uses a {@link Cluster}) just return the cluster members' OIDs.
     *
     * @param grouping uses either Group or Cluster to define the membership.
     * @param graph The topology graph on which to perform the search.
     * @return OIDs of the members of the input grouping.
     * @throws GroupResolutionException when a group cannot be resolved.
     */
    public Set<Long> resolve(@Nonnull final PolicyGrouping grouping,
                    @Nonnull final TopologyGraph graph) throws GroupResolutionException {
        if (grouping.hasGroup()) { // Group
            return resolve(grouping.getGroup(), graph);
        } else { // Cluster
            return new HashSet<>(grouping
                            .getCluster()
                            .getInfo()
                            .getMembers()
                            .getStaticMemberOidsList());
        }
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
                             @Nonnull final TopologyGraph graph) throws GroupResolutionException {
        switch (group.getInfo().getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                return new HashSet<>(group.getInfo().getStaticGroupMembers().getStaticMemberOidsList());
            case SEARCH_PARAMETERS_COLLECTION:
                List<SearchParameters> searchParametersList = group.getInfo().getSearchParametersCollection().getSearchParametersList();
                Optional<Set<Long>> groupMembers = Optional.empty();
                for (SearchParameters searchParameters : searchParametersList) {
                    final Set<Long> resolvedMembers = resolveDynamicGroup(group.getId(), getGroupEntityType(group),
                            searchParameters, graph);
                    // need to save the first resolve result in order to perform intersection
                    groupMembers = groupMembers.isPresent() ? groupMembers.map(groupSet -> {
                        groupSet.retainAll(resolvedMembers);
                        return groupSet;
                    }) : Optional.of(resolvedMembers);
                }
                return groupMembers.orElse(Collections.emptySet());
            default:
                throw new GroupResolutionException("Unknown group members type: " +
                        group.getInfo().getSelectionCriteriaCase());
        }
    }

    /**
     * Resolve the members of a group defined by certain criteria.
     *
     * @param groupId The ID of the group to resolveDynamicGroup.
     * @param groupEntityType The name of the entity type for this group.
     * @param search The search criteria to use in resolving the group's members.
     * @param graph The topology graph on which to perform the search.
     * @return A collection of OIDs for the group members that match the {@link SearchParameters}.
     * @throws GroupResolutionException when trying to
     *               resolve the group throws a {@link RuntimeException}.
     */
    @Nonnull
    private Set<Long> resolveDynamicGroup(final long groupId,
                                          final String groupEntityType,
                                          @Nonnull final SearchParameters search,
                                          @Nonnull final TopologyGraph graph) throws GroupResolutionException {
        try {
            long resolutionStartTime = System.currentTimeMillis();

            final Set<Long> members = executeResolution(search, graph);

            final long duration = System.currentTimeMillis() - resolutionStartTime;
            final long numTraversalFilters = search.getSearchFilterList().stream()
                .filter(f -> f.getFilterTypeCase() == FilterTypeCase.TRAVERSAL_FILTER)
                .count();
            final long numPropertyFilters = search.getSearchFilterList().size() - numTraversalFilters + 1;

            logger.debug("Dynamic group {} ({}P, {}T) resolved to {} {} in {} ms .",
                groupId, numPropertyFilters, numTraversalFilters, members.size(), groupEntityType, duration);

            return members;
        } catch (RuntimeException e) {
            throw new GroupResolutionException(e);
        }
    }

    private Set<Long> executeResolution(@Nonnull final SearchParameters search, @Nonnull final TopologyGraph graph) {
        Stream<Vertex> matchingVertices = filterFactory.filterFor(search.getStartingFilter())
            .apply(graph.vertices(), graph);

        for (SearchFilter filter : search.getSearchFilterList()) {
            matchingVertices = filterFactory.filterFor(filter).apply(matchingVertices, graph);
        }

        return matchingVertices
            .map(Vertex::getOid)
            .collect(Collectors.toSet());
    }

    private String getGroupEntityType(@Nonnull final Group group) {
        return Stream.of(EntityType.values())
            .filter(entityType -> entityType.getNumber() == group.getInfo().getEntityType())
            .findFirst()
            .map(EntityType::name)
            .orElse("Unknown");
    }
}
