package com.vmturbo.mediation.udt.explore;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * This is an object able to resolve filters. There are some filters that could not be executed
 * by repository component (search grpc service) because repository does not know anything about
 * groups.
 *
 * <p></p>This object is to convert filters that repository is not aware of to filters that it can
 * consume substituting complicated filters with an oid filter, where oids are resolved using
 * another components.
 */
public class UdtSearchFilterResolver extends SearchFilterResolver {

    private final RequestExecutor requestExecutor;
    private final DataRequests requests;
    private final TargetSearchServiceBlockingStub targetSearchService;

    /**
     * Constructor.
     *
     * @param connection      - a holder for gRPC connections.
     * @param requestExecutor - gRPC executor.
     * @param requests        - a provider of gRPC requests models.
     */
    @ParametersAreNonnullByDefault
    public UdtSearchFilterResolver(Connection connection, RequestExecutor requestExecutor, DataRequests requests) {
        this.targetSearchService = Objects.requireNonNull(TargetSearchServiceGrpc
                .newBlockingStub(connection.getTopologyProcessorChannel()));
        this.requestExecutor = requestExecutor;
        this.requests = requests;
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter) {
        final GetGroupsRequest groupsRequest = requests.getGroupRequest(groupFilter);
        final Set<Long> groups = requestExecutor.getGroupIds(groupsRequest);
        return groups.stream()
                .map(requests::getGroupMembersRequest)
                .map(requestExecutor::getGroupMembers)
                .map(resp -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(resp, 0), false)
                        .collect(Collectors.toSet()))
                .flatMap(set -> set.stream().map(GetMembersResponse::getMemberIdList))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    protected Set<Long> getGroupOwners(@Nonnull Collection<Long> groupIds, @Nullable GroupType type) {
        final GetOwnersRequest request = requests.getGroupOwnerRequest(groupIds, type);
        return requestExecutor.getOwnersOfGroups(request);
    }

    @Nonnull
    @Override
    protected Collection<Long> getTargetIdsFromFilter(@Nonnull PropertyFilter filter) {
        return targetSearchService.searchTargets(filter).getTargetsList();
    }
}
