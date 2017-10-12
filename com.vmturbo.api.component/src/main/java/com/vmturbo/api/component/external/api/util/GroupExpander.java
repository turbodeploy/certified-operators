package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import static com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.Group;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.tools.StringUtils;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;

/**
 * Process a list of UUIDs to replace {@link Group}s or {@link Cluster}s with the
 * {@link ServiceEntityApiDTO}s that belong to the Group (or Cluster, respectively).
 * UUIDs for ServiceEntities are included in the output.
 *
 * There's a special case for the 'uuid' of the live "Market"...this must be the only UUID in
 * the input 'uuidList'; the output result for the live "Market" is the empty list, indicating
 * "All SE's".
 **/
public class GroupExpander {

    private final GroupServiceBlockingStub groupServiceGrpc;
    private final ClusterServiceBlockingStub clusterServiceRpc;

    public GroupExpander(@Nonnull GroupServiceBlockingStub groupServiceGrpc,
                         @Nonnull ClusterServiceBlockingStub clusterServiceRpc) {
        this.groupServiceGrpc = groupServiceGrpc;
        this.clusterServiceRpc = clusterServiceRpc;
    }

    /**
     * Process a UUID, expanding if a Group or Cluster uuid into the list of
     * elements. If the UUID is neither, it is simply included in the output list.
     *
     * If uuid is the special UUID "Market", then return an empty list.
     *
     * @param uuid a UUID for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs if the given uuid is for a Group or Cluster; otherwise just the given uuid
     */
    public @Nonnull Set<Long> expandUuid(@Nonnull String uuid) {
        return expandUuidList(Collections.singletonList(uuid));
    }

    /**
     * Process a list of UUIDs, expanding each Group or Cluster uuid into the list of
     * elements. If a UUID in the input is neither, it is simply included in the output list.
     *
     * If the list contains the special UUID "Market", then return an empty list.
     *
     * @param uuidList a list of UUIDs for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs from each Group or Cluster in the input list; other UUIDs are passed through
     */
    public @Nonnull Set<Long> expandUuidList(@Nonnull List<String> uuidList) {

        Set<Long> answer = Sets.newHashSet();
        for (String uuidString : uuidList) {
            // sanity-check the uuidString
            if (StringUtils.isEmpty(uuidString)) {
                throw new IllegalArgumentException("Empty uuid string given: " + uuidList);
            }
            // is this the special "Market" uuid string?
            if (uuidString.equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
                return Collections.emptySet();
            }

            // try to fetch the members for a Group with the given OID
            long oid = Long.valueOf(uuidString);
            GetMembersRequest getGroupMembersReq = GetMembersRequest.newBuilder()
                    .setId(oid)
                    .build();
            try {
                GetMembersResponse groupMembersResp = groupServiceGrpc.getMembers(getGroupMembersReq);
                answer.addAll(groupMembersResp.getMemberIdList());
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                    // not a group, try a cluster
                    GetClusterResponse response =
                            clusterServiceRpc.getCluster(GetClusterRequest.newBuilder()
                                    .setClusterId(oid)
                                    .build());
                    if (response.hasCluster()) {
                        answer.addAll(response.getCluster().getInfo().getMembers()
                                .getStaticMemberOidsList());
                    } else {
                        // neither group nor cluster
                        answer.add(Long.valueOf(uuidString));
                    }
                } else {
                    throw e;
                }
            }
        }
        return answer;
    }
}
