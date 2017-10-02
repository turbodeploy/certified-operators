package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.common.protobuf.group.GroupDTO.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.tools.StringUtils;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.GroupApiDTO;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
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
     * Process a list of UUIDs, expanding each Group or Cluster uuid into the list of
     * elements. If a UUID in the input is neither, it is simply included in the output list.
     *
     * If the list contains the special UUID "Market", then return an empty list.
     *
     * @param uuidList a list of UUIDs for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs from each Group or Cluster in the input list; other UUIDs are passed through
     */
    public List<Long> expandUuidList(List<String> uuidList) {

        List<Long> answer = Lists.newArrayList();
        for (String uuidString : uuidList) {
            // sanity-check the uuidString
            if (StringUtils.isEmpty(uuidString)) {
                throw new IllegalArgumentException("Empty uuid string given: " + uuidList);
            }
            // is this the special "Market" uuid string?
            if (uuidString.equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
                return Collections.emptyList();
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
