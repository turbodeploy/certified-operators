package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.Group;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.tools.StringUtils;

import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;

/**
 * Process a list of UUIDs to replace {@link Group}s with the
 * {@link ServiceEntityApiDTO}s that belong to the Group (or Cluster, respectively).
 * UUIDs for ServiceEntities are included in the output.
 *
 * There's a special case for the 'uuid' of the live "Market"...this must be the only UUID in
 * the input 'uuidList'; the output result for the live "Market" is the empty list, indicating
 * "All SE's".
 **/
public class GroupExpander {

    private final GroupServiceBlockingStub groupServiceGrpc;

    public GroupExpander(@Nonnull GroupServiceBlockingStub groupServiceGrpc) {
        this.groupServiceGrpc = groupServiceGrpc;
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
        return expandUuids(Collections.singleton(uuid));
    }

    /**
     * Process a list of UUIDs, expanding each Group or Cluster uuid into the list of
     * elements. If a UUID in the input is neither, it is simply included in the output list.
     *
     * If the list contains the special UUID "Market", then return an empty list.
     *
     * @param uuidSet a list of UUIDs for which Cluster or Group UUIDs will be expanded.
     * @return UUIDs from each Group or Cluster in the input list; other UUIDs are passed through
     */
    public @Nonnull Set<Long> expandUuids(@Nonnull Set<String> uuidSet) {
        Set<Long> answer = Sets.newHashSet();
        for (String uuidString : uuidSet) {
            // sanity-check the uuidString
            if (StringUtils.isEmpty(uuidString)) {
                throw new IllegalArgumentException("Empty uuid string given: " + uuidSet);
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
                answer.add(oid);
            }
        }
        return answer;
    }


}
