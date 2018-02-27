package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.Group;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
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
     * Get the group associated with a particular UUID, if any.
     * @param uuid The string UUID. This may be the OID of a group, an entity, or a magic string
     *             (e.g. Market).
     * @return The {@link Group} associated with the UUID, if any.
     */
    public Optional<Group> getGroup(@Nonnull final String uuid) {
        if (StringUtils.isNumeric(uuid)) {
            final GetGroupResponse response = groupServiceGrpc.getGroup(GroupID.newBuilder()
                    .setId(Long.parseLong(uuid))
                    .build());
            return response.hasGroup() ? Optional.of(response.getGroup()) : Optional.empty();
        } else {
            return Optional.empty();
        }
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
     * @throws StatusRuntimeException if there is an error (other than NOT_FOUND) from the
     * groupServiceGrpc call tp getMembers().
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
                .setExpectPresent(false)
                .build();
            GetMembersResponse groupMembersResp = groupServiceGrpc.getMembers(getGroupMembersReq);

            if (groupMembersResp.hasMembers()){
                answer.addAll(groupMembersResp.getMembers().getIdsList());
            } else {
                answer.add(oid);
            }
        }
        return answer;
    }
}
