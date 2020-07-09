package com.vmturbo.extractor.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/** Utility class to help with tests of group-related functionality. */
public class GroupServiceTestUtil {

    private GroupServiceTestUtil() {
    }

    /**
     * Create a new group builder, to set up a group for testing.
     *
     * @param type group type
     * @param id   group id
     * @return group builder
     */
    public static GroupBuilder makeGroup(GroupType type, long id) {
        return new GroupBuilder(type, id);
    }

    /**
     * Turns some group builders into a list of built {@link Grouping} objects.
     *
     * @param builders group builders
     * @return built groups
     */
    public static List<Grouping> groupList(GroupBuilder... builders) {
        return Arrays.stream(builders).map(GroupBuilder::build).collect(Collectors.toList());
    }

    /**
     * Create a new group service based on a test grpc server.
     *
     * @param grpcServer test server
     * @return group service client
     */
    public static GroupServiceBlockingStub getGroupService(GrpcTestServer grpcServer) {
        return GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Utility to build a {@link Grouping} object.
     */
    public static class GroupBuilder {
        private final StaticMembers.Builder members = StaticMembers.newBuilder();
        private final GroupDefinition.Builder def = GroupDefinition.newBuilder();
        private final Grouping.Builder group = Grouping.newBuilder();

        /**
         * Create a new instance.
         *
         * @param type group type
         * @param id   group id
         */
        public GroupBuilder(GroupType type, long id) {
            group.setId(id);
            def.setType(type).setDisplayName(String.format("%s group %d", type, id));
        }

        /**
         * Add static group members of a given entity type.
         *
         * @param entityType entity type of these members
         * @param entityOids entity oids
         * @return this builder
         */
        public GroupBuilder withStaticEntityMembers(EntityType entityType, long... entityOids) {
            members.addMembersByType(StaticMembersByType.newBuilder()
                    .setType(MemberType.newBuilder().setEntity(entityType.getNumber()))
                    .addAllMembers(Longs.asList(entityOids)));
            return this;
        }

        /**
         * Build a {@link Grouping} instance.
         *
         * @return the group
         */
        public Grouping build() {
            return group.setDefinition(def.setStaticGroupMembers(members)).build();
        }
    }
}
