package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_CLUSTER_INFO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_GROUP_INFO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;

@ThreadSafe
public class DiscoveredGroupUploaderTest {

    private DiscoveredGroupUploader recorderSpy;

    private final TestDiscoveredService uploadServiceSpy = spy(new TestDiscoveredService());

    private final DiscoveredGroupInterpreter converter = mock(DiscoveredGroupInterpreter.class);

    private InterpretedGroup interpretedGroup = mock(InterpretedGroup.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(uploadServiceSpy);

    @Before
    public void setup() throws Exception {
        recorderSpy = spy(new DiscoveredGroupUploader(server.getChannel(), converter));
        when(interpretedGroup.getDtoAsCluster()).thenReturn(Optional.empty());
        when(interpretedGroup.getDtoAsGroup()).thenReturn(Optional.empty());
    }

    @Test
    public void testUploadDiscoveredGroups() {
        assertTrue(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        recorderSpy.uploadDiscoveredGroups();
        verify(uploadServiceSpy).storeDiscoveredGroups(
                eq(StoreDiscoveredGroupsRequest.newBuilder()
                    .setTargetId(TARGET_ID)
                    .addDiscoveredGroup(PLACEHOLDER_GROUP_INFO)
                    .build()), any());
    }

    @Test
    public void testDiscoveredGroupsNotEmptiedByUpload() {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertFalse(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());

        // Uploading discovered groups should not empty the discovered groups known by the uploader.
        recorderSpy.uploadDiscoveredGroups();
        assertFalse(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());
    }

    @Test
    public void testTargetRemoved() {
        when(converter.interpretSdkGroupList(eq(Collections.singletonList(STATIC_MEMBER_DTO)),
                eq(TARGET_ID))).thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertFalse(recorderSpy.getDiscoveredGroupInfoByTarget().get(TARGET_ID).isEmpty());

        recorderSpy.targetRemoved(TARGET_ID);
        assertTrue(recorderSpy.getDiscoveredGroupInfoByTarget().get(TARGET_ID).isEmpty());
    }

    /**
     * Test that even if no groups/clusters got successfully converted we still
     * update the group definitions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessUnsuccessfulInterpretation() throws Exception {
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        recorderSpy.uploadDiscoveredGroups();

        verify(uploadServiceSpy).storeDiscoveredGroups(
                eq(StoreDiscoveredGroupsRequest.newBuilder()
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    @Test
    public void testProcessClusterSuccess() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_CLUSTER));
        when(interpretedGroup.getDtoAsCluster()).thenReturn(Optional.of(PLACEHOLDER_CLUSTER_INFO.toBuilder()));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        recorderSpy.uploadDiscoveredGroups();

        verify(uploadServiceSpy).storeDiscoveredGroups(
            eq(StoreDiscoveredGroupsRequest.newBuilder()
                .addDiscoveredCluster(PLACEHOLDER_CLUSTER_INFO)
                .setTargetId(TARGET_ID)
                .build()), any());
    }

    @Test
    public void testProcessGroupSuccess() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));
        when(interpretedGroup.getDtoAsGroup()).thenReturn(Optional.of(PLACEHOLDER_GROUP_INFO.toBuilder()));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        recorderSpy.uploadDiscoveredGroups();

        verify(uploadServiceSpy).storeDiscoveredGroups(
                eq(StoreDiscoveredGroupsRequest.newBuilder()
                        .addDiscoveredGroup(PLACEHOLDER_GROUP_INFO)
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    @Test
    public void testFixupGroupsModifiesUploadedGroups() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(
                new InterpretedGroup(STATIC_MEMBER_DTO,
                    Optional.of(PLACEHOLDER_GROUP_INFO.toBuilder()),
                    Optional.empty())));
        when(interpretedGroup.getDtoAsGroup()).thenReturn(Optional.of(PLACEHOLDER_GROUP_INFO.toBuilder()));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        // Apply the group fixer so that the uploader's group should be modified to replace
        // the member PLACEHOLDER_GROUP_MEMBER with the member 12345L.
        final StitchingGroupFixer fixer = new StitchingGroupFixer();
        final TopologyStitchingGraph graph = mock(TopologyStitchingGraph.class);
        final TopologyStitchingEntity mergedEntity = mock(TopologyStitchingEntity.class);
        when(mergedEntity.getOid()).thenReturn(12345L);
        when(mergedEntity.hasMergeInformation()).thenReturn(true);
        when(mergedEntity.getMergeInformation()).thenReturn(Collections.singletonList(
            new StitchingMergeInformation(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER, TARGET_ID)
        ));

        when(graph.entities()).thenReturn(Stream.of(mergedEntity));
        fixer.fixupGroups(graph, recorderSpy.buildMemberCache());

        recorderSpy.uploadDiscoveredGroups();

        // Ensure that the group that got uploaded contained 12345L and not PLACEHOLDER_GROUP_MEMBER
        final GroupInfo modifiedGroup = PLACEHOLDER_GROUP_INFO.toBuilder()
            .clearStaticGroupMembers()
            .setStaticGroupMembers(StaticGroupMembers.newBuilder().addStaticMemberOids(12345L))
            .build();

        verify(uploadServiceSpy).storeDiscoveredGroups(
                eq(StoreDiscoveredGroupsRequest.newBuilder()
                        .addDiscoveredGroup(modifiedGroup)
                        .setTargetId(TARGET_ID)
                        .build()), any());
    }

    @Test
    public void testProcessGroupsException() throws Exception {
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        uploadServiceSpy.enableError();

        try {
            recorderSpy.uploadDiscoveredGroups();
            Assert.fail("Expected runtime exception.");
        } catch (RuntimeException e) {
            // Verify that discovered group info was not cleared by the exception
            assertFalse(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());
        }
    }

    @Test
    public void testAppendDiscoveredGroupsAndSettings() {
        final List<InterpretedGroup> groups = new ArrayList<>(); // Create a mutable list so it can be added to.
        groups.add(PLACEHOLDER_INTERPRETED_GROUP);
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(groups);
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertEquals(0, recorderSpy.getDiscoveredSettingPolicyInfoForTarget(TARGET_ID).get().size());
        assertEquals(1, recorderSpy.getDiscoveredGroupInfoByTarget().get(TARGET_ID).size());

        recorderSpy.addDiscoveredGroupsAndPolicies(TARGET_ID, Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP),
            Collections.singletonList(DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO));
        assertEquals(1, recorderSpy.getDiscoveredSettingPolicyInfoForTarget(TARGET_ID).get().size());
        assertEquals(2, recorderSpy.getDiscoveredGroupInfoByTarget().get(TARGET_ID).size());
    }

    @Test
    public void testAddDiscoveredGroupsAndSettingsWhenEmpty() {
        recorderSpy.addDiscoveredGroupsAndPolicies(TARGET_ID, Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP),
            Collections.singletonList(DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO));
        assertEquals(1, recorderSpy.getDiscoveredSettingPolicyInfoForTarget(TARGET_ID).get().size());
        assertEquals(1, recorderSpy.getDiscoveredGroupInfoByTarget().get(TARGET_ID).size());
    }

    @Test
    public void testSettingPoliciesClearedBySet() {
        recorderSpy.addDiscoveredGroupsAndPolicies(TARGET_ID, Collections.emptyList(),
            Collections.singletonList(DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO));
        assertFalse(recorderSpy.getDiscoveredSettingPolicyInfoForTarget(TARGET_ID).get().isEmpty());

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertTrue(recorderSpy.getDiscoveredSettingPolicyInfoForTarget(TARGET_ID).get().isEmpty());
    }

    public static class TestDiscoveredService extends DiscoveredGroupServiceImplBase {

        private boolean error = false;

        public void enableError() {
            error = true;
        }

        public void storeDiscoveredGroups(StoreDiscoveredGroupsRequest request,
                   StreamObserver<StoreDiscoveredGroupsResponse> responseObserver) {
            if (error) {
                responseObserver.onError(Status.INTERNAL.asException());
            } else {
                responseObserver.onNext(StoreDiscoveredGroupsResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }
    }
}
