package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.COMPUTE_CLUSTER_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.COMPUTE_INTERPRETED_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.DC_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_GROUP;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_INTERPRETED_GROUP;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STORAGE_CLUSTER_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STORAGE_INTERPRETED_CLUSTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TOPOLOGY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceStub;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.GroupTestUtils;

@ThreadSafe
public class DiscoveredGroupUploaderTest {

    private DiscoveredGroupUploader recorderSpy;

    private final DiscoveredGroupInterpreter converter = mock(DiscoveredGroupInterpreter.class);

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache =
            mock(DiscoveredClusterConstraintCache.class);

    private InterpretedGroup interpretedGroup = mock(InterpretedGroup.class);

    private GroupServiceMole groupServiceMole = spy(new GroupServiceMole());

    private GroupServiceStub groupServiceStub;

    private TargetStore targetStore = mock(TargetStore.class);

    private static final SDKProbeType PROBE_TYPE = SDKProbeType.VCENTER;

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(groupServiceMole);


    @Before
    public void setup() throws Exception {
        groupServiceStub = GroupServiceGrpc.newStub(server.getChannel());
        recorderSpy = spy(new DiscoveredGroupUploader(groupServiceStub, converter,
                discoveredClusterConstraintCache, targetStore));
        when(interpretedGroup.getGroupDefinition()).thenReturn(Optional.empty());
        when(targetStore.getProbeTypeForTarget(TARGET_ID)).thenReturn(Optional.of(PROBE_TYPE));
    }

    @Test
    public void testUploadDiscoveredGroups() {
        assertTrue(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);
        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
                eq(Collections.singletonList(
                        DiscoveredGroupsPoliciesSettings.newBuilder()
                                .setTargetId(TARGET_ID)
                                .setProbeType(PROBE_TYPE.toString())
                                .addUploadedGroups(PLACEHOLDER_GROUP)
                                .build())));
    }

    @Test
    public void testDiscoveredGroupsNotEmptiedByUpload() {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertFalse(recorderSpy.getDiscoveredGroupInfoByTarget().isEmpty());

        // Uploading discovered groups should not empty the discovered groups known by the uploader.
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);
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
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
                eq(Collections.singletonList(
                        DiscoveredGroupsPoliciesSettings.newBuilder()
                                .setTargetId(TARGET_ID)
                                .setProbeType(PROBE_TYPE.toString())
                                .build())));
    }

    @Test
    public void testProcessClusterSuccess() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_CLUSTER));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
                eq(Collections.singletonList(
                        DiscoveredGroupsPoliciesSettings.newBuilder()
                                .setTargetId(TARGET_ID)
                                .setProbeType(PROBE_TYPE.toString())
                                .addUploadedGroups(PLACEHOLDER_CLUSTER)
                                .build())));
    }

    @Test
    public void testProcessGroupSuccess() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
                eq(Collections.singletonList(
                        DiscoveredGroupsPoliciesSettings.newBuilder()
                                .setTargetId(TARGET_ID)
                                .setProbeType(PROBE_TYPE.toString())
                                .addUploadedGroups(PLACEHOLDER_GROUP)
                                .build())));
    }

    @Test
    public void testAddDatacenterPrefixToCluster() throws Exception {
        // trigger the group discovery from the target (mocking the response from the converter)
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
                .thenReturn(ImmutableList.of(COMPUTE_INTERPRETED_CLUSTER, STORAGE_INTERPRETED_CLUSTER));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        // trigger the upload request creation multiple times
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        // generate the expected cluster info with the prefix name added
        // note: even after triggering the upload multiple time, we need to make sure that a single
        // prefix is added
        UploadedGroup computeClusterDefWithPrefix = GroupTestUtils.createUploadedCluster(
                COMPUTE_INTERPRETED_CLUSTER.getSourceId(), DC_NAME + "/" + COMPUTE_CLUSTER_NAME,
                GroupType.COMPUTE_HOST_CLUSTER,
                new ArrayList<>(COMPUTE_INTERPRETED_CLUSTER.getAllStaticMembers()))
                .build();

        UploadedGroup storageClusterDefWithoutPrefix = GroupTestUtils.createUploadedCluster(
                STORAGE_INTERPRETED_CLUSTER.getSourceId(), STORAGE_CLUSTER_NAME,
                GroupType.STORAGE_CLUSTER,
                new ArrayList<>(STORAGE_INTERPRETED_CLUSTER.getAllStaticMembers()))
                .build();

        // check that the datacenter prefix has been applied correctly
        DiscoveredGroupsPoliciesSettings expectedRequest = DiscoveredGroupsPoliciesSettings.newBuilder()
                .addUploadedGroups(computeClusterDefWithPrefix)
                .addUploadedGroups(storageClusterDefWithoutPrefix)
                .build();

        ArgumentCaptor<List> actualRequestCaptor = ArgumentCaptor.forClass(List.class);
        verify(groupServiceMole, times(2)).storeDiscoveredGroupsPoliciesSettings(
                actualRequestCaptor.capture());
        List<DiscoveredGroupsPoliciesSettings> actualRequest =
                (List<DiscoveredGroupsPoliciesSettings>)actualRequestCaptor.getValue();

        assertEquals(1, actualRequest.size());
        assertThat(actualRequest.get(0).getUploadedGroupsList(), containsInAnyOrder(
                expectedRequest.getUploadedGroupsList().toArray()));
    }

    @Test
    public void testFixupGroupsModifiesUploadedGroups() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(new InterpretedGroup(
                    STATIC_MEMBER_DTO, Optional.of(PLACEHOLDER_GROUP.getDefinition().toBuilder()))));
        when(interpretedGroup.getGroupDefinition()).thenReturn(
                Optional.of(PLACEHOLDER_GROUP.getDefinition().toBuilder()));
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));

        // Apply the group fixer so that the uploader's group should be modified to replace
        // the member PLACEHOLDER_GROUP_MEMBER with the member 12345L.
        final StitchingGroupFixer fixer = new StitchingGroupFixer();
        final TopologyStitchingGraph graph = mock(TopologyStitchingGraph.class);
        final TopologyStitchingEntity mergedEntity = mock(TopologyStitchingEntity.class);
        when(mergedEntity.getOid()).thenReturn(12345L);
        when(mergedEntity.hasMergeInformation()).thenReturn(true);
        when(mergedEntity.getMergeInformation()).thenReturn(Collections.singletonList(
            new StitchingMergeInformation(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER, TARGET_ID, StitchingErrors.none())
        ));

        when(graph.entities()).thenReturn(Stream.of(mergedEntity));
        fixer.fixupGroups(graph, recorderSpy.buildMemberCache());

        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        // Ensure that the group that got uploaded contained 12345L and not PLACEHOLDER_GROUP_MEMBER
        UploadedGroup.Builder modifiedGroup = PLACEHOLDER_GROUP.toBuilder();
        modifiedGroup.getDefinitionBuilder()
                .clearStaticGroupMembers()
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                .addMembers(12345L)));

        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
                eq(Collections.singletonList(
                        DiscoveredGroupsPoliciesSettings.newBuilder()
                                .setTargetId(TARGET_ID)
                                .setProbeType(PROBE_TYPE.toString())
                                .addUploadedGroups(modifiedGroup.build())
                                .build())));

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

}
