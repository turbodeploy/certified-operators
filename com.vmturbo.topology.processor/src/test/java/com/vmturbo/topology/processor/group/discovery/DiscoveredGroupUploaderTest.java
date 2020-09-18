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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.protobuf.util.JsonFormat;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceStub;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.GroupTestUtils;

/**
 * Unit test for {@link DiscoveredGroupUploader}.
 */
@ThreadSafe
public class DiscoveredGroupUploaderTest {
    private static final String VM_ASG_POLICY_NAME = "CSP:VMs_Accelerated Networking Enabled_EA - Development:1";
    private static final String CONTAINER_ASG_POLICY_NAME = "CSP:Deployment::btc/kubeturbo-btc-Kubernetes-btc AWS[Container]:1";
    private static final String VM_TEMPLATE_EXCLUSION_POLICY_NAME = "EXP:VMs_Accelerated Networking Enabled_EA - Development:1";

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
                discoveredClusterConstraintCache, targetStore, new StitchingGroupFixer()));
        when(interpretedGroup.getGroupDefinition()).thenReturn(Optional.empty());
        when(targetStore.getProbeTypeForTarget(TARGET_ID)).thenReturn(Optional.of(PROBE_TYPE));
        when(targetStore.getTargetDisplayName(TARGET_ID))
                .thenReturn(Optional.of("Test target " + TARGET_ID));
    }

    @Test
    public void testUploadDiscoveredGroups() {
        assertTrue(recorderSpy.getDataByTarget().isEmpty());
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
        assertFalse(recorderSpy.getDataByTarget().isEmpty());

        // Uploading discovered groups should not empty the discovered groups known by the uploader.
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);
        assertFalse(recorderSpy.getDataByTarget().isEmpty());
    }

    /**
     * Tests the behaviour when a target has been removed. It is expected, that it is removed
     * from the internal map and no longer sent do anybody.
     */
    @Test
    public void testTargetRemoved() {
        when(converter.interpretSdkGroupList(eq(Collections.singletonList(STATIC_MEMBER_DTO)),
                eq(TARGET_ID))).thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        assertFalse(recorderSpy.getDataByTarget().get(TARGET_ID).getGroups()
            .collect(Collectors.toList()).isEmpty());

        recorderSpy.targetRemoved(TARGET_ID);
        Assert.assertNull(recorderSpy.getDataByTarget().get(TARGET_ID));
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

    /**
     * When there is no probe type, then OTHER should be uploaded.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testUploadProbeTypeUnknown() throws Exception {
        when(converter.interpretSdkGroupList(any(), eq(TARGET_ID)))
            .thenReturn(Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP));

        // mock so that the probe type is unknown
        when(targetStore.getProbeTypeForTarget(TARGET_ID)).thenReturn(Optional.empty());

        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        recorderSpy.uploadDiscoveredGroups(TOPOLOGY);

        verify(groupServiceMole).storeDiscoveredGroupsPoliciesSettings(
            eq(Collections.singletonList(
                DiscoveredGroupsPoliciesSettings.newBuilder()
                    .setTargetId(TARGET_ID)
                    .setProbeType("OTHER")
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
                actualRequestCaptor.getValue();

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
        final TopologyStitchingGraph graph = mock(TopologyStitchingGraph.class);
        final TopologyStitchingEntity mergedEntity = mock(TopologyStitchingEntity.class);
        when(mergedEntity.getOid()).thenReturn(12345L);
        when(mergedEntity.hasMergeInformation()).thenReturn(true);
        when(mergedEntity.getMergeInformation()).thenReturn(Collections.singletonList(
            new StitchingMergeInformation(DiscoveredGroupConstants.PLACEHOLDER_GROUP_MEMBER, TARGET_ID, StitchingErrors.none())
        ));
        when(graph.entities()).thenReturn(Stream.of(mergedEntity));
        recorderSpy.analyzeStitchingGroups(graph);
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
        assertEquals(0, getSettingPoliciesOfTarget(TARGET_ID).size());
        assertEquals(1, getGroupsOfTarget(TARGET_ID).size());

        recorderSpy.setScannedGroupsAndPolicies(TARGET_ID, Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP),
            Collections.singletonList(DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO));
        assertEquals(1, getSettingPoliciesOfTarget(TARGET_ID).size());
        assertEquals(2, getGroupsOfTarget(TARGET_ID).size());
    }

    @Test
    public void testAddDiscoveredGroupsAndSettingsWhenEmpty() {
        recorderSpy.setScannedGroupsAndPolicies(TARGET_ID, Collections.singletonList(PLACEHOLDER_INTERPRETED_GROUP),
            Collections.singletonList(DiscoveredGroupConstants.DISCOVERED_SETTING_POLICY_INFO));
        assertEquals(1, getSettingPoliciesOfTarget(TARGET_ID).size());
        assertEquals(1, getGroupsOfTarget(TARGET_ID).size());
    }

    private List<InterpretedGroup> getGroupsOfTarget(final long targetId) {
        return recorderSpy.getDataByTarget().get(targetId).getGroups()
            .collect(Collectors.toList());
    }

    private List<DiscoveredSettingPolicyInfo> getSettingPoliciesOfTarget(final long targetId) {
        return recorderSpy.getDataByTarget().get(targetId).getSettingPolicies()
            .collect(Collectors.toList());
    }

    private List<DiscoveredPolicyInfo> getPoliciesOfTarget(final long targetId) {
        return recorderSpy.getDataByTarget().get(targetId).getPolicies()
            .collect(Collectors.toList());
    }

    /**
     * Test method convertTemplateExclusionGroupsToPolicies.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testconvertTemplateExclusionGroupsToPolicies() throws Exception {
        // Test the group with no excluded template policies
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID,
                Collections.singletonList(DiscoveredGroupConstants.RESOURCE_GROUP_DTO));
        List<DiscoveredSettingPolicyInfo> noPolicies = getSettingPoliciesOfTarget(TARGET_ID);
        Assert.assertTrue(noPolicies.isEmpty());

        // Test against the Accelerated Networking group
        GroupDTO group = loadGroupDto("AcceleratedNetworkingGroupDTO.json");
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, Collections.singletonList(group));

        List<DiscoveredSettingPolicyInfo> policies = getSettingPoliciesOfTarget(TARGET_ID);
        Assert.assertTrue(!policies.isEmpty());
        DiscoveredSettingPolicyInfo policy = policies.iterator().next();
        String name = "VMs_Accelerated Networking Enabled_EA - Development";
        Assert.assertThat(policy.getName(), CoreMatchers.containsString(name));
        Assert.assertThat(policy.getDisplayName(), CoreMatchers.containsString(name));
        Assert.assertThat(policy.getDiscoveredGroupNames(0), CoreMatchers.containsString(name));
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, policy.getEntityType());
        Assert.assertTrue(doesPolicyHaveSetting(policy, EntitySettingSpecs.ExcludedTemplates));
    }

    /**
     * Verify that groups with consistent scaling enabled generate the correct associated policies.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testConvertConsistentScalingSettingsToPolicies() throws Exception {
        /*
         * Load some groups:
         * - Accelerated networking with consistent scaling set.  This will generate two policies.
         * - Consistent scaling only, enabled.  This will generate one policy.
         * - Consistent scaling only, disabled.  This will not generate a policy.
         *
         * Total number of policies generated will be two consistent scaling and one template
         * exclusion.
         */
        String[] groupFiles = {
                        "AcceleratedNetworkingGroupDTO-consistent-scaling.json",  // VM
                        "ConsistentScalingEnabled.json",  // CONTAINER
                        "ConsistentScalingDisabled.json"  // CONTAINER
        };
        List<GroupDTO> groupDTOS = new ArrayList<>();
        for (String filename : groupFiles) {
            groupDTOS.add(loadGroupDto(filename));
        }
        recorderSpy.setTargetDiscoveredGroups(TARGET_ID, groupDTOS);

        List<DiscoveredSettingPolicyInfo> policies = getSettingPoliciesOfTarget(TARGET_ID);
        Assert.assertEquals(3, policies.size());
        // Template exclusion policies are generated first, followed by consistent scaling.
        Iterator it = policies.iterator();
        DiscoveredSettingPolicyInfo policy = (DiscoveredSettingPolicyInfo)it.next();
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, policy.getEntityType());
        Assert.assertTrue(doesPolicyHaveSetting(policy, EntitySettingSpecs.ExcludedTemplates));
        Assert.assertEquals(1, countPoliciesWithSetting(policies,
                EntitySettingSpecs.ExcludedTemplates));
        // Check for consistent scaling policies
        Assert.assertEquals(2, countPoliciesWithSetting(policies,
                EntitySettingSpecs.EnableConsistentResizing));

        Set<String> policyNames = policies.stream().map(DiscoveredSettingPolicyInfo::getName)
            .collect(Collectors.toSet());
        Assert.assertThat(policyNames, containsInAnyOrder(VM_ASG_POLICY_NAME,
            CONTAINER_ASG_POLICY_NAME, VM_TEMPLATE_EXCLUSION_POLICY_NAME));
    }

    /**
     * Check whether a policy contains the specified setting.
     * @param policy policy to check
     * @param setting setting to check for
     * @return true if the policy contains the setting
     */
    private boolean doesPolicyHaveSetting(DiscoveredSettingPolicyInfo policy,
                                          EntitySettingSpecs setting) {
        return policy.getSettingsList().stream()
                        .anyMatch(s -> s.getSettingSpecName() == setting.getSettingName());
    }

    /**
     * Return the number of policies that contain the specified setting.
     * @param policies list of policies to check
     * @param setting setting to check for
     * @return the number of policies with the specified setting
     */
    private long countPoliciesWithSetting(final List<DiscoveredSettingPolicyInfo> policies,
                                         final EntitySettingSpecs setting) {
        return policies.stream().filter(p -> doesPolicyHaveSetting(p, setting)).count();
    }

    /**
     * Load DTO from a JSON file.
     *
     * @param jsonFileName file name
     * @return action DTO
     * @throws IOException error reading the file
     */
    private GroupDTO loadGroupDto(@Nonnull String jsonFileName) throws IOException {
        String str = readResourceFileAsString(jsonFileName);
        GroupDTO.Builder builder = GroupDTO.newBuilder();
        JsonFormat.parser().merge(str, builder);

        return builder.build();
    }

    private String readResourceFileAsString(String fileName) throws IOException {
        File file = ResourcePath.getTestResource(getClass(), fileName).toFile();
        return Files.asCharSource(file, Charset.defaultCharset()).read();
    }
}
