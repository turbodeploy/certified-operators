package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class SettingsManagerTest {

    private final TopologyFilterFactory topologyFilterFactory =
        Mockito.mock(TopologyFilterFactory.class);

    private final GroupResolver groupResolver = Mockito.mock(GroupResolver.class);

    private TopologyGraph topologyGraph = Mockito.mock(TopologyGraph.class);

    private ClusterServiceBlockingStub clusterServiceClient;

    private GroupServiceBlockingStub groupServiceClient;

    private SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private final TestGroupService testGroupService = spy(new TestGroupService());

    private final TestClusterService testClusterService = spy(new TestClusterService());

    private final TestSettingService testSettingService = spy(new TestSettingService());

    private SettingsManager settingsManager;

    private final Long entity1 = 111L;
    private final Long entity2 = 222L;
    private final Long entity3 = 333L;
    private final Set<Long> entities = new HashSet<Long>(Arrays.asList(
                new Long[]{entity1, entity2, entity3}));
    private final Long groupId1 = 111L;
    private final Group group = createGroup(groupId1, "group1");
    private final Long clusterId1 = 222L;
    private final Cluster cluster = createCluster(clusterId1, "cluster1");

    private final Setting setting1 = createSettings("setting1", 10);
    private final Setting setting2 = createSettings("setting2", 20);
    private final List<Setting> inputSettings1  =
        new LinkedList<Setting>(Arrays.asList(new Setting[]{setting1, setting2}));
    private final SettingPolicy settingPolicy1 =
        createUserSettingPolicy("sp1", inputSettings1,
            Collections.singletonList(groupId1),
            Collections.singletonList(clusterId1));

    private final Setting setting3 = createSettings("setting1", 20);
    private final Setting setting4 = createSettings("setting4", 50);
    private final List<Setting> inputSettings2  =
        new LinkedList<Setting>(Arrays.asList(new Setting[]{setting1, setting2}));
    private final SettingPolicy settingPolicy2 = createUserSettingPolicy("sp2", inputSettings2);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
            testClusterService, testSettingService);

    @Before
    public void setup() throws Exception {
        settingPolicyServiceClient = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        clusterServiceClient = ClusterServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingsManager = new SettingsManager(settingPolicyServiceClient, groupServiceClient,
            clusterServiceClient, topologyFilterFactory);
    }

    @Test
    public void testApplySettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);

        Map<Long, List<Setting>> entitySettingsMap =
            settingsManager.applySettings(groupResolver, topologyGraph);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(entitySettingsMap.get(entity1), containsInAnyOrder(setting1, setting2));
        assertEquals(entitySettingsMap.size(), 3);
        // settingPolicy2 doesn't have groups or ids. So it should't be in the
        // final result
        assertFalse(entitySettingsMap.get(entity2).contains(setting3));
        assertFalse(entitySettingsMap.get(entity3).contains(setting4));
    }

    @Test
    public void testApply() {

        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap =
            new HashMap<>();

        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entity1)
                .values()
                .stream()
                .collect(Collectors.toList());
        assertThat(appliedSettings, containsInAnyOrder(setting1, setting2));
    }

    @Test
    public void testApplyConflictResolution() {

        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap =
            new HashMap<>();

        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entity1)
                .values()
                .stream()
                .collect(Collectors.toList());

        //now check if the conflict resolution is done correctly
        // current conflict resoultion is to pick the 1st one added to the map
        settingsManager.apply(entities,
            Collections.singletonList(settingPolicy2),
            entitySettingsBySettingNameMap);

        assertNotEquals(getSettingNumericValue(
            entitySettingsBySettingNameMap.get(entity2)
                .get(setting3.getSettingSpecName())), getSettingNumericValue(setting3));
        assertNotEquals(getSettingNumericValue(
            entitySettingsBySettingNameMap.get(entity1)
                .get(setting3.getSettingSpecName())), getSettingNumericValue(setting3));
    }

    @Test
    public void testGetClusterEntities() {

        List<Long> members = Arrays.asList(new Long[] {1L, 2L, 3L});

        Cluster cluster =
            Cluster.newBuilder()
                .setId(1)
                .setTargetId(2)
                .setInfo(
                    ClusterInfo.newBuilder()
                        .setClusterType(ClusterInfo.Type.COMPUTE)
                        .setMembers(
                            StaticGroupMembers.newBuilder()
                            .addAllStaticMemberOids(members)))
            .build();

        Set<Long> returnedMembers = settingsManager.getClusterEntities(cluster);
        assertEquals(members.toArray(), returnedMembers.toArray());
    }

    @Test
    public void testSendEntitySettings() {

        long topologyId = 111;
        long topologyContexId = 777;
         ArgumentCaptor<UploadEntitySettingsRequest> requestCaptor =
            ArgumentCaptor.forClass(UploadEntitySettingsRequest.class);

        UploadEntitySettingsRequest request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId)
                .build();

        settingsManager.sendEntitySettings(topologyId, topologyContexId,
            new HashMap<Long, List<Setting>>());

        verify(testSettingService).uploadEntitySettings(requestCaptor.capture(),
            Matchers.<StreamObserver<UploadEntitySettingsResponse>>any());
    }

    private SettingPolicy createUserSettingPolicy(String name,
                                              List<Setting> settings,
                                              List<Long> groupIds,
                                              List<Long> clusterIds) {
        return  SettingPolicy.newBuilder()
                    .setInfo(SettingPolicyInfo.newBuilder()
                        .setName(name)
                        .addAllSettings(settings)
                        .setEnabled(true)
                        .setScope(Scope.newBuilder()
                            .addAllGroups(groupIds)
                            .addAllClusters(clusterIds)
                            .build())
                        .build())
                    .setSettingPolicyType(SettingPolicy.Type.USER)
                    .build();
    }

    private SettingPolicy createUserSettingPolicy(String name, List<Setting> settings) {
        return  SettingPolicy.newBuilder()
                    .setInfo(SettingPolicyInfo.newBuilder()
                        .setName(name)
                        .addAllSettings(settings)
                        .build())
                    .build();
    }

    private Setting createSettings(String name, int val) {
        return Setting.newBuilder()
                    .setSettingSpecName(name)
                    .setNumericSettingValue(
                        NumericSettingValue.newBuilder()
                        .setValue(val)
                        .build())
                    .build();
    }

    private float getSettingNumericValue(Setting setting) {
        return setting.getNumericSettingValue().getValue();
    }

    private Group createGroup(long groupId, String groupName) {
        return Group.newBuilder()
            .setId(groupId)
            .setInfo(GroupInfo.newBuilder()
                .setName(groupName))
            .build();
    }

    private Cluster createCluster(long clusterId, String clusterName) {
        return Cluster.newBuilder()
            .setId(clusterId)
            .setInfo(ClusterInfo.newBuilder()
                .setName(clusterName))
            .build();
    }

    private class TestSettingService extends SettingPolicyServiceImplBase {

        public List<SettingPolicy> listSettingPolicies(ListSettingPoliciesRequest request) {
            return new LinkedList(Arrays.asList(new SettingPolicy[]{settingPolicy1, settingPolicy2}));
        }

        public void listSettingPolicies(ListSettingPoliciesRequest request,
                                    StreamObserver<SettingPolicy> responseObserver) {
            listSettingPolicies(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    private class TestGroupService extends GroupServiceImplBase {

        public List<Group> getGroups(GetGroupsRequest request) {
            return Collections.singletonList(group);
        }

        public void getGroups(GetGroupsRequest request,
                                StreamObserver<Group> responseObserver) {
            getGroups(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    private class TestClusterService extends ClusterServiceImplBase {

        public List<Cluster> getClusters(GetClustersRequest request) {
            return Collections.singletonList(cluster);
        }

        public void getClusters(GetClustersRequest request,
                                StreamObserver<Cluster> responseObserver) {
            getClusters(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }
}
