package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class FailedCloudVMGroupProcessorTest {
    private static final String FAILED_GROUP_CLOUD_VMS = "Cloud VMs with Failed Sizing";
    private static final GetGroupsRequest GET_GROUP_REQUEST = GetGroupsRequest.newBuilder()
        .setGroupFilter(GroupDTO.GroupFilter.newBuilder()
            .setGroupType(CommonDTO.GroupDTO.GroupType.REGULAR)
            .addPropertyFilters(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(
                    Search.PropertyFilter.StringFilter.newBuilder()
                        .setStringPropertyRegex(FAILED_GROUP_CLOUD_VMS))
            )
        )
        .build();

    private final long actionId = 123456;
    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);
    private final ActionDTO.Action recommendationOnCloud = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                    .setMove(Move.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(1L)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.CLOUD))
                            .build()))
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private final ActionDTO.Action recommendationOnCloud_2 = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                    .setMove(Move.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(2L)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.CLOUD))
                            .build()))
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private final ActionDTO.Action scaleOnCloud = ActionDTO.Action.newBuilder()
        .setId(actionId)
        .setDeprecatedImportance(0)
        .setSupportingLevel(SupportLevel.SUPPORTED)
        .setInfo(ActionInfo.newBuilder()
            .setScale(ActionDTO.Scale.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(5L)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEnvironmentType(EnvironmentType.CLOUD))
                .build()))
        .setExplanation(Explanation.getDefaultInstance())
        .build();

    private final ActionDTO.Action recommendationOnPrem = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                    .setMove(Move.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(2)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))
                            .build()))
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private final ActionDTO.Action recommendation_reconfigure = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                    .setReconfigure(Reconfigure.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(3)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.CLOUD))
                            .setIsProvider(false)
                            .build()))
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());

    private final ArgumentCaptor<Runnable> scheduledRunnableCaptor =
            ArgumentCaptor.forClass(Runnable.class);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(testGroupService);

    private FailedCloudVMGroupProcessor failedCloudVMGroupProcessor;
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private ScheduledExecutorService scheduledExecutorService = Mockito.spy(ScheduledExecutorService.class);


    @Before
    public void setup() {
        GroupServiceBlockingStub groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        RepositoryServiceBlockingStub repositoryServiceBlockingStub =
                RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());
        SettingPolicyServiceBlockingStub settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(
                testServer.getChannel());
        failedCloudVMGroupProcessor = new FailedCloudVMGroupProcessor(groupServiceRpc,
                repositoryServiceBlockingStub, settingPolicyService,
                scheduledExecutorService, 10, "");
    }


    /**
     * test onprem action
     */
    @Test
    public void onPremTypeAction() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnPrem);
        verify(testGroupService, never()).getGroups(any());
    }

    /**
     * test invalid action
     */
    @Test
    public void notAMoveOrResizeAction() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendation_reconfigure);
        failedCloudVMGroupProcessor.handleActionFailure(testAction, null);
        verify(testGroupService, never()).getGroups(any());
    }

    @Test
    public void isValidActionTest() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        failedCloudVMGroupProcessor.handleActionFailure(testAction, null);
        assertThat("Incorrect set size ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L));
    }

    /**
     * Tests that failed scale action results in adding failed vm in failed vm set.
     */
    @Test
    public void isValidScaleActionTest() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(scaleOnCloud);
        failedCloudVMGroupProcessor.handleActionFailure(testAction, null);
        assertThat("Incorrect set size ", failedCloudVMGroupProcessor.getFailedOidsSet(),
            containsInAnyOrder(5L));
    }

    /**
     * Test the ability to add multiple unique entities in set for processing.
     */
    @Test
    public void addMultipleVMInSet() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        Action testAction_2 = makeAction(recommendationOnCloud_2);
        failedCloudVMGroupProcessor.handleActionFailure(testAction, null);
        assertThat("Set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L));
        failedCloudVMGroupProcessor.handleActionFailure(testAction_2, null);
        assertThat("Set size incorrect after adding 2 action ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L, 2L));
    }


    /*
     * test to check there are not duplicate entities in the sets. Entities are exclusive to a set.
     */
    @Test
    public void addDuplicateVMsInSetTest() {
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        failedCloudVMGroupProcessor.handleActionFailure(testAction, null);
        assertThat("Failed Set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L));
        assertTrue("Success set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getSuccessOidsSet().isEmpty());
        failedCloudVMGroupProcessor.handleActionSuccess(testAction);
        assertThat("Success Set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getSuccessOidsSet(), containsInAnyOrder(1L));
        assertTrue("Failed set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getFailedOidsSet().isEmpty());
    }


    /**
     * no update request is sent if there is no new vm entity in the set
     */
    @Test
    public void noNewVmsAddedForProcessingTest() {
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();
        verify(testGroupService, never()).getGroups(any());
        verify(testGroupService, never()).createGroup(any());
        verify(testGroupService, never()).updateGroup(any());
        assertTrue("Expected an empty set", failedCloudVMGroupProcessor.getFailedOidsSet().isEmpty());
        assertTrue("Expected an empty set", failedCloudVMGroupProcessor.getSuccessOidsSet().isEmpty());
    }

    /**
     * test Fault tolerance when groupService is down.
     */
    @Test
    public void whenGroupRPCServiceIsDownTest() {
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenThrow(new RuntimeException("Intentional exception"));
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();
        verify(testGroupService, atLeast(1)).getGroups(any());
        assertFalse("Unexpected thread shutdown on error.", scheduledExecutorService.isShutdown());
    }


    /**
     * Test to verify the update group request does not contain duplicate entity .
     */
    @Test
    public void updateMultipleEntitiesTest() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setDisplayName("TestGroup")
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                                        ))
                        .build();
        Grouping group = Grouping
                        .newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setDefinition(groupDefinition).build();

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.singletonList(group));
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(2L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(3L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, false); // success action
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();

        HashSet<Long> set = new HashSet<>(Arrays.asList(2L, 3L));
        GroupDefinition newInfo = GroupDefinition
            .newBuilder()
            .setDisplayName("TestGroup")
            .setStaticGroupMembers(StaticMembers.newBuilder()
                .addMembersByType(StaticMembersByType.newBuilder()
                    .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                    .addAllMembers(set)
                    ))
            .build();

        UpdateGroupRequest updateGroupRequest = UpdateGroupRequest.newBuilder()
                .setId(group.getId())
                .setNewDefinition(newInfo).build();
        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, times(1)).updateGroup(eq(updateGroupRequest));
        verify(testGroupService, never()).createGroup(any());
    }


    /**
     * Test to verify createGroup is called only if getGroup returns empty.
     */
    @Test
    public void failedGroupCreatedIffNotExist() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setDisplayName("TestGroup")
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                            .addAllMembers(Arrays.asList(1L, 2L))
                                            ))
                        .build();
        Grouping group = Grouping
                        .newBuilder().setDefinition(groupDefinition)
                        .addExpectedTypes(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .build();

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST)))
            .thenReturn(Collections.singletonList(group));
        when(testGroupService.createGroup(any(CreateGroupRequest.class)))
                .thenReturn(CreateGroupResponse.newBuilder().setGroup(group).build());

        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();

        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, never()).createGroup(any());

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.emptyList());
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        scheduledRunnableCaptor.getValue().run();
        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, times(1)).createGroup(any());
    }

    /**
     * test when there are new vms added but member oids remains the same. In this case update call is cancelled.
     */
    @Test
    public void noNewVMToProcessTest() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setDisplayName("TestGroup")
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                            .addAllMembers(Arrays.asList(1L, 2L))
                                            ))
                        .build();
        Grouping group = Grouping
                        .newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setDefinition(groupDefinition).build();

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.singletonList(group));
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(2L, true);
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();
        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, never()).updateGroup(any());
    }

    /*
     * test when get group succeeds and update group fails.
     */
    @Test
    public void failureDuringUpdateTest() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setDisplayName("TestGroup")
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                            ))
                        .build();
        Grouping group = Grouping
                        .newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setDefinition(groupDefinition).build();

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.singletonList(group));
        when(testGroupService.updateGroup(any(UpdateGroupRequest.class))).thenThrow(StatusRuntimeException.class);
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(2L, true);
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();
        assertTrue("Failed set should have vmid : 1", failedCloudVMGroupProcessor.getFailedOidsSet().contains(1L));
        assertThat("Wrong faildOid set size", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L, 2L));
        assertTrue("Success oid set not empty", failedCloudVMGroupProcessor.getSuccessOidsSet().isEmpty());
    }

    /**
     * Test for a successful group update and check if the the sets remains empty.
     */
    @Test
    public void successfulGroupUpdateTest() {
        GroupDefinition groupDefinition = GroupDefinition
                        .newBuilder()
                        .setDisplayName("TestGroup")
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                            .addAllMembers(Arrays.asList(3L))
                                            ))
                        .build();
        Grouping group = Grouping
                        .newBuilder()
                        .addExpectedTypes(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setDefinition(groupDefinition).build();

        when(testGroupService.getGroups(eq(GET_GROUP_REQUEST))).thenReturn(Collections.singletonList(group));
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(2L, true);
        assertTrue("Success oid set not empty", failedCloudVMGroupProcessor.getSuccessOidsSet().isEmpty());
        assertThat("Failed oid set not empty", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L, 2L));
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();
        assertTrue("Success oid set not empty", failedCloudVMGroupProcessor.getSuccessOidsSet().isEmpty());
        assertTrue("Failed oid set not empty", failedCloudVMGroupProcessor.getFailedOidsSet().isEmpty());
    }


    private Action makeAction(ActionDTO.Action action) {
        return new Action(action, 4, actionModeCalculator, 1L);
    }
}
