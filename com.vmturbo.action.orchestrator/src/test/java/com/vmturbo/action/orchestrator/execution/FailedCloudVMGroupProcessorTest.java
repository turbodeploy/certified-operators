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
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.translation.ActionTranslator.TranslationExecutor;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class FailedCloudVMGroupProcessorTest {
    private final long actionId = 123456;
    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);
    private final ActionDTO.Action recommendationOnCloud = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setImportance(0)
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
            .setImportance(0)
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

    private final ActionDTO.Action recommendationOnPrem = ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setImportance(0)
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
            .setImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setInfo(ActionInfo.newBuilder()
                    .setReconfigure(Reconfigure.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(3)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.CLOUD))
                            .build()))
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(new TranslationExecutor() {
        @Override
        public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream) {
            return actionStream.peek(action -> action.getActionTranslation().setPassthroughTranslationSuccess());
        }
    }));
    private final ArgumentCaptor<Runnable> scheduledRunnableCaptor =
            ArgumentCaptor.forClass(Runnable.class);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(testGroupService);

    private FailedCloudVMGroupProcessor failedCloudVMGroupProcessor;
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
    private ScheduledExecutorService scheduledExecutorService = Mockito.spy(ScheduledExecutorService.class);


    @Before
    public void setup() {
        GroupServiceBlockingStub groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        failedCloudVMGroupProcessor = new FailedCloudVMGroupProcessor(groupServiceRpc, scheduledExecutorService, 10);
    }


    /**
     * test onprem action
     */
    @Test
    public void onPremTypeAction() {
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnPrem);
        failedCloudVMGroupProcessor.handleActionFailure(testAction);
        verify(testGroupService, never()).getGroups(any());
    }

    /**
     * test invalid action
     */
    @Test
    public void notAMoveOrResizeAction() {
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendation_reconfigure);
        failedCloudVMGroupProcessor.handleActionFailure(testAction);
        verify(testGroupService, never()).getGroups(any());
    }

    @Test
    public void isValidActionTest() {
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        failedCloudVMGroupProcessor.handleActionFailure(testAction);
        assertThat("Incorrect set size ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L));
    }

    /**
     * Test the ability to add multiple unique entities in set for processing
     */
    @Test
    public void addMultipleVMInSet() {
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        Action testAction_2 = makeAction(recommendationOnCloud_2);
        failedCloudVMGroupProcessor.handleActionFailure(testAction);
        assertThat("Set size incorrect after adding 1 action ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L));
        failedCloudVMGroupProcessor.handleActionFailure(testAction_2);
        assertThat("Set size incorrect after adding 2 action ", failedCloudVMGroupProcessor.getFailedOidsSet(), containsInAnyOrder(1L, 2L));
    }


    /*
     * test to check there are not duplicate entities in the sets. Entities are exclusive to a set.
     */
    @Test
    public void addDuplicateVMsInSetTest() {
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
        Action testAction = makeAction(recommendationOnCloud);
        failedCloudVMGroupProcessor.handleActionFailure(testAction);
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
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenThrow(new RuntimeException("Intentional exception"));
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
        Group group = Group.newBuilder().setGroup(GroupInfo.newBuilder().setName("TestGroup").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()).build();
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.singletonList(group));
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(2L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(3L, true);
        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, false); // success action
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();

        HashSet<Long> set = new HashSet<>(Arrays.asList(2L, 3L));
        GroupInfo newInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(set))
                .setName("TestGroup")
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();
        UpdateGroupRequest updateGroupRequest = UpdateGroupRequest.newBuilder()
                .setId(group.getId())
                .setNewInfo(newInfo).build();
        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, times(1)).updateGroup(eq(updateGroupRequest));
        verify(testGroupService, never()).createGroup(any());
    }


    /**
     * Test to verify createGroup is called only if getGroup returns empty.
     */
    @Test
    public void failedGroupCreatedIffNotExist() {
        Group group = Group.newBuilder().setGroup(GroupInfo.newBuilder().setName("TestGroup").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()).build();
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.singletonList(group));
        when(testGroupService.createGroup(any(GroupInfo.class)))
                .thenReturn(CreateGroupResponse.newBuilder().setGroup(group).build());

        failedCloudVMGroupProcessor.recordVmActionWithLock(1L, true);
        verify(scheduledExecutorService, atLeast(1)).scheduleWithFixedDelay(scheduledRunnableCaptor.capture(), eq(0L), eq(10L), eq(TimeUnit.SECONDS));
        scheduledRunnableCaptor.getValue().run();

        verify(testGroupService, atLeast(1)).getGroups(any());
        verify(testGroupService, never()).createGroup(any());

        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.emptyList());
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
        Group group = Group.newBuilder().setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(Arrays.asList(1L, 2L)))
                .setName("TestGroup").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()).build();
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.singletonList(group));
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
        Group group = Group.newBuilder().setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(Collections.emptyList()))
                .setName("TestGroup").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()).build();
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.singletonList(group));
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
        Group group = Group.newBuilder().setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addAllStaticMemberOids(Arrays.asList(3L)))
                .setName("TestGroup").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build()).build();
        when(testGroupService.getGroups(any(GetGroupsRequest.class))).thenReturn(Collections.singletonList(group));
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
        return new Action(action, 4, actionModeCalculator);
    }
}
