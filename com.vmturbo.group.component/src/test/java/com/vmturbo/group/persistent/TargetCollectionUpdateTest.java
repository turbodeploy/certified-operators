package com.vmturbo.group.persistent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TargetCollectionUpdate.RemoveInstance;
import com.vmturbo.group.persistent.TargetCollectionUpdate.StoreInstance;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetGroupUpdate;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetSettingPolicyUpdate;

public class TargetCollectionUpdateTest {

    private final long groupId = 7L;

    private final long targetId = 10L;

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    @Mock
    private StoreInstance<Group> storeInstance;

    @Mock
    private RemoveInstance removeInstance;

    @Captor
    private ArgumentCaptor<Group> groupCaptor;

    @Captor
    private ArgumentCaptor<Long> longCaptor;

    @Mock
    private StoreInstance<InputPolicy> policyStoreInstance;

    @Captor
    private ArgumentCaptor<InputPolicy> policyCaptor;

    @Mock
    private StoreInstance<SettingPolicy> settingPolicyStoreInstance;

    @Captor
    private ArgumentCaptor<SettingPolicy> settingPolicyCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGroupsToAdd() throws Exception {
        when(identityProvider.next()).thenReturn(groupId);

        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                Collections.singletonList(GroupInfo.newBuilder().setName("test").build()),
                Collections.emptyList());
        update.apply(storeInstance, removeInstance);
        verifyZeroInteractions(removeInstance);
        verify(storeInstance).storeInstance(groupCaptor.capture());

        final Group createdGroup = groupCaptor.getValue();
        assertEquals(groupId, createdGroup.getId());
        assertEquals(targetId, createdGroup.getTargetId());
        assertEquals(Origin.DISCOVERED, createdGroup.getOrigin());
        assertEquals("test", createdGroup.getGroup().getName());
    }

    @Test
    public void testGroupToModify() throws Exception {
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                Collections.singletonList(GroupInfo.newBuilder()
                        .setName("test")
                        .setEntityType(10)
                        // Modify the members.
                        .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                            .addStaticMemberOids(1))
                        .build()),
                Collections.singletonList(Group.newBuilder()
                    .setGroup(GroupInfo.newBuilder().setName("test").setEntityType(10))
                    .setId(groupId)
                    .build()));
        update.apply(storeInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(removeInstance);

        verify(storeInstance).storeInstance(groupCaptor.capture());

        final Group createdGroup = groupCaptor.getValue();
        assertEquals(groupId, createdGroup.getId());
        assertEquals(targetId, createdGroup.getTargetId());
        assertEquals(10, createdGroup.getGroup().getEntityType());
        assertEquals(Origin.DISCOVERED, createdGroup.getOrigin());
        assertEquals("test", createdGroup.getGroup().getName());

        // Check that the members got set.
        assertEquals(1, createdGroup.getGroup().getStaticGroupMembers().getStaticMemberOidsCount());
        assertEquals(1, createdGroup.getGroup().getStaticGroupMembers().getStaticMemberOids(0));
    }

    @Test
    public void testGroupToDelete() throws Exception {
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                Collections.emptyList(),
                Collections.singletonList(Group.newBuilder()
                        .setGroup(GroupInfo.newBuilder().setName("test"))
                        .setId(groupId)
                        .build()));
        update.apply(storeInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(storeInstance);

        verify(removeInstance).removeInstance(longCaptor.capture());

        final long deletedGroupId = longCaptor.getValue();
        assertEquals(groupId, deletedGroupId);
    }

    @Test
    public void testCollidingDiscovered() throws Exception {
        final String name = "Bozo";
        final int entityType = 9001;
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
            // Discovered two identical groups.
            ImmutableList.of(
                GroupInfo.newBuilder()
                    .setName(name)
                    .setEntityType(entityType)
                    .build(),
                GroupInfo.newBuilder()
                    .setName(name)
                    .setEntityType(entityType)
                    .build()),
            Collections.emptyList());

        update.apply(storeInstance, removeInstance);

        verifyZeroInteractions(removeInstance);
        // Verify exactly one attempt to store.
        verify(storeInstance).storeInstance(any());
    }

    @Test
    public void testCollidingExisting() throws Exception {
        final String name = "Bozo";
        final int entityType = 9001;
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                Collections.singletonList(GroupInfo.newBuilder()
                        .setName(name)
                        .setEntityType(entityType)
                        .build()),
                // There exist two identical groups (for some reason)
                ImmutableList.of(
                        Group.newBuilder()
                            .setId(1)
                            .setGroup(GroupInfo.newBuilder()
                                .setName(name)
                                .setEntityType(entityType))
                            .build(),
                        Group.newBuilder()
                            .setId(2)
                            .setGroup(GroupInfo.newBuilder()
                                .setName(name)
                                .setEntityType(entityType))
                            .build()));

        // There should be one call to modify a group.
        update.apply(storeInstance, removeInstance);

        // Make sure the lower ID got removed.
        verify(removeInstance).removeInstance(longCaptor.capture());
        assertEquals(1, longCaptor.getValue().longValue());

        // The other should have gotten updated.
        verify(storeInstance).storeInstance(groupCaptor.capture());
        final Group updatedGroup = groupCaptor.getValue();
        assertEquals(2, updatedGroup.getId());
    }

    @Test
    public void testTargetPolicyDisabled() throws Exception {
        final TargetPolicyUpdate update = new TargetPolicyUpdate(targetId, identityProvider,
            Collections.singletonList(InputPolicy.newBuilder()
                .setName("test")
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setProviderGroup(1)
                    .setConsumerGroup(2))
                .build()),
            Collections.singletonList(InputPolicy.newBuilder()
                .setName("test")
                .setEnabled(false)
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setProviderGroup(1)
                    .setConsumerGroup(3))
                .build()));
        update.apply(policyStoreInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(removeInstance);

        verify(policyStoreInstance).storeInstance(policyCaptor.capture());

        final InputPolicy inputPolicy = policyCaptor.getValue();
        assertFalse(inputPolicy.getEnabled());
        assertEquals("test", inputPolicy.getName());
        assertEquals(2, inputPolicy.getBindToGroup().getConsumerGroup());
        assertEquals(1, inputPolicy.getBindToGroup().getProviderGroup());
    }

    @Test
    public void testTargetSettingPolicyUpdate() throws Exception {
        when(identityProvider.next()).thenReturn(groupId);

        final TargetSettingPolicyUpdate update = new TargetSettingPolicyUpdate(targetId, identityProvider,
            Collections.singletonList(SettingPolicyInfo.newBuilder()
                .setTargetId(12345L)
                .setName("test")
                .build()),
            Collections.emptyList());
        update.apply(settingPolicyStoreInstance, removeInstance);
        verifyZeroInteractions(removeInstance);
        verify(settingPolicyStoreInstance).storeInstance(settingPolicyCaptor.capture());

        final SettingPolicy createdSettingPolicy = settingPolicyCaptor.getValue();
        assertEquals(groupId, createdSettingPolicy.getId());
        assertEquals(12345L, createdSettingPolicy.getInfo().getTargetId());
        assertEquals(Type.DISCOVERED, createdSettingPolicy.getSettingPolicyType());
        assertEquals("test", createdSettingPolicy.getInfo().getName());
    }
}
