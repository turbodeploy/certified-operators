package com.vmturbo.group.common;

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

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetClusterUpdate;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.common.TargetCollectionUpdate.RemoveInstance;
import com.vmturbo.group.common.TargetCollectionUpdate.StoreInstance;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetGroupUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetSettingPolicyUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.UpdateInstance;

public class TargetCollectionUpdateTest {

    private final long groupId = 7L;

    private final long targetId = 10L;

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    @Mock
    private StoreInstance<Group> storeInstance;

    @Mock
    private RemoveInstance removeInstance;

    @Mock
    private UpdateInstance<Group> updateInstance;

    @Captor
    private ArgumentCaptor<Group> groupCaptor;

    @Captor
    private ArgumentCaptor<Long> longCaptor;

    @Mock
    private StoreInstance<Policy> policyStoreInstance;

    @Mock
    private UpdateInstance<Policy> policyUpdateInstance;

    @Captor
    private ArgumentCaptor<Policy> policyCaptor;

    @Mock
    private StoreInstance<SettingPolicy> settingPolicyStoreInstance;

    @Mock
    private UpdateInstance<SettingPolicy> settingPolicyUpdateInstance;

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
        update.apply(storeInstance, updateInstance, removeInstance);
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
        update.apply(storeInstance, updateInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(removeInstance);

        verify(updateInstance).updateInstance(groupCaptor.capture());

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
    public void testClusterUpdatePreservesHeadroomTemplateId() throws Exception {
        final TargetClusterUpdate update = new TargetClusterUpdate(targetId, identityProvider,
            Collections.singletonList(ClusterInfo.newBuilder()
                .setName("test")
                // No cluster headroom template ID specified in the new ClusteInfo.
                .build()),
            Collections.singletonList(Group.newBuilder()
                .setCluster(ClusterInfo.newBuilder()
                    .setName("test")
                    .setClusterHeadroomTemplateId(7))
                .setId(groupId)
                .build()));
        update.apply(storeInstance, updateInstance, removeInstance);

        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(removeInstance);

        verify(updateInstance).updateInstance(groupCaptor.capture());

        final Group createdGroup = groupCaptor.getValue();
        assertEquals(groupId, createdGroup.getId());
        assertEquals(targetId, createdGroup.getTargetId());
        assertEquals(Origin.DISCOVERED, createdGroup.getOrigin());

        // Make sure the cluster ID got preserved.
        assertEquals(7, createdGroup.getCluster().getClusterHeadroomTemplateId());
    }

    @Test
    public void testGroupToDelete() throws Exception {
        Group groupToDelete = Group.newBuilder()
                .setGroup(GroupInfo.newBuilder().setName("test"))
                .setId(groupId)
                .build();
        final TargetGroupUpdate update = new TargetGroupUpdate(targetId, identityProvider,
                Collections.emptyList(),
                Collections.singletonList(groupToDelete));
        update.apply(storeInstance, updateInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(storeInstance);

        verify(removeInstance).removeInstance(groupCaptor.capture());

        final Group deletedGroup = groupCaptor.getValue();
        assertEquals(groupToDelete.getId(), deletedGroup.getId());
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

        update.apply(storeInstance, updateInstance, removeInstance);

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
        update.apply(storeInstance, updateInstance, removeInstance);

        // Make sure the lower ID got removed.
        verify(removeInstance).removeInstance(groupCaptor.capture());
        assertEquals(1, groupCaptor.getValue().getId());

        // The other should have gotten updated.
        verify(updateInstance).updateInstance(groupCaptor.capture());
        final Group updatedGroup = groupCaptor.getValue();
        assertEquals(2, updatedGroup.getId());
    }

    @Test
    public void testTargetPolicyDisabled() throws Exception {
        final TargetPolicyUpdate update = new TargetPolicyUpdate(targetId, identityProvider,
            Collections.singletonList(PolicyInfo.newBuilder()
                .setName("test")
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setProviderGroupId(1)
                    .setConsumerGroupId(2))
                .build()),
            Collections.singletonList(Policy.newBuilder()
                .setId(7L)
                .setTargetId(targetId)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName("test")
                    .setEnabled(false)
                    .setBindToGroup(BindToGroupPolicy.newBuilder()
                        .setProviderGroupId(1)
                        .setConsumerGroupId(3)))
                .build()));
        update.apply(policyStoreInstance, policyUpdateInstance, removeInstance);
        verifyZeroInteractions(identityProvider);
        verifyZeroInteractions(removeInstance);

        verify(policyUpdateInstance).updateInstance(policyCaptor.capture());

        final Policy inputPolicy = policyCaptor.getValue();
        assertFalse(inputPolicy.getPolicyInfo().getEnabled());
        assertEquals("test", inputPolicy.getPolicyInfo().getName());
        assertEquals(2, inputPolicy.getPolicyInfo().getBindToGroup().getConsumerGroupId());
        assertEquals(1, inputPolicy.getPolicyInfo().getBindToGroup().getProviderGroupId());
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
        update.apply(settingPolicyStoreInstance, settingPolicyUpdateInstance, removeInstance);
        verifyZeroInteractions(removeInstance);
        verify(settingPolicyStoreInstance).storeInstance(settingPolicyCaptor.capture());

        final SettingPolicy createdSettingPolicy = settingPolicyCaptor.getValue();
        assertEquals(groupId, createdSettingPolicy.getId());
        assertEquals(12345L, createdSettingPolicy.getInfo().getTargetId());
        assertEquals(Type.DISCOVERED, createdSettingPolicy.getSettingPolicyType());
        assertEquals("test", createdSettingPolicy.getInfo().getName());
    }
}
