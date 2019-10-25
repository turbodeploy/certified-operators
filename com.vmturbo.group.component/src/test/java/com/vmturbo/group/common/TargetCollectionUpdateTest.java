package com.vmturbo.group.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.common.TargetCollectionUpdate.RemoveInstance;
import com.vmturbo.group.common.TargetCollectionUpdate.StoreInstance;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetSettingPolicyUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.UpdateInstance;
import com.vmturbo.group.identity.IdentityProvider;

public class TargetCollectionUpdateTest {

    private final long groupId = 7L;

    private final long targetId = 10L;

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    @Mock
    private RemoveInstance removeInstance;

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
