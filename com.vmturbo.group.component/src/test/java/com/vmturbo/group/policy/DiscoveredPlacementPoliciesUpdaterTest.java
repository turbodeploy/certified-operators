package com.vmturbo.group.policy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * Unit test for {@link DiscoveredPlacementPolicyUpdater}.
 */
public class DiscoveredPlacementPoliciesUpdaterTest {
    private static final long oid1 = 10001L;
    private static final long oid2 = 10001L;
    private static final long oid3 = 10001L;
    private static final long oid4 = 10001L;
    private static final long TGT1 = 20001L;
    private static final long TGT2 = 20002L;
    private static final long GROUP_1 = 30001;
    private static final long GROUP_2 = 30002;
    private static final long GROUP_3 = 30003;
    private static final long GROUP_4 = 30004;
    private static final String GROUP_1_NAME = "Rawenclaw";
    private static final String GROUP_2_NAME = "Gryffindor";
    private static final String GROUP_3_NAME = "Hufflepuff";
    private static final String GROUP_4_NAME = "Slytherin";
    private static final String POLICY1_NAME = "Do not enter the forbidden forest";
    private static final String POLICY2_NAME = "No Entering Other House Dormitories";
    private Table<Long, String, Long> groupIdsMap;
    private static final DiscoveredPolicyInfo POLICY1_INFO = DiscoveredPolicyInfo.newBuilder()
            .setPolicyName(POLICY1_NAME)
            .setConstraintType(1)
            .setBuyersGroupStringId(GROUP_1_NAME)
            .setSellersGroupStringId(GROUP_2_NAME)
            .build();
    private static final DiscoveredPolicyInfo POLICY2_INFO = DiscoveredPolicyInfo.newBuilder()
            .setPolicyName(POLICY2_NAME)
            .setConstraintType(2)
            .setBuyersGroupStringId(GROUP_3_NAME)
            .setSellersGroupStringId(GROUP_4_NAME)
            .build();

    private IdentityProvider identityProvider;
    private IPlacementPolicyStore policyStore;
    @Captor
    private ArgumentCaptor<Collection<Policy>> policiesCaptor;
    private DiscoveredPlacementPolicyUpdater updater;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.next()).thenReturn(oid2).thenReturn(oid3).thenReturn(oid4);
        policyStore = Mockito.mock(IPlacementPolicyStore.class);
        groupIdsMap = HashBasedTable.create();
        groupIdsMap.put(TGT1, GROUP_1_NAME, GROUP_1);
        groupIdsMap.put(TGT1, GROUP_2_NAME, GROUP_2);
        groupIdsMap.put(TGT1, GROUP_3_NAME, GROUP_3);
        groupIdsMap.put(TGT1, GROUP_4_NAME, GROUP_4);
        groupIdsMap.put(TGT2, GROUP_1_NAME, GROUP_1);
        groupIdsMap.put(TGT2, GROUP_2_NAME, GROUP_2);
        groupIdsMap.put(TGT2, GROUP_3_NAME, GROUP_3);
        groupIdsMap.put(TGT2, GROUP_4_NAME, GROUP_4);
        updater = new DiscoveredPlacementPolicyUpdater(identityProvider);
    }

    /**
     * Tests updating of the existing policy in the store.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testExistingPolicyUpdate() throws Exception {
        Mockito.when(policyStore.getDiscoveredPolicies()).thenReturn(
                Collections.singletonMap(TGT1, Collections.singletonMap(POLICY1_NAME, oid1)));
        updater.updateDiscoveredPolicies(policyStore,
                Collections.singletonMap(TGT1, Collections.singletonList(POLICY1_INFO)),
                groupIdsMap, Collections.emptySet());
        Mockito.verify(policyStore).deletePolicies(Collections.singletonList(oid1));
        Mockito.verify(policyStore).createPolicies(policiesCaptor.capture());
        Assert.assertEquals(Collections.singleton(oid1),
                policiesCaptor.getValue().stream().map(Policy::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests adding a new placement policy.
     *
     * @throws Exception on exceptions expected
     */
    @Test
    public void testNewPolicy() throws Exception {
        Mockito.when(policyStore.getDiscoveredPolicies()).thenReturn(
                Collections.singletonMap(TGT1, Collections.singletonMap(POLICY1_NAME, oid1)));
        updater.updateDiscoveredPolicies(policyStore,
                Collections.singletonMap(TGT1, Arrays.asList(POLICY1_INFO, POLICY2_INFO)),
                groupIdsMap, Collections.emptySet());
        Mockito.verify(policyStore).deletePolicies(Collections.singletonList(oid1));
        Mockito.verify(policyStore).createPolicies(policiesCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(oid1, oid2),
                policiesCaptor.getValue().stream().map(Policy::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests a policy is reported for one target, and the other target is not affected. It is
     * expected that we do not remove policy related to a target that is not reported.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testTargetIgnored() throws Exception {
        Mockito.when(policyStore.getDiscoveredPolicies()).thenReturn(
                Collections.singletonMap(TGT1, Collections.singletonMap(POLICY1_NAME, oid1)));
        updater.updateDiscoveredPolicies(policyStore,
                Collections.singletonMap(TGT2, Arrays.asList(POLICY1_INFO, POLICY2_INFO)),
                groupIdsMap, Collections.singleton(TGT1));
        Mockito.verify(policyStore).deletePolicies(Collections.emptyList());
        Mockito.verify(policyStore).createPolicies(policiesCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(oid2),
                policiesCaptor.getValue().stream().map(Policy::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests a policy is reported for one target, and the other target is no longer is the list
     * of policies that report discovered policy. It is expected that we remove the policies
     * previous discovered by other target.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testTargetRemoved() throws Exception {
        Mockito.when(policyStore.getDiscoveredPolicies()).thenReturn(
            Collections.singletonMap(TGT1, Collections.singletonMap(POLICY1_NAME, oid1)));
        updater.updateDiscoveredPolicies(policyStore,
            Collections.singletonMap(TGT2, Arrays.asList(POLICY1_INFO, POLICY2_INFO)),
            groupIdsMap, Collections.emptySet());
        Mockito.verify(policyStore).deletePolicies(Collections.singletonList(oid1));
        Mockito.verify(policyStore).createPolicies(policiesCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(oid2),
            policiesCaptor.getValue().stream().map(Policy::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests if the policy reported is invalid. We just ignore it.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testPolicyIsInvalid() throws Exception {
        final DiscoveredPolicyInfo policy = DiscoveredPolicyInfo.newBuilder(POLICY2_INFO)
                .setBuyersGroupStringId("non-existing-group")
                .build();
        Mockito.when(policyStore.getDiscoveredPolicies()).thenReturn(
                Collections.singletonMap(TGT1, Collections.singletonMap(POLICY1_NAME, oid1)));
        updater.updateDiscoveredPolicies(policyStore,
                Collections.singletonMap(TGT1, Arrays.asList(POLICY1_INFO, policy)), groupIdsMap, Collections.emptySet());
        Mockito.verify(policyStore).deletePolicies(Collections.singletonList(oid1));
        Mockito.verify(policyStore).createPolicies(policiesCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(oid1),
                policiesCaptor.getValue().stream().map(Policy::getId).collect(Collectors.toSet()));
    }
}
