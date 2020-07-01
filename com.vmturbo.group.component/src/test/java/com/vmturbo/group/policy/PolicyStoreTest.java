package com.vmturbo.group.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.PolicyGroup;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.group.db.tables.records.PolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class PolicyStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final long TARGET_ID = 107L;

    private static final long POLICY_ID = 7L;

    private static final long CONSUMER_GROUP_ID = 1L;

    private static final long PRODUCER_GROUP_ID = 2L;

    private static final PolicyInfo POLICY_INFO = PolicyInfo.newBuilder()
            .setName("no watch tv")
            .setEnabled(true)
            .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(CONSUMER_GROUP_ID)
                    .setProviderGroupId(PRODUCER_GROUP_ID))
            .build();

    private static final DiscoveredPolicyInfo DISCOVERED_POLICY_INFO = DiscoveredPolicyInfo.newBuilder()
            .setBuyersGroupStringId(Long.toString(CONSUMER_GROUP_ID))
            .setSellersGroupStringId(Long.toString(PRODUCER_GROUP_ID))
            .setPolicyName(POLICY_INFO.getName())
            .setConstraintType(1)
            .build();

    private static final Table<Long, String, Long> GROUP_TO_OID_MAP = ImmutableTable
            .<Long, String, Long>builder()
            .put(TARGET_ID, Long.toString(CONSUMER_GROUP_ID), CONSUMER_GROUP_ID)
            .put(TARGET_ID, Long.toString(PRODUCER_GROUP_ID), PRODUCER_GROUP_ID)
            .build();

    private PolicyStore policyStore;

    private PolicyValidator policyValidator = mock(PolicyValidator.class);

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private DiscoveredPoliciesMapper discoveredPoliciesMapper = mock(DiscoveredPoliciesMapper.class);

    @Before
    public void setup() {
        final DSLContext dslContext = dbConfig.getDslContext();
        policyStore = new PolicyStore(dslContext, identityProvider, policyValidator);
        injectGroup(CONSUMER_GROUP_ID);
        injectGroup(PRODUCER_GROUP_ID);
    }

    @Test
    public void testCreateAndGet() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);
        assertThat(policy.getId(), is(POLICY_ID));
        assertThat(policy.getPolicyInfo(), is(POLICY_INFO));
        assertFalse(policy.hasTargetId());

        final Policy gotPolicy = policyStore.get(POLICY_ID).get();
        assertThat(gotPolicy, is(policy));

        final Map<Long, Set<Long>> policyToGroup = getPolicyToGroupMapping();
        assertThat(policyToGroup.keySet(), contains(POLICY_ID));
        assertThat(policyToGroup.get(POLICY_ID), contains(CONSUMER_GROUP_ID, PRODUCER_GROUP_ID));
    }

    @Test
    public void testGetAll() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        final Collection<Policy> gotPolicies = policyStore.getAll();
        assertThat(gotPolicies.size(), is(1));

        final Policy gotPolicy = gotPolicies.iterator().next();
        assertThat(gotPolicy, is(policy));
    }

    @Test(expected = DuplicateNameException.class)
    public void testCreateDuplicateName() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID).thenReturn(POLICY_ID + 1);
        policyStore.newUserPolicy(POLICY_INFO);

        // Create another policy with the same name.
        policyStore.newUserPolicy(POLICY_INFO);
    }

    /**
     * Test that creation attempts for policies that fail validation throw the expected exception.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test(expected = InvalidPolicyException.class)
    public void testCreateInvalidPolicy() throws Exception {
        doThrow(new InvalidPolicyException(Collections.singletonList("Bad policy.")))
            .when(policyValidator).validatePolicy(any(), any());
        when(identityProvider.next()).thenReturn(POLICY_ID).thenReturn(POLICY_ID + 1);
        policyStore.newUserPolicy(POLICY_INFO);
    }

    @Test
    public void testEditNewGroupAssociation() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        final long newConsumerGroup = CONSUMER_GROUP_ID + 100;
        injectGroup(newConsumerGroup);

        final PolicyInfo newPolicyInfo = PolicyInfo.newBuilder(POLICY_INFO)
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                        .setConsumerGroupId(newConsumerGroup)
                        .setProviderGroupId(PRODUCER_GROUP_ID))
                .build();

        final Policy newPolicy = policyStore.editPolicy(POLICY_ID, newPolicyInfo);
        assertThat(newPolicy.getId(), is(POLICY_ID));
        assertThat(newPolicy.getPolicyInfo(), is(newPolicyInfo));
        assertFalse(policy.hasTargetId());

        final Policy gotPolicy = policyStore.get(POLICY_ID).get();
        assertThat(gotPolicy, is(newPolicy));

        final Map<Long, Set<Long>> policyToGroup = getPolicyToGroupMapping();
        assertThat(policyToGroup.keySet(), contains(POLICY_ID));
        assertThat(policyToGroup.get(POLICY_ID), containsInAnyOrder(newConsumerGroup, PRODUCER_GROUP_ID));
    }

    @Test(expected = DuplicateNameException.class)
    public void testEditDuplicateName() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID).thenReturn(POLICY_ID + 1);
        final String dupeName = "the DUPE";
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        policyStore.newUserPolicy(POLICY_INFO.toBuilder()
                .setName(dupeName)
                .build());

        policyStore.editPolicy(policy.getId(), POLICY_INFO.toBuilder()
                .setName(dupeName)
                .build());
    }

    /**
     * Test that edit attempts for policies that fail validation throw the expected exception.
     *
     * @throws Exception To satisfy the compiler.
     */
    @Test(expected = InvalidPolicyException.class)
    public void testEditInvalidPolicy() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID).thenReturn(POLICY_ID + 1);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        doThrow(new InvalidPolicyException(Collections.singletonList("Bad policy.")))
            .when(policyValidator).validatePolicy(any(), any());

        policyStore.editPolicy(policy.getId(), POLICY_INFO);
    }


    @Test(expected = PolicyNotFoundException.class)
    public void testEditPoliyNotFound() throws Exception {
        policyStore.editPolicy(1L, POLICY_INFO);
    }

    @Test
    public void testDelete() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        assertTrue(policyStore.get(policy.getId()).isPresent());
        policyStore.deleteUserPolicy(policy.getId());
        // Group shouldn't be found anymore.
        assertFalse(policyStore.get(policy.getId()).isPresent());

        // Policy-to-group mappings should be cleared.
        assertTrue(getPolicyToGroupMapping().isEmpty());
    }

    @Test(expected = PolicyNotFoundException.class)
    public void testDeleteNotFound() throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        policyStore.deleteUserPolicy(1L);
    }

    @Test(expected = ImmutablePolicyUpdateException.class)
    public void testDeleteDiscoveredException() throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        final PolicyRecord policyRecord = dbConfig.getDslContext().newRecord(Tables.POLICY);
        policyRecord.setEnabled(true);
        policyRecord.setName("foo");
        policyRecord.setId(POLICY_ID);
        // Discovered group
        policyRecord.setDiscoveredById(TARGET_ID);
        policyRecord.setPolicyData(PolicyInfo.getDefaultInstance());
        policyRecord.store();

        policyStore.deleteUserPolicy(POLICY_ID);
    }

    @Test
    public void testDeletePoliciesForGroup() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        policyStore.newUserPolicy(POLICY_INFO.toBuilder().setName("User Policy").build());

        assertTrue(policyStore.get(POLICY_ID).isPresent());

        // add a discovered policy scoped to the same groups
        long discoveredPolicyId = 77L;
        when(identityProvider.next()).thenReturn(discoveredPolicyId);
        when(discoveredPoliciesMapper.inputPolicy(DISCOVERED_POLICY_INFO)).thenReturn(Optional.of(POLICY_INFO));
        final DiscoveredPlacementPolicyUpdater updater = new DiscoveredPlacementPolicyUpdater(identityProvider);
        updater.updateDiscoveredPolicies(policyStore, Collections.singletonMap(TARGET_ID, Collections.singletonList(DISCOVERED_POLICY_INFO)),
                GROUP_TO_OID_MAP, Collections.emptySet());

        // delete the discovered group
        policyStore.deletePoliciesForGroupBeingRemoved(Collections.singleton(CONSUMER_GROUP_ID));

        // the user policy should have been deleted
        assertFalse(policyStore.get(POLICY_ID).isPresent());
        // the discovered policy should still be there.
        assertFalse(policyStore.get(discoveredPolicyId).isPresent());
        Map<Long, Set<Long>> policyToGroupMap = getPolicyToGroupMapping();
        Assert.assertEquals(Collections.emptyMap(), policyToGroupMap);
    }

    @Test
    public void testDiagsRoundTrip() throws Exception {
        when(identityProvider.next()).thenReturn(POLICY_ID);
        final Policy policy = policyStore.newUserPolicy(POLICY_INFO);

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        policyStore.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());
        dbConfig.getDslContext().transaction(config -> {
            DSL.using(config).deleteFrom(Tables.POLICY);
        });

        final PolicyStore newPolicyStore =
                new PolicyStore(dbConfig.getDslContext(), identityProvider, policyValidator);
        newPolicyStore.restoreDiags(diags.getAllValues());

        final Policy gotPolicy = policyStore.get(POLICY_ID).get();
        assertThat(gotPolicy, is(policy));

        final Map<Long, Set<Long>> policyToGroup = getPolicyToGroupMapping();
        assertThat(policyToGroup.keySet(), contains(POLICY_ID));
        assertThat(policyToGroup.get(POLICY_ID), contains(CONSUMER_GROUP_ID, PRODUCER_GROUP_ID));
    }

    private void injectGroup(final long id) {
        final GroupingRecord record = dbConfig.getDslContext().newRecord(Tables.GROUPING);
        record.setId(id);
        record.setDisplayName("Fuel Injected " + id);
        record.setGroupType(GroupType.REGULAR);
        record.setOriginUserCreator("super-user");
        record.setSupportsMemberReverseLookup(true);
        record.setIsHidden(false);
        record.store();
    }

    @Nonnull
    private Map<Long, Set<Long>> getPolicyToGroupMapping() {
        return dbConfig.getDslContext().selectFrom(Tables.POLICY_GROUP)
                .fetch()
                .into(PolicyGroup.class)
                .stream()
                .collect(Collectors.toMap(
                        PolicyGroup::getPolicyId,
                        group -> {
                            final Set<Long> list = new HashSet<>();
                            list.add(group.getGroupId());
                            return list;
                        },
                        (v1, v2) -> {
                            v1.addAll(v2);
                            return v1;
                        }));
    }
}
