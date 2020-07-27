package com.vmturbo.topology.processor.stitching.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.billing.AwsBillingBusinessAccountStitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Integration test for the {@link AwsBillingBusinessAccountStitchingOperation}.
 */
public class AwsBillingBusinessAccountStitchingOperationIntegrationTest extends StitchingIntegrationTest {
    private static final long AWS_PROBE_ID = 1111L;
    private static final long AWS_TARGET_ID = 2222L;

    private static final long BILLING_PROBE_ID = 1234L;
    private static final long BILLING_TARGET_ID = 1235L;

    private StitchingManager stitchingManager;

    /**
     * Pre-test setup. This will run after {@link StitchingIntegrationTest#integrationSetup()}.
     */
    @Before
    public void setup() {

        setOperationsForProbe(BILLING_PROBE_ID,
            Collections.singletonList(new AwsBillingBusinessAccountStitchingOperation()));
        setOperationsForProbe(AWS_PROBE_ID, Collections.emptyList());

        stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
            cpuCapacityStore);

        final Target awsBillingTarget = mock(Target.class);
        when(awsBillingTarget.getId()).thenReturn(BILLING_TARGET_ID);

        when(targetStore.getProbeTargets(BILLING_PROBE_ID))
            .thenReturn(Collections.singletonList(awsBillingTarget));

        final Target awsTarget = mock(Target.class);
        when(awsTarget.getId()).thenReturn(AWS_TARGET_ID);

        when(targetStore.getProbeTargets(AWS_PROBE_ID))
            .thenReturn(Collections.singletonList(awsTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.BILLING))
            .thenReturn(Collections.singletonList(BILLING_PROBE_ID));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.CLOUD_MANAGEMENT))
            .thenReturn(Collections.singletonList(AWS_PROBE_ID));
        when(probeStore.getProbeIdForType(SDKProbeType.AWS.getProbeType()))
            .thenReturn(Optional.of(AWS_PROBE_ID));
        when(probeStore.getProbeIdForType(SDKProbeType.AWS_BILLING.getProbeType()))
            .thenReturn(Optional.of(AWS_PROBE_ID));
    }

    /**
     * Test the case where only the AWS probe is reporting the business accounts.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAwsOnlyBusinessAccounts() throws Exception {
        final EntityDTO awsMasterAccount = account("1", "master", Collections.singletonList("2"));
        final EntityDTO awsOwnedAccount = account("2", "owned", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
            ImmutableMap.of(2L, awsOwnedAccount, 1L, awsMasterAccount);

        addEntities(Collections.emptyMap(), BILLING_TARGET_ID);
        addEntities(awsEntities, AWS_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(stitchedTopology.get(2L).getEntityBuilder().getConnectedEntityListList(),
            empty());
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getConnectedEntityListList(),
            contains(ConnectedEntity.newBuilder()
                .setConnectedEntityId(2L)
                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .build()));
    }

    /**
     * Test the case where only the AWS billing probe is reporting the business account display name.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAwsOnlyBusinessAccountsWithNoDisplayName() throws Exception {
        final EntityDTO billingMasterAccount = account("1", "billing Account name", Collections.singletonList("2"));
        final EntityDTO billingOwnedAccount = account("2", "owned1", Collections.emptyList());

        final Map<Long, EntityDTO> billingEntities =
                ImmutableMap.of(1L, billingMasterAccount, 2L, billingOwnedAccount);

        final EntityDTO awsMasterAccount = account("1", "", Collections.singletonList("3"));
        final EntityDTO awsOwnedAccount = account("3", "owned2", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
                ImmutableMap.of(1L, awsMasterAccount, 3L, awsOwnedAccount);

        addEntities(awsEntities, AWS_TARGET_ID);
        addEntities(billingEntities, BILLING_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getDisplayName(),
                is(billingEntities.get(1L).getDisplayName()));
    }

    /**
     * Test the case where there's no billing account and main probe does not have account name.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAwsWithNoDisplayName() throws Exception {

        final EntityDTO awsMasterAccount = account("1", "", Collections.singletonList("3"));
        final EntityDTO awsOwnedAccount = account("3", "owned2", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
                ImmutableMap.of(1L, awsMasterAccount, 3L, awsOwnedAccount);

        addEntities(Collections.emptyMap(), BILLING_TARGET_ID);
        addEntities(awsEntities, AWS_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getDisplayName(),
                isEmptyString());
    }

    /**
     * Test the case where both probes are reporting the business account display name. and name from main probe is respected.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAwsOnlyBusinessAccountsWithDisplayName() throws Exception {
        final EntityDTO billingMasterAccount = account("1", "billing Account name", Collections.singletonList("2"));
        final EntityDTO billingOwnedAccount = account("2", "owned1", Collections.emptyList());

        final Map<Long, EntityDTO> billingEntities =
                ImmutableMap.of(1L, billingMasterAccount, 2L, billingOwnedAccount);

        final EntityDTO awsMasterAccount = account("1", "Real Account name", Collections.singletonList("3"));
        final EntityDTO awsOwnedAccount = account("3", "owned2", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
                ImmutableMap.of(1L, awsMasterAccount, 3L, awsOwnedAccount);

        addEntities(awsEntities, AWS_TARGET_ID);
        addEntities(billingEntities, BILLING_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getDisplayName(),
                is(awsEntities.get(1L).getDisplayName()));
    }


    /**
     * Test the case where only the AWS Billing probe is reporting the business accounts.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testBillingOnlyBusinessAccounts() throws Exception {
        final EntityDTO billingMasterAccount = account("1", "master", Collections.singletonList("2"));
        final EntityDTO billingOwnedAccount = account("2", "owned", Collections.emptyList());
        final Map<Long, EntityDTO> billingEntities =
            ImmutableMap.of(2L, billingOwnedAccount, 1L, billingMasterAccount);

        addEntities(Collections.emptyMap(), AWS_TARGET_ID);
        addEntities(billingEntities, BILLING_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(stitchedTopology.get(2L).getEntityBuilder().getConnectedEntityListList(),
            empty());
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getConnectedEntityListList(),
            contains(ConnectedEntity.newBuilder()
                .setConnectedEntityId(2L)
                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .build()));
    }

    /**
     * Test the case where AWS and AWS Billing report non-conflicting ownership information.
     * We should merge the ownership information from the two probes.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMergeBusinessAccounts() throws Exception {
        /*
         * AWS says Account 1 owns Account 3.
         * Billing says Account 1 owns Account 2.
         * Final result: Account 1 owns Account 2 and 3.
         *               Non-relational properties (e.g. display name) preserved from AWS.
         */
        final EntityDTO billingMasterAccount = account("1", "master", Collections.singletonList("2"));
        final EntityDTO billingOwnedAccount = account("2", "owned1", Collections.emptyList());

        final Map<Long, EntityDTO> billingEntities =
            ImmutableMap.of(1L, billingMasterAccount, 2L, billingOwnedAccount);

        final EntityDTO awsMasterAccount = account("1", "m", Collections.singletonList("3"));
        final EntityDTO awsOwnedAccount = account("3", "owned2", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
            ImmutableMap.of(1L, awsMasterAccount, 3L, awsOwnedAccount);

        addEntities(awsEntities, AWS_TARGET_ID);
        addEntities(billingEntities, BILLING_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.keySet(), containsInAnyOrder(1L, 2L, 3L));
        assertThat(stitchedTopology.get(2L).getEntityBuilder().getConnectedEntityListList(),
            empty());
        assertThat(stitchedTopology.get(3L).getEntityBuilder().getConnectedEntityListList(),
            empty());
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getConnectedEntityListList(),
            containsInAnyOrder(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(2L)
                    .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .build(),
                ConnectedEntity.newBuilder()
                    .setConnectedEntityId(3L)
                    .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .build()));
        assertThat(stitchedTopology.get(1L).getDisplayName(), is(awsMasterAccount.getDisplayName()));
    }

    /**
     * Test the case where the AWS and AWS Billing Probe have conflicting ownership directions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRelationshipSwap() throws Exception {
        /*
         * AWS says Account 1 owns Account 2 and Account 3.
         * Billing says Account 1 owns Account 2, Account 2 owns Account 3.
         * Final result: Account 1 owns Account 2 and Account 3.
         */
        final EntityDTO billingMasterAccount = account("2", "billingMaster", Collections.singletonList("1"));
        final EntityDTO billingOwnedAccount = account("1", "billingOwned", Collections.emptyList());

        final Map<Long, EntityDTO> billingEntities =
            ImmutableMap.of(2L, billingMasterAccount, 1L, billingOwnedAccount);

        final EntityDTO awsMasterAccount = account("1", "awsMaster", Collections.singletonList("2"));
        final EntityDTO awsOwnedAccount = account("2", "awsOwned", Collections.emptyList());
        final Map<Long, EntityDTO> awsEntities =
            ImmutableMap.of(1L, awsMasterAccount, 2L, awsOwnedAccount);

        addEntities(awsEntities, AWS_TARGET_ID);
        addEntities(billingEntities, BILLING_TARGET_ID);

        final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
        assertThat(stitchedTopology.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(stitchedTopology.get(2L).getEntityBuilder().getConnectedEntityListList(),
            empty());
        assertThat(stitchedTopology.get(1L).getEntityBuilder().getConnectedEntityListList(),
            containsInAnyOrder(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(2L)
                    .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .build()));
    }

    /**
     * Test the case where the AWS and AWS Billing Probe disagree about who owns an account.
     * We should choose the AWS probe's results.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConflictingOwners() throws Exception {
        // Run it 100 times because there is the possibility of intermittent failures if the operation
        // relies on certain unstable orderings of operations.
        for (int i = 0; i < 100; ++i) {
            /*
             * AWS says Account 1 owns Account 2 and Account 3.
             * Billing says Account 1 owns Account 2, Account 2 owns Account 3.
             * Final result: Account 1 owns Account 2 and Account 3.
             */
            final EntityDTO billingMasterAccount = account("1", "master", Collections.singletonList("2"));
            final EntityDTO billingOwnedAccount = account("2", "owned", Collections.singletonList("3"));
            final EntityDTO billingNestedOwnedAccount = account("3", "nestedOwned", Collections.emptyList());

            final Map<Long, EntityDTO> billingEntities =
                ImmutableMap.of(1L, billingMasterAccount, 2L, billingOwnedAccount, 3L, billingNestedOwnedAccount);

            final EntityDTO awsMasterAccount = account("1", "master", Arrays.asList("2", "3"));
            final EntityDTO awsOwnedAccount1 = account("2", "owned2", Collections.emptyList());
            final EntityDTO awsOwnedAccount2 = account("3", "owned3", Collections.emptyList());
            final Map<Long, EntityDTO> awsEntities =
                ImmutableMap.of(1L, awsMasterAccount, 2L, awsOwnedAccount1, 3L, awsOwnedAccount2);

            addEntities(awsEntities, AWS_TARGET_ID);
            addEntities(billingEntities, BILLING_TARGET_ID);

            final Map<Long, TopologyEntity.Builder> stitchedTopology = stitch();
            assertThat(stitchedTopology.keySet(), containsInAnyOrder(1L, 2L, 3L));
            assertThat(stitchedTopology.get(2L).getEntityBuilder().getConnectedEntityListList(),
                empty());
            assertThat(stitchedTopology.get(3L).getEntityBuilder().getConnectedEntityListList(),
                empty());
            assertThat(stitchedTopology.get(1L).getEntityBuilder().getConnectedEntityListList(),
                containsInAnyOrder(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(2L)
                        .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .build(),
                    ConnectedEntity.newBuilder()
                        .setConnectedEntityId(3L)
                        .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .build()));
            assertThat(stitchedTopology.get(1L).getEntityBuilder().getDisplayName(),
                    is(awsEntities.get(1L).getDisplayName()));
        }
    }

    private Map<Long, TopologyEntity.Builder> stitch() {
        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        final StringBuilderRecorder recorder = new StringBuilderRecorder(journalStringBuilder);
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
            .configurableStitchingJournalFactory(Clock.systemUTC())
            .addRecorder(recorder);
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
            .setVerbosity(Verbosity.COMPLETE_VERBOSITY)
            .build());

        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);

        // We can print the StringBuilderRecorder here.

        return stitchingContext.constructTopology();
    }

    private EntityDTO account(final String id, final String displayName, List<String> ownedIds) {
        return EntityDTO.newBuilder()
            .setId(id)
            .setEntityType(EntityType.BUSINESS_ACCOUNT)
            .setDisplayName(displayName)
            .addAllConsistsOf(ownedIds)
            .build();
    }
}
