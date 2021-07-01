package com.vmturbo.topology.processor.targets.status;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;

/**
 * Tests implementation of {@link SQLTargetStatusStore}.
 */
public class SQLTargetStatusStoreTest {
    private static final long TARGET_ID_1 = 1L;
    private static final long TARGET_ID_2 = 2L;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(
            com.vmturbo.topology.processor.db.tables.TargetStatus.TARGET_STATUS.getSchema());

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private TargetStatusStore targetStatusStore;

    /**
     * Initialise test environment.
     */
    @Before
    public void initialize() {
        targetStatusStore = new SQLTargetStatusStore(dbConfig.getDslContext());
    }

    /**
     * Tests persisting and fetching all targets statuses.
     *
     * @throws TargetStatusStoreException if something goes wrong with DB communication
     */
    @Test
    public void testPopulatingAndFetchingAllTargetsStatuses() throws TargetStatusStoreException {
        // ARRANGE
        final long operationCompletionTime = 12312412L;
        final TargetStatus targetStatus1 = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_1)
                .setOperationCompletionTime(operationCompletionTime)
                .build();
        final List<ProbeStageDetails> stagesDetails = Collections.singletonList(
                createStageDetails(StageStatus.FAILURE, "Discovery stage"));
        final TargetStatus targetStatus2 = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_2)
                .setOperationCompletionTime(operationCompletionTime)
                .addAllStageDetails(stagesDetails)
                .build();

        // ACT
        // persist target status in DB
        targetStatusStore.setTargetStatus(targetStatus1);
        targetStatusStore.setTargetStatus(targetStatus2);
        // fetch all existed targets statuses
        final Map<Long, TargetStatus> targetsStatuses = targetStatusStore.getTargetsStatuses(null);

        // ASSERT
        Assert.assertEquals(2, targetsStatuses.size());
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(Sets.newHashSet(TARGET_ID_1, TARGET_ID_2),
                        targetsStatuses.keySet()));
        final TargetStatus persistedTargetStatus1 = targetsStatuses.get(TARGET_ID_1);
        Assert.assertEquals(TARGET_ID_1, persistedTargetStatus1.getTargetId());
        Assert.assertEquals(operationCompletionTime,
                persistedTargetStatus1.getOperationCompletionTime());
        Assert.assertTrue(persistedTargetStatus1.getStageDetailsList().isEmpty());
        final TargetStatus persistedTargetStatus2 = targetsStatuses.get(TARGET_ID_2);
        Assert.assertEquals(TARGET_ID_2, persistedTargetStatus2.getTargetId());
        Assert.assertEquals(operationCompletionTime,
                persistedTargetStatus2.getOperationCompletionTime());
        Assert.assertTrue(CollectionUtils.isEqualCollection(stagesDetails,
                persistedTargetStatus2.getStageDetailsList()));
    }

    /**
     * Tests persisting and fetching status for the certain target.
     *
     * @throws TargetStatusStoreException if something goes wrong with DB communication
     */
    @Test
    public void testPopulatingAndFetchingStatusForCertainTarget()
            throws TargetStatusStoreException {
        // ARRANGE
        final long operationCompletionTime = 12312412L;
        final List<ProbeStageDetails> stagesDetails = Arrays.asList(
                createStageDetails(StageStatus.SUCCESS, "Discovery stage 1"),
                createStageDetails(StageStatus.FAILURE, "Discovery stage 2"));
        final TargetStatus targetStatus = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_1)
                .setOperationCompletionTime(operationCompletionTime)
                .addAllStageDetails(stagesDetails)
                .build();

        // ACT
        targetStatusStore.setTargetStatus(targetStatus);
        final Map<Long, TargetStatus> targetsStatuses = targetStatusStore.getTargetsStatuses(
                Collections.singleton(TARGET_ID_1));

        // ASSERT
        Assert.assertEquals(1, targetsStatuses.size());
        final TargetStatus persistedTargetStatus = targetsStatuses.get(TARGET_ID_1);
        Assert.assertEquals(TARGET_ID_1, persistedTargetStatus.getTargetId());
        Assert.assertTrue(CollectionUtils.isEqualCollection(stagesDetails, persistedTargetStatus.getStageDetailsList()));
    }

    /**
     * Tests cleaning up target status data when target is deleted.
     *
     * @throws TargetStatusStoreException if something goes wrong with DB communication
     */
    @Test
    public void testCleanUpWhenTargetDeleted() throws TargetStatusStoreException {
        // ARRANGE
        final long operationCompletionTime = 12312412L;
        final TargetStatus targetStatus1 = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_1)
                .setOperationCompletionTime(operationCompletionTime)
                .build();

        final List<ProbeStageDetails> stagesDetails = Collections.singletonList(
                createStageDetails(StageStatus.FAILURE, "Discovery stage"));
        final TargetStatus targetStatus2 = TargetStatus.newBuilder()
                .setTargetId(TARGET_ID_2)
                .setOperationCompletionTime(operationCompletionTime)
                .addAllStageDetails(stagesDetails)
                .build();

        // ACT
        // persist target status in DB
        targetStatusStore.setTargetStatus(targetStatus1);
        targetStatusStore.setTargetStatus(targetStatus2);
        // delete target with TARGET_ID_1 id
        targetStatusStore.deleteTargetStatus(TARGET_ID_1);
        // fetch all existed targets statuses
        final Map<Long, TargetStatus> targetsStatuses = targetStatusStore.getTargetsStatuses(null);

        // ASSERT
        Assert.assertEquals(1, targetsStatuses.size());
        final TargetStatus persistedTargetStatus1 = targetsStatuses.get(TARGET_ID_1);
        final TargetStatus persistedTargetStatus2 = targetsStatuses.get(TARGET_ID_2);
        Assert.assertNull(persistedTargetStatus1);
        Assert.assertNotNull(persistedTargetStatus2);
    }

    @Nonnull
    private ProbeStageDetails createStageDetails(final StageStatus failure,
            final String description) {
        return ProbeStageDetails.newBuilder()
                .setStatus(failure)
                .setDescription(description)
                .build();
    }
}