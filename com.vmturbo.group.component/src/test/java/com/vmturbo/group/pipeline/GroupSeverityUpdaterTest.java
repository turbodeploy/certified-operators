package com.vmturbo.group.pipeline;

import static com.vmturbo.group.GroupMockUtil.mockEnvironment;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Sets;

import io.grpc.Status;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.BadSqlGrammarException;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.MockGroupStore;
import com.vmturbo.group.service.MockTransactionProvider;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;

/**
 * Unit tests for {@link GroupSeverityUpdater}.
 */
public class GroupSeverityUpdaterTest {

    private final CachingMemberCalculator memberCache = mock(CachingMemberCalculator.class);

    private final GroupSeverityCalculator groupSeverityCalculator =
            mock(GroupSeverityCalculator.class);

    private MockGroupStore groupStoreMock;

    private MockTransactionProvider transactionProvider;

    private ExecutorService executorService;

    /**
     * Sets up environment for tests.
     */
    @Before
    public void setUp() {
        transactionProvider = new MockTransactionProvider();
        groupStoreMock = transactionProvider.getGroupStore();
        executorService = Executors.newFixedThreadPool(2);
    }

    /**
     * Tests that {@link GroupSeverityUpdater#refreshGroupSeverities()} refreshes groups' severity
     * data.
     *
     * @throws StoreOperationException to satisfy compiler
     * @throws InterruptedException to satisfy compiler
     */
    @Test
    public void testRefreshGroupSeverities() throws StoreOperationException, InterruptedException {
        // GIVEN
        final long group1Uuid = 1001L;
        final long group2Uuid = 1002L;
        final long entity1Uuid = 2001L;
        final long entity2Uuid = 2002L;
        final GroupSeverityUpdater updater = new GroupSeverityUpdater(memberCache,
                groupSeverityCalculator, groupStoreMock, transactionProvider, executorService, 10);
        groupStoreMock.createGroupSupplementaryInfo(group1Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreMock.createGroupSupplementaryInfo(group2Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        LongOpenHashSet groupIds = new LongOpenHashSet();
        groupIds.addAll(Arrays.asList(group1Uuid, group2Uuid));
        when(memberCache.getCachedGroupIds()).thenReturn(groupIds);
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group1Uuid), true))
                .thenReturn(Sets.newHashSet(entity1Uuid));
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group2Uuid), true))
                .thenReturn(Sets.newHashSet(entity2Uuid));
        when(groupSeverityCalculator.calculateSeverity(any()))
                .thenReturn(Severity.MAJOR);
//        when(groupStoreMock.updateBulkGroupsSeverity(any())).thenReturn(2);
        // WHEN
        final boolean result = updater.refreshGroupSeverities();
        // THEN
        Assert.assertTrue(result);
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        verify(groupStoreMock, times(1)).updateBulkGroupsSeverity(captor.capture());
        // validate the arguments passed to updateBulkGroupsSeverity (only groupUuid and severity
        // matter, since the rest are ignored during severity updates)
        Assert.assertEquals(2, captor.getValue().size());
        Iterator<GroupSupplementaryInfo> it = captor.getValue().iterator();
        GroupSupplementaryInfo g1 = it.next();
        Assert.assertEquals(group1Uuid, g1.getGroupId().longValue());
        Assert.assertEquals(Severity.MAJOR.getNumber(), g1.getSeverity().intValue());
        GroupSupplementaryInfo g2 = it.next();
        Assert.assertEquals(group2Uuid, g2.getGroupId().longValue());
        Assert.assertEquals(Severity.MAJOR.getNumber(), g2.getSeverity().intValue());
    }

    /**
     * Test that if we get an exception during the retrieval of a group's members, we just skip this
     * group and continue resolving the rest.
     *
     * @throws StoreOperationException to satisfy compiler
     * @throws InterruptedException to satisfy compiler
     */
    @Test
    public void testThatExecutionContinuesAfterSingleGroupFailure()
            throws StoreOperationException, InterruptedException {
        // GIVEN
        final long group1Uuid = 1001L;
        final long group2Uuid = 1002L;
        final Severity newSeverity = Severity.MAJOR;
        final GroupSeverityUpdater updater = new GroupSeverityUpdater(memberCache,
                groupSeverityCalculator, groupStoreMock, transactionProvider, executorService, 10);
        groupStoreMock.createGroupSupplementaryInfo(group1Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreMock.createGroupSupplementaryInfo(group2Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        LongOpenHashSet groupIds = new LongOpenHashSet();
        groupIds.addAll(Arrays.asList(group1Uuid, group2Uuid));
        when(memberCache.getCachedGroupIds()).thenReturn(groupIds);
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group1Uuid), true))
                .thenThrow(new BadSqlGrammarException(null, "SqlQuery", new SQLException()));
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group2Uuid), true))
                .thenReturn(Sets.newHashSet(1L));
        when(groupSeverityCalculator.calculateSeverity(any()))
                .thenReturn(newSeverity);
        groupStoreMock.createGroupSupplementaryInfo(group2Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        // WHEN
        final boolean result = updater.refreshGroupSeverities();
        // THEN
        Assert.assertTrue(result);
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        verify(groupStoreMock, times(1)).updateBulkGroupsSeverity(captor.capture());
        // Validate that group 2 was added to the groups to be updated. This would indicate that
        // processing didn't stop after group 1 threw an exception during member retrieval.
        Assert.assertEquals(1, captor.getValue().size());
        Iterator<GroupSupplementaryInfo> it = captor.getValue().iterator();
        GroupSupplementaryInfo gsi = it.next();
        Assert.assertEquals(group2Uuid, gsi.getGroupId().longValue());
        Assert.assertEquals(newSeverity.getNumber(), gsi.getSeverity().intValue());
    }

    /**
     * Test that if the processing of a batch fails, processing the rest continues and the update
     * (partially) succeeds.
     *
     * @throws StoreOperationException to satisfy compiler
     * @throws InterruptedException to satisfy compiler
     */
    @Test
    public void testThatExecutionContinuesAfterSingleBatchFailure()
            throws StoreOperationException, InterruptedException {
        // GIVEN
        final long group1Uuid = 1001L;
        final long group2Uuid = 1002L;
        final Severity newSeverity = Severity.MAJOR;
        final TransactionProvider mockTransactionProvider = mock(TransactionProvider.class);
        final GroupSeverityUpdater updater = new GroupSeverityUpdater(memberCache,
                groupSeverityCalculator, groupStoreMock, mockTransactionProvider,
                executorService, 1);
        groupStoreMock.createGroupSupplementaryInfo(group1Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        groupStoreMock.createGroupSupplementaryInfo(group2Uuid, false,
                mockEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD),
                Severity.NORMAL);
        LongOpenHashSet groupIds = new LongOpenHashSet();
        groupIds.addAll(Arrays.asList(group1Uuid, group2Uuid));
        when(memberCache.getCachedGroupIds()).thenReturn(groupIds);
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group1Uuid), true))
                .thenReturn(Sets.newHashSet(1L));
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(group2Uuid), true))
                .thenReturn(Sets.newHashSet(2L));
        when(groupSeverityCalculator.calculateSeverity(any()))
                .thenReturn(newSeverity);
        when(mockTransactionProvider.transaction(any()))
                // 1 batch succeeds
                .thenReturn(1)
                // 1 batch fails
                .thenThrow(new StoreOperationException(Status.DATA_LOSS,
                        "db error during severity update"));
        // WHEN
        final boolean result = updater.refreshGroupSeverities();
        // THEN
        // verify that the process succeeds since at least 1 batch succeeded.
        Assert.assertTrue(result);
    }
}
