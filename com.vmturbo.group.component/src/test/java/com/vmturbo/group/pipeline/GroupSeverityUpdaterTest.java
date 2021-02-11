package com.vmturbo.group.pipeline;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Unit tests for {@link GroupSeverityUpdater}.
 */
public class GroupSeverityUpdaterTest {

    private final CachingMemberCalculator memberCache = mock(CachingMemberCalculator.class);

    private final GroupSeverityCalculator groupSeverityCalculator =
            mock(GroupSeverityCalculator.class);

    private final IGroupStore groupStore = mock(GroupDAO.class);

    /**
     * Tests that {@link GroupSeverityUpdater#refreshGroupSeverities()} refreshes groups' severity
     * data.
     *
     * @throws StoreOperationException to satisfy compiler
     */
    @Test
    public void testRefreshGroupSeverities() throws StoreOperationException {
        // GIVEN
        final long group1Uuid = 1001L;
        final long group2Uuid = 1002L;
        final long entity1Uuid = 2001L;
        final long entity2Uuid = 2002L;
        final GroupSeverityUpdater updater = new GroupSeverityUpdater(memberCache,
                groupSeverityCalculator, groupStore);
        LongOpenHashSet groupIds = new LongOpenHashSet();
        groupIds.addAll(Arrays.asList(group1Uuid, group2Uuid));
        when(memberCache.getCachedGroupIds()).thenReturn(groupIds);
        when(memberCache.getGroupMembers(groupStore, Collections.singleton(group1Uuid), true))
                .thenReturn(Sets.newHashSet(entity1Uuid));
        when(memberCache.getGroupMembers(groupStore, Collections.singleton(group2Uuid), true))
                .thenReturn(Sets.newHashSet(entity2Uuid));
        when(groupSeverityCalculator.calculateSeverity(any()))
                .thenReturn(Severity.MAJOR);
        when(groupStore.updateBulkGroupsSeverity(any())).thenReturn(2);
        // WHEN
        updater.refreshGroupSeverities();
        // THEN
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        verify(groupStore, times(1)).updateBulkGroupsSeverity(captor.capture());
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
}
