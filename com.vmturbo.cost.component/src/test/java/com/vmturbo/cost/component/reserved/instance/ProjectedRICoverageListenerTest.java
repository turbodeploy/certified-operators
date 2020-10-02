package com.vmturbo.cost.component.reserved.instance;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.notification.CostNotificationSender;

/**
 * A class to test {@link ProjectedRICoverageListener}.
 */
public class ProjectedRICoverageListenerTest {
    private static final long PROJECTED_TOPO_ID = 101;
    private static final long PLAN_ID = 102;
    private static final int ORIGINAL_TOPO_INFO_ID = 1;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyId(PROJECTED_TOPO_ID - 1)
            .build();

    private ProjectedRICoverageAndUtilStore projectedRICoverageStore
            = mock(ProjectedRICoverageAndUtilStore.class);

    private PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore =
            mock(PlanProjectedRICoverageAndUtilStore.class);

    private CostNotificationSender costNotificationSender =
            mock(CostNotificationSender.class);

    private static final long realtimeTopologyContextId = 1234;

    /**
     * Verifies that both a projected topology broadcast and a projected RI coverage notification
     * have been received sending the status notification corresponding to RI coverage.
     */
    @Test
    public void testRiCoverageNotificationNotSentUntilProjectedTopologyReceived() {
        final ProjectedRICoverageListener listener = Mockito.spy(new ProjectedRICoverageListener(
                projectedRICoverageStore,
                planProjectedRICoverageAndUtilStore,
                costNotificationSender,
                realtimeTopologyContextId
        ));

        when(planProjectedRICoverageAndUtilStore.projectedTopologyAvailableHandler(PROJECTED_TOPO_ID, PLAN_ID))
                .thenReturn(null);

        final TopologyInfo originalTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(ORIGINAL_TOPO_INFO_ID)
                .setTopologyType(TopologyType.PLAN)
                .build();

        final TestIterator iterator = new TestIterator();

        Mockito.doNothing().when(planProjectedRICoverageAndUtilStore)
                .updateProjectedEntityToRIMappingTableForPlan(originalTopologyInfo, Lists.newArrayList());
        when(planProjectedRICoverageAndUtilStore.updateProjectedRICoverageTableForPlan(PROJECTED_TOPO_ID, originalTopologyInfo, Lists.newArrayList()))
            .thenReturn(null);

        when(projectedRICoverageStore.resolveBuyRIsInScope(ORIGINAL_TOPO_INFO_ID)).thenReturn(
                Lists.newArrayList());
        Mockito.doNothing().when(planProjectedRICoverageAndUtilStore)
                .updateProjectedRIUtilTableForPlan(originalTopologyInfo, Lists.newArrayList(), Lists.newArrayList());

        listener.onProjectedEntityRiCoverageReceived(
                PROJECTED_TOPO_ID,
                originalTopologyInfo,
                iterator);

        // Verify the ProjectedRiCoverageNotification was NOT sent before the projected
        // topology is available
        verify(listener, never()).sendProjectedRiCoverageNotification(Mockito.any());


        listener.onProjectedTopologyAvailable(PROJECTED_TOPO_ID, PLAN_ID);

        // Verify the ProjectedRiCoverageNotification was sent after the projected
        // topology becomes available
        verify(listener).sendProjectedRiCoverageNotification(Mockito.any());
    }


    /**
     * Test iterator, with a utility method to check if it's been drained.
     */
    private static class TestIterator implements RemoteIterator<EntityReservedInstanceCoverage> {
        private final Iterator<EntityReservedInstanceCoverage> it = Collections.singletonList(EntityReservedInstanceCoverage.getDefaultInstance())
                .iterator();

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Nonnull
        @Override
        public Collection<EntityReservedInstanceCoverage> nextChunk() {
            return Collections.singletonList(it.next());
        }
    }
}
