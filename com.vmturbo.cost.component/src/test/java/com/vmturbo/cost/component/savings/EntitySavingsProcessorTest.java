package com.vmturbo.cost.component.savings;

import org.junit.Test;
import org.mockito.Mockito;

public class EntitySavingsProcessorTest {

    private TopologyEventsPoller topologyEventsPoller = Mockito.mock(TopologyEventsPoller.class);

    private EntitySavingsTracker entitySavingsTracker = Mockito.mock(EntitySavingsTracker.class);

    private RollupSavingsProcessor rollupSavingsProcessor = Mockito.mock(RollupSavingsProcessor.class);

    private EntitySavingsProcessor entitySavingsProcessor = new EntitySavingsProcessor(
            entitySavingsTracker, topologyEventsPoller, rollupSavingsProcessor);

    @Test
    public void testExecute() {
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller).poll();
        Mockito.verify(entitySavingsTracker).processEvents();
    }
}
