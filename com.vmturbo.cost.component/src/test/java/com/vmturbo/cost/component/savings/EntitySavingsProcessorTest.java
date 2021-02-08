package com.vmturbo.cost.component.savings;

import org.junit.Test;
import org.mockito.Mockito;

public class EntitySavingsProcessorTest {

    private TopologyEventsPoller topologyEventsPoller = Mockito.mock(TopologyEventsPoller.class);

    private EntitySavingsTracker entitySavingsTracker = Mockito.mock(EntitySavingsTracker.class);

    private EntitySavingsProcessor entitySavingsProcessor = new EntitySavingsProcessor(entitySavingsTracker, topologyEventsPoller);

    @Test
    public void testExecute() {
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller).poll();
        Mockito.verify(entitySavingsTracker).processEvents();
    }
}
