package com.vmturbo.history.stats;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.ingesters.plan.writers.PlanStatsWriter;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;

/**
 * Test {@link PlanStatsWriter}
 **/
public class PlanStatsWriterTest {

    /**
     * Verify when numberOfEntities or numOriginalPriceIndex is 0, we still persist them to DB
     */
    @Ignore
    @Test
    public void testCounts() throws InterruptedException, TimeoutException, CommunicationException, VmtDbException {
        // TODO unify: revive this test
        HistorydbIO historydbIO = mock(HistorydbIO.class);
//        PlanStatsWriter planStatsWriter = new PlanStatsWriter(historydbIO, null);
        ScenariosRecord scenariosRecord = new ScenariosRecord();
        when(historydbIO.getOrAddScenariosRecord(any())).thenReturn(scenariosRecord);
        RemoteIterator<ProjectedTopologyEntity> iterator
            = Mockito.mock(RemoteIterator.class);
        when(iterator.hasNext()).thenReturn(false);
//        planStatsWriter.processProjectedChunks(TopologyInfo.newBuilder().build(), iterator);
        verify(historydbIO, atLeastOnce()).clipValue(anyDouble());
    }


}
