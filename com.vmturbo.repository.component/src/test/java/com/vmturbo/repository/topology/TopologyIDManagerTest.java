package com.vmturbo.repository.topology;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;

/**
 * Unit tests for {@link TopologyIDManager}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyIDManagerTest {

    private TopologyIDManager topologyIDManagerTest;

    @Before
    public void setup() throws GraphDatabaseException {
        topologyIDManagerTest = new TopologyIDManager();
    }

    @Test
    public void testDatabaseName() {
        final long contextId = 100L;
        final long topoId = 200L;
        final TopologyID tid = new TopologyID(contextId, topoId, TopologyType.SOURCE);
        final String dbName = topologyIDManagerTest.databaseName(tid);

        assertThat(dbName).isEqualTo(dbName(contextId, topoId));
    }

    @Test
    public void testCurrentRealtimeDatabase() {
        final long contextId = 100L;
        final long topoId = 200L;
        final TopologyID tid = new TopologyID(contextId, topoId, TopologyType.SOURCE);
        // This will up the mapping from the contextId to tid, which is required in databaseOf method.
        topologyIDManagerTest.setCurrentRealTimeTopologyId(tid);
        assertThat(topologyIDManagerTest.getCurrentRealTimeTopologyId().get()).isSameAs(tid);

        final TopologyDatabase topoDb = topologyIDManagerTest.currentRealTimeDatabase().get();

        assertThat(TopologyDatabases.getDbName(topoDb)).isEqualTo(dbName(contextId, topoId));
    }

    @Test
    public void testDatabaseOf() {
        final long contextId = 100L;
        final long topoId = 200L;
        final TopologyID tid = new TopologyID(contextId, topoId, TopologyType.SOURCE);
        // This will up the mapping from the contextId to tid, which is required in databaseOf method.
        topologyIDManagerTest.register(tid);
        topologyIDManagerTest.setCurrentRealTimeTopologyId(tid);

        final TopologyDatabase topoDb = topologyIDManagerTest.databaseOf(Long.toString(contextId));

        assertThat(TopologyDatabases.getDbName(topoDb)).isEqualTo(dbName(contextId, topoId));
    }

    @Test(expected = RuntimeException.class)
    public void testDatabaseOfException() {
        final long contextId = 100L;
        final long topoId = 200L;
        final TopologyID tid = new TopologyID(contextId, topoId, TopologyType.SOURCE);
        topologyIDManagerTest.register(tid);
        topologyIDManagerTest.setCurrentRealTimeTopologyId(tid);
        // will throw because contextId + 1 is not registered
        topologyIDManagerTest.databaseOf(Long.toString(contextId + 1));
    }

    @Test
    public void testTopologyID() {
        TopologyID tid = new TopologyID(111L, 222L, TopologyType.SOURCE);
        Assert.assertNotEquals(tid, null);
        Assert.assertNotEquals(tid, 1);
        Assert.assertEquals(tid, tid);

        TopologyID tid2 = new TopologyID(tid.getContextId(), tid.getTopologyId(), tid.getType());
        Assert.assertEquals(tid, tid2);
        Assert.assertEquals(tid.hashCode(), tid2.hashCode());

        tid2 = new TopologyID(tid.getContextId() + 1, tid.getTopologyId(), tid.getType());
        Assert.assertNotEquals(tid,  tid2);

        tid2 = new TopologyID(tid.getContextId(), tid.getTopologyId() + 1, tid.getType());
        Assert.assertNotEquals(tid,  tid2);

        tid2 = new TopologyID(tid.getContextId(), tid.getTopologyId(), TopologyType.PROJECTED);
        Assert.assertNotEquals(tid,  tid2);
    }

    @Test
    public void testTopologyIDRegsiterDeregister() {
        TopologyID tid = new TopologyID(111L, 222L, TopologyType.SOURCE);
        Assert.assertTrue(topologyIDManagerTest.register(tid));
        Assert.assertFalse(topologyIDManagerTest.register(tid));
        Assert.assertTrue(topologyIDManagerTest.deRegister(tid));
        Assert.assertFalse(topologyIDManagerTest.deRegister(tid));
    }

    private static String dbName(long cid, long tid) {
        return "topology-" + cid + "-" + tid;
    }
}
