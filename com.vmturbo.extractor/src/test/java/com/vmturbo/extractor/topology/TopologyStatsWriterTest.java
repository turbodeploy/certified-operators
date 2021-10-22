package com.vmturbo.extractor.topology;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jooq.DSLContext;
import org.jooq.InsertValuesStep2;
import org.jooq.JSONB;
import org.jooq.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test class for TopologyStatsWriter.
 */
public class TopologyStatsWriterTest {

    private TopologyStatsWriter topologyStatsWriter;

    /**
     * Set up the write for tests.
     *
     * @throws Exception any exception
     */
    @Before
    public void before() throws Exception {
        final DSLContext dsl = getMockDsl();
        final DbEndpoint endpoint = mock(DbEndpoint.class);
        doReturn(dsl).when(endpoint).dslContext();

        final ExecutorService pool = Executors.newSingleThreadExecutor();
        this.topologyStatsWriter = spy(new TopologyStatsWriter(endpoint, pool));
    }

    /**
     * Verify the topology attribute.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGetTopologyStats() throws Exception {
        DataProvider dataProvider = mock(DataProvider.class);
        TopologyGraph<SupplyChainEntity> graph = mock(TopologyGraph.class);
        when(dataProvider.getTopologyGraph()).thenReturn(graph);
        when(graph.size()).thenReturn(1000);

        OffsetDateTime time = OffsetDateTime.now();
        final WriterConfig config = mock(WriterConfig.class);
        topologyStatsWriter.startTopology(TopologyInfo.newBuilder()
                .setCreationTime(time.toEpochSecond()).build(), config, null);

        ResultCaptor<String> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(topologyStatsWriter).getTopologyAttrs(graph);

        topologyStatsWriter.finish(dataProvider);

        verify(topologyStatsWriter).getTopologyAttrs(graph);

        String expectedTopologyAttrs = "{\"numberOfEntities\":1000}";
        Assert.assertEquals(expectedTopologyAttrs, resultCaptor.getResult());
    }

    private DSLContext getMockDsl() {
        final DSLContext mock = mock(DSLContext.class);
        final InsertValuesStep2 insertValuesStep2 = mock(InsertValuesStep2.class);
        when(mock.insertInto(any(Table.class), any(), any())).thenReturn(insertValuesStep2);
        when(insertValuesStep2.values(any(OffsetDateTime.class), any(JSONB.class))).thenReturn(insertValuesStep2);
        when(insertValuesStep2.execute()).thenReturn(1);
        return mock;
    }

    /**
     * Class for retrieving return value of a mocked method.
     *
     * @param <T> object type of the return object
     */
    class ResultCaptor<T> implements Answer {
        private T result = null;

        T getResult() {
            return result;
        }

        @Override
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            result = (T)invocationOnMock.callRealMethod();
            return result;
        }
    }
}
