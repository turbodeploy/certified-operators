package com.vmturbo.extractor.topology.attributes;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for {@link HistoricalAttributeWriter}.
 */
public class HistoricalAttributeWriterTest {

    private List<Record> upsertCapture;

    private HistoricalAttributeProcessor<Integer> processor = mock(HistoricalAttributeProcessor.class);

    private HistoricalAttributeWriter writer;

    private WriterConfig writerConfig = mock(WriterConfig.class);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .build();

    /**
     * Setup before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        final DbEndpoint endpoint = mock(DbEndpoint.class);
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslUpsertRecordSink upsertSink = mock(DslUpsertRecordSink.class);
        this.upsertCapture = captureSink(upsertSink, false);

        writer = new HistoricalAttributeWriter(Collections.singletonList(processor), endpoint, (x, y) -> upsertSink);
    }

    /**
     * Test that the writer correctly uses the internal {@link HistoricalAttributeProcessor} and
     * saves the mapped {@link Record}s to the sink.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWriter() throws Exception {
        final Consumer<TopologyEntityDTO> consumer =
                writer.startTopology(topologyInfo, writerConfig, new MultiStageTimer(null));
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setDisplayName("foo")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        Record mappedRecord = new Record(HistoricalAttributes.TABLE);
        mappedRecord.set(HistoricalAttributes.INT_VALUE, 100);
        when(processor.processEntity(entity, topologyInfo))
                .thenReturn(mappedRecord);
        consumer.accept(entity);

        writer.finish(mock(DataProvider.class));

        verify(processor).onSuccess();
        assertThat(upsertCapture.size(), is(1));
        assertThat(upsertCapture.get(0).asMap(), is(mappedRecord.asMap()));
    }
}