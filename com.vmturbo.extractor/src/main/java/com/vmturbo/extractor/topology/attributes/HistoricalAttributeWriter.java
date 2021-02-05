package com.vmturbo.extractor.topology.attributes;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ITopologyWriter;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.extractor.topology.attributes.HistoricalAttributeWriterFactory.SinkFactory;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer responsible for recording historical attributes of entities in a particular topology.
 *
 * <p/>A new instance of this writer is created for every topology.
 */
public class HistoricalAttributeWriter implements ITopologyWriter {
    private static final Logger logger = LogManager.getLogger();

    private final List<HistoricalAttributeProcessor<?>> attributeProcessors;

    /**
     * The records to write to the database at the end of the topology processing.
     */
    private final List<Record> records = new ArrayList<>();

    private final DbEndpoint dbEndpoint;

    private final SinkFactory sinkFactory;

    /**
     * Initialized in {@link HistoricalAttributeWriter#startTopology(TopologyInfo, WriterConfig, MultiStageTimer)}.
     */
    private WriterConfig writerConfig;

    HistoricalAttributeWriter(@Nonnull final List<HistoricalAttributeProcessor<?>> attributeProcessors,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final SinkFactory sinkFactory) {
        this.attributeProcessors = attributeProcessors;
        this.dbEndpoint = dbEndpoint;
        this.sinkFactory = sinkFactory;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(TopologyInfo topologyInfo,
            WriterConfig writerConfig, MultiStageTimer timer) {
        this.writerConfig = writerConfig;
        return entity -> {
            attributeProcessors.forEach(processor -> {
                Record record = processor.processEntity(entity, topologyInfo);
                if (record != null) {
                    // TODO: We can put these in a queue and write asynchronously in batches as we
                    // are processing the topology.
                    records.add(record);
                }
            });
        };
    }

    @Override
    public int finish(DataProvider dataProvider)  throws InterruptedException, UnsupportedDialectException, SQLException {
        Metrics.totalRecords(records.size());
        logger.debug("Attempting to write {} metrics to the historical attributes database.", records.size());
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter upserter = HistoricalAttributes.TABLE.open(
                     sinkFactory.newSink(dsl, writerConfig),
                     "Historical Attribute Upserter", logger)) {
            records.forEach(record -> {
                try (Record r = upserter.open(record)) {
                    // Nothing to change in the record.
                    // The record gets flushed to the database on close() after we get out of
                    // the try block.
                }
            });

            attributeProcessors.forEach(HistoricalAttributeProcessor::onSuccess);

            Metrics.writtenRecords(upserter.getRecordsWritten());
            return (int)upserter.getRecordsWritten();
        }
    }

    /**
     * Track metrics associated with thew riter.
     */
    private static class Metrics {
        private static final String TOTAL_ATTRS_LABEL = "total";

        private static final String WRITTEN_ATTRS_LABEL = "written";

        private static final DataMetricCounter ATTRIBUTE_COUNTER = DataMetricCounter.builder()
                .withName("xtr_historical_attribute_record_cnt")
                .withLabelNames("type")
                .withHelp("Number of historical attributes recorded since the start of the extractor.")
                .build()
                .register();

        static void totalRecords(int records) {
            Metrics.ATTRIBUTE_COUNTER.labels(Metrics.TOTAL_ATTRS_LABEL).increment((double)records);
        }

        static void writtenRecords(long records) {
            Metrics.ATTRIBUTE_COUNTER.labels(Metrics.WRITTEN_ATTRS_LABEL).increment((double)records);
        }
    }
}
