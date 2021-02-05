package com.vmturbo.extractor.topology.attributes;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.topology.ITopologyWriter.TopologyWriterFactory;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.extractor.topology.attributes.EnumOidRetriever.PostgresEnumOidRetriever;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Factory class for the {@link HistoricalAttributeWriter}.
 *
 * <p/>The {@link HistoricalAttributeWriterFactory} and the
 * {@link HistoricalAttributeProcessorFactory} objects it contains live across broadcasts and
 * maintain any inter-broadcast state.
 */
public class HistoricalAttributeWriterFactory implements TopologyWriterFactory<HistoricalAttributeWriter> {
    private static final Logger logger = LogManager.getLogger();

    private final DbEndpoint dbEndpoint;

    private final SinkFactory sinkFactory;

    private final List<HistoricalAttributeProcessorFactory<?>> attributeProcessorFactories;

    /**
     * Create a new instance of the factory.
     *
     * @param dbEndpoint Endpoint to connect to the database.
     * @param dbWriterPool Threadpool to use for data insertion.
     * @param clock Clock to use to tell the time :)
     * @param forceUpdateInterval Interval at which we should force writing attributes to the
     *                            database.
     * @param forceUpdateIntervalUnits The units for the interval.
     */
    public HistoricalAttributeWriterFactory(DbEndpoint dbEndpoint,
            @Nonnull final ExecutorService dbWriterPool,
            @Nonnull Clock clock,
            long forceUpdateInterval,
            TimeUnit forceUpdateIntervalUnits) {
        this.dbEndpoint = dbEndpoint;
        this.sinkFactory = (dslContext, writerConfig) -> {
            return new DslUpsertRecordSink(dslContext, HistoricalAttributes.TABLE, writerConfig,
                    dbWriterPool, "upsert",
                    Arrays.asList(HistoricalAttributes.TIME, HistoricalAttributes.ENTITY_OID,
                            HistoricalAttributes.TYPE),
                    // We don't expect overlaps.
                    Collections.emptyList());
        };
        this.attributeProcessorFactories = Arrays.asList(
                new EntityStateAttributeProcessorFactory(
                        new PostgresEnumOidRetriever<>(EntityState.class, EntityState.POWERED_ON.getName()),
                        clock, forceUpdateInterval, forceUpdateIntervalUnits),
                new NumCpuAttributeProcessorFactory(clock, forceUpdateInterval, forceUpdateIntervalUnits),
                new VolumeAttachmentAttributeProcessorFactory(clock, forceUpdateInterval, forceUpdateIntervalUnits));
    }

    @Override
    public HistoricalAttributeWriter newInstance() {
        final List<HistoricalAttributeProcessor<?>> processorList =
                attributeProcessorFactories.stream()
                        .map(factory -> factory.newProcessor(dbEndpoint))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return new HistoricalAttributeWriter(processorList, dbEndpoint, sinkFactory);
    }

    /**
     * Helper utility factory to create a sink.
     */
    @FunctionalInterface
    interface SinkFactory {
        DslUpsertRecordSink newSink(DSLContext dsl, WriterConfig writerConfig);
    }
}
