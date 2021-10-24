package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Factory for creating different action writers, while respecting the writing interval.
 */
public class ActionWriterFactory {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The last time we wrote actions for each action plan type. It's necessary to split up by
     * type, because otherwise a BuyRI action plan will prevent a follow-up Market action plan
     * from being processed.
     */
    private final Map<TypeInfoCase, Long> lastActionWrite = new EnumMap<>(TypeInfoCase.class);

    /**
     * Last time when actions are extracted successfully and sent to Kafka.
     */
    private final MutableLong lastActionExtraction = new MutableLong(0);

    private final Clock clock;
    private final ActionConverter actionConverter;
    private final DbEndpoint reportsEndpoint;
    private final WriterConfig writerConfig;
    private final ExecutorService pool;
    private final DataProvider dataProvider;
    private final ExtractorKafkaSender extractorKafkaSender;

    /**
     * The minimum interval for writing action information to the database. We don't write actions
     * every broadcast, because for reporting purposes we don't need action information at 10-minute
     * intervals - especially because actions don't change as frequently as commodities.
     */
    private final long actionWritingIntervalMillis;
    /**
     * The interval for extracting actions and sending to Kafka.
     */
    private final long actionExtractionIntervalMillis;
    private final DbEndpoint searchEndpoint;

    /**
     * Constructor.
     *
     * @param clock                          clock
     * @param actionConverter                action converter
     * @param reportsEndpoint                db endpoint for reporting
     * @param searchEndpoint                 db endpoint for search
     * @param actionWritingIntervalMillis    interval for writing reporting actions
     * @param writerConfig                   writer config
     * @param pool                           thread pool
     * @param dataProvider                   providing cached topology info
     * @param extractorKafkaSender           for sending actions to kafka
     * @param actionExtractionIntervalMillis interval for extracting actions
     */
    public ActionWriterFactory(Clock clock, ActionConverter actionConverter,
            DbEndpoint reportsEndpoint, DbEndpoint searchEndpoint,
            long actionWritingIntervalMillis,
            WriterConfig writerConfig, ExecutorService pool,
            DataProvider dataProvider, ExtractorKafkaSender extractorKafkaSender,
            long actionExtractionIntervalMillis) {
        this.clock = clock;
        this.actionConverter = actionConverter;
        this.reportsEndpoint = reportsEndpoint;
        this.searchEndpoint = searchEndpoint;
        this.writerConfig = writerConfig;
        this.pool = pool;
        this.dataProvider = dataProvider;
        this.extractorKafkaSender = extractorKafkaSender;
        this.actionWritingIntervalMillis = actionWritingIntervalMillis;
        this.actionExtractionIntervalMillis = actionExtractionIntervalMillis;
    }

    /**
     * Create a ReportPendingActionWriter, if writing interval is satisfied.
     *
     * @param actionPlanType is it a market or buyRI plan
     * @return ReportPendingActionWriter or null if writing interval is not satisfied
     */
    public Optional<ReportPendingActionWriter> getReportPendingActionWriter(TypeInfoCase actionPlanType) {
        // check if we need to ingest actions for reporting this time
        long lastWriteForType = lastActionWrite.computeIfAbsent(actionPlanType, k -> 0L);
        final long now = clock.millis();
        final long nextUpdateTime = lastWriteForType + actionWritingIntervalMillis;
        if (nextUpdateTime <= now) {
            TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
            if (topologyGraph != null) {
                return Optional.of(new ReportPendingActionWriter(clock, pool, reportsEndpoint,
                        writerConfig, actionConverter, actionWritingIntervalMillis,
                        actionPlanType, lastActionWrite));
            } else {
                logger.error("Not writing reporting actions because no topology graph "
                    + "is available from ingestion.");
                return Optional.empty();
            }
        } else {
            logger.info("Not writing reporting action metrics for another {} minutes.",
                    TimeUnit.MILLISECONDS.toMinutes(nextUpdateTime - now));
            return Optional.empty();
        }
    }

    /**
     * Creates a SearchPendingActionWriter.
     *
     * @return SearchPendingActionWriter
     */
    public Optional<SearchPendingActionWriter> getSearchPendingActionWriter() {
        if (dataProvider.getTopologyGraph() == null) {
            logger.warn("Topology graph is not ready, skipping writing search actions for this cycle");
            return Optional.empty();
        }
        return Optional.of(new SearchPendingActionWriter(dataProvider, searchEndpoint, writerConfig, pool));
    }

    /**
     * Get an action writer for data extraction, if writing interval is satisfied.
     *
     * @return DataExtractionActionWriter or null if writing interval is not satisfied
     */
    public Optional<DataExtractionPendingActionWriter> getDataExtractionPendingActionWriter() {
        if (dataProvider.getTopologyGraph() == null) {
            logger.warn("Topology graph is not ready, skipping action extraction for this cycle");
            return Optional.empty();
        }
        final long now = clock.millis();
        final long nextExtractionTime = lastActionExtraction.longValue() + actionExtractionIntervalMillis;
        if (nextExtractionTime <= now) {
            return Optional.of(new DataExtractionPendingActionWriter(extractorKafkaSender,
                    clock, lastActionExtraction, actionConverter));
        } else {
            logger.info("Not extracting pending actions for another {} minutes.",
                    TimeUnit.MILLISECONDS.toMinutes(nextExtractionTime - now));
            return Optional.empty();
        }
    }
}
