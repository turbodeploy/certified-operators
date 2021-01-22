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
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;

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
    private final DbEndpoint ingesterEndpoint;
    private final WriterConfig writerConfig;
    private final ExecutorService pool;
    private final DataProvider dataProvider;
    private final ExtractorKafkaSender extractorKafkaSender;
    private final DataExtractionFactory dataExtractionFactory;
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

    /**
     * Constructor.
     *
     * @param clock clock
     * @param actionConverter action converter
     * @param ingesterEndpoint db endpoint
     * @param actionWritingIntervalMillis interval for writing reporting actions
     * @param writerConfig writer config
     * @param pool thread pool
     * @param dataProvider providing cached topology info
     * @param extractorKafkaSender for sending actions to kafka
     * @param dataExtractionFactory factory for creating different extractors
     * @param actionExtractionIntervalMillis interval for extracting actions
     */
    public ActionWriterFactory(Clock clock,
                               ActionConverter actionConverter,
                               DbEndpoint ingesterEndpoint,
                               long actionWritingIntervalMillis,
                               WriterConfig writerConfig,
                               ExecutorService pool,
                               DataProvider dataProvider,
                               ExtractorKafkaSender extractorKafkaSender,
                               DataExtractionFactory dataExtractionFactory,
                               long actionExtractionIntervalMillis) {
        this.clock = clock;
        this.actionConverter = actionConverter;
        this.ingesterEndpoint = ingesterEndpoint;
        this.writerConfig = writerConfig;
        this.pool = pool;
        this.dataProvider = dataProvider;
        this.extractorKafkaSender = extractorKafkaSender;
        this.dataExtractionFactory = dataExtractionFactory;
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
            return Optional.of(new ReportPendingActionWriter(clock, pool, ingesterEndpoint,
                    writerConfig, actionConverter, actionWritingIntervalMillis,
                    actionPlanType, lastActionWrite));
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
        return Optional.of(new SearchPendingActionWriter(dataProvider, ingesterEndpoint, writerConfig, pool));
    }

    /**
     * Get an action writer for data extraction, if writing interval is satisfied.
     *
     * @return DataExtractionActionWriter or null if writing interval is not satisfied
     */
    public Optional<DataExtractionPendingActionWriter> getDataExtractionActionWriter() {
        if (dataProvider.getTopologyGraph() == null) {
            logger.warn("Topology graph is not ready, skipping action extraction for this cycle");
            return Optional.empty();
        }
        final long now = clock.millis();
        final long nextExtractionTime = lastActionExtraction.longValue() + actionExtractionIntervalMillis;
        if (nextExtractionTime <= now) {
            return Optional.of(new DataExtractionPendingActionWriter(extractorKafkaSender,
                    dataExtractionFactory, dataProvider, clock, lastActionExtraction));
        } else {
            logger.info("Not extracting actions for another {} minutes.",
                    TimeUnit.MILLISECONDS.toMinutes(nextExtractionTime - now));
            return Optional.empty();
        }
    }
}
