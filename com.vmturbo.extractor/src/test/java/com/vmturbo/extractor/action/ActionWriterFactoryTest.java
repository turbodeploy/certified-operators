package com.vmturbo.extractor.action;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test for {@link ActionWriterFactory}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class ActionWriterFactoryTest {

    private static final Logger logger = LogManager.getLogger();

    private static final MultiStageTimer timer = new MultiStageTimer(logger);

    private static final long ACTION_WRITING_INTERVAL_MS = 10_000;

    private static final long ACTION_EXTRACTION_INTERVAL_MS = 60_000;

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Autowired
    private ExtractorDbConfig dbConfig;

    private WriterConfig writerConfig = ImmutableWriterConfig.builder()
            .lastSeenUpdateIntervalMinutes(1)
            .lastSeenAdditionalFuzzMinutes(1)
            .insertTimeoutSeconds(10)
            .populateScopeTable(true)
            .build();

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    private ExecutorService executorService = mock(ExecutorService.class);

    private ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);

    private DataExtractionFactory dataExtractionFactory = mock(DataExtractionFactory.class);

    ActionWriterFactory actionWriterFactory;

    /**
     * Set up before each test.
     *
     * @throws Exception any error
     */
    @Before
    public void setUp() throws Exception {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        doReturn(mock(TopologyGraph.class)).when(dataProvider).getTopologyGraph();
        actionWriterFactory = new ActionWriterFactory(
                clock, actionConverter, endpoint, ACTION_WRITING_INTERVAL_MS,
                writerConfig, executorService, dataProvider, extractorKafkaSender,
                dataExtractionFactory, ACTION_EXTRACTION_INTERVAL_MS);
    }

    /**
     * Test that the reporting action writing interval is respected per context.
     *
     * @throws UnsupportedDialectException if the type of endpoint is unsupported.
     * @throws InterruptedException if thread has been interrupted
     * @throws SQLException if DB exception is thrown
     */
    @Test
    public void testSkipUpdateForReporting() throws UnsupportedDialectException,
            InterruptedException, SQLException {
        // first write
        Optional<ReportPendingActionWriter> writer = actionWriterFactory.getReportPendingActionWriter(TypeInfoCase.MARKET);
        assertThat(writer.isPresent(), is(true));
        writer.get().write(timer);

        writer = actionWriterFactory.getReportPendingActionWriter(TypeInfoCase.MARKET);
        assertThat(writer.isPresent(), is(false));

        // A buy RI plan should still get processed.
        writer = actionWriterFactory.getReportPendingActionWriter(TypeInfoCase.BUY_RI);
        assertThat(writer.isPresent(), is(true));
        writer.get().write(timer);

        // still within interval
        clock.addTime(ACTION_WRITING_INTERVAL_MS - 1, ChronoUnit.MILLIS);
        writer = actionWriterFactory.getReportPendingActionWriter(TypeInfoCase.MARKET);
        assertThat(writer.isPresent(), is(false));

        // interval satisfied, it should return a new instance
        clock.addTime(1, ChronoUnit.MILLIS);
        writer = actionWriterFactory.getReportPendingActionWriter(TypeInfoCase.MARKET);
        assertThat(writer.isPresent(), is(true));
    }

    /**
     * Test that the action extraction interval is respected per context.
     */
    @Test
    public void testSkipUpdateForActionExtraction() {
        // first write
        Optional<DataExtractionPendingActionWriter> writer = actionWriterFactory.getDataExtractionActionWriter();
        assertThat(writer.isPresent(), is(true));
        writer.get().write(timer);

        // still within interval
        clock.addTime(ACTION_EXTRACTION_INTERVAL_MS - 1, ChronoUnit.MILLIS);
        writer = actionWriterFactory.getDataExtractionActionWriter();
        assertThat(writer.isPresent(), is(false));

        // interval satisfied, it should return a new instance
        clock.addTime(1, ChronoUnit.MILLIS);
        writer = actionWriterFactory.getDataExtractionActionWriter();
        assertThat(writer.isPresent(), is(true));
    }
}