package com.vmturbo.extractor.export;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Test for {@link DataExtractionWriter}.
 */
public class DataExtractionFactoryTest {

    private static final long ENTITY_EXTRACTION_INTERVAL_MS = 60_000;

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);
    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private final ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);
    private DataExtractionFactory dataExtractionFactory;

    /**
     * Set up before each test.
     *
     * @throws Exception any error
     */
    @Before
    public void setUp() throws Exception {
        dataExtractionFactory = new DataExtractionFactory(dataProvider, targetCache,
                extractorKafkaSender, ENTITY_EXTRACTION_INTERVAL_MS, clock);
    }

    /**
     * Verify that extraction interval is verified.
     */
    @Test
    public void testExtractionIntervalIsRespected() {
        // first write
        DataExtractionWriter writer = dataExtractionFactory.newDataExtractionWriter();
        assertNotNull(writer);
        dataExtractionFactory.updateLastExtractionTime(clock.millis());

        // still within interval
        clock.addTime(ENTITY_EXTRACTION_INTERVAL_MS - 1, ChronoUnit.MILLIS);
        writer = dataExtractionFactory.newDataExtractionWriter();
        assertNull(writer);

        // interval satisfied, it should return a new instance
        clock.addTime(1, ChronoUnit.MILLIS);
        writer = dataExtractionFactory.newDataExtractionWriter();
        assertNotNull(writer);
        dataExtractionFactory.updateLastExtractionTime(clock.millis() + ENTITY_EXTRACTION_INTERVAL_MS);

        // still within interval
        clock.addTime(2, ChronoUnit.MILLIS);
        writer = dataExtractionFactory.newDataExtractionWriter();
        assertNull(writer);
    }
}