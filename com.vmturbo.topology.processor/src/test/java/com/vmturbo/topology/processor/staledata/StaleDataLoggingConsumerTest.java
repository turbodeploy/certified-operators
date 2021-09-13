package com.vmturbo.topology.processor.staledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Used to demonstrate the logging consumer.
 */
public class StaleDataLoggingConsumerTest {

    private static final String TARGET1_NAME = "target1";
    private static final Long TARGET1_OID = 1L;
    private static final TargetHealthSubCategory TARGET1_SUBCATEGORY =
            TargetHealthSubCategory.DISCOVERY;
    private static final ErrorType TARGET1_ERROR_TYPE = ErrorType.INTERNAL_PROBE_ERROR;

    private static final String TARGET2_NAME = "target2";
    private static final Long TARGET2_OID = 2L;
    private static final TargetHealthSubCategory TARGET2_SUBCATEGORY =
            TargetHealthSubCategory.VALIDATION;
    private static final ErrorType TARGET2_ERROR_TYPE = ErrorType.CONNECTION_TIMEOUT;

    private static final ImmutableMap<Long, TargetHealth> data =
            new ImmutableMap.Builder<Long, TargetHealth>()
                    .put(TARGET1_OID, TargetHealth
                            .newBuilder()
                            .setTargetName(TARGET1_NAME)
                            .setSubcategory(TARGET1_SUBCATEGORY)
                            .setErrorType(TARGET1_ERROR_TYPE)
                            .build())
                    .put(TARGET2_OID, TargetHealth
                            .newBuilder()
                            .setTargetName(TARGET2_NAME)
                            .setSubcategory(TARGET2_SUBCATEGORY)
                            .setErrorType(TARGET2_ERROR_TYPE)
                            .build())
                    .build();

    private static final Level loggingLevel = Level.DEBUG;

    private static Logger logger;

    private static MockedAppender mockedAppender;

    private StaleDataLoggingConsumer staleDataLoggingConsumer;

    /**
     * Creates the {@link MockedAppender} and wires it to the logger for the class we are testing.
     */
    @BeforeClass
    public static void beforeClass() {
        mockedAppender = new MockedAppender();
        mockedAppender.start();
        logger = (Logger)LogManager.getLogger(StaleDataLoggingConsumer.class);
        logger.addAppender(mockedAppender);
        logger.setLevel(loggingLevel);
    }

    /**
     * Remove the appender.
     */
    @AfterClass
    public static void tearDown() {
        logger.removeAppender(mockedAppender);
        mockedAppender.stop();
    }

    /**
     * Setup the lines array and pass the fake logger to the {@link StaleDataLoggingConsumer}.
     */
    @Before
    public void setUp() {
        mockedAppender.clear();
        staleDataLoggingConsumer = new StaleDataLoggingConsumer();
    }

    private void verifyCorrectLogs() {
        List<String> lines = mockedAppender.getLines();
        /* 1 line for beginning of report
           1 line for header
           2 lines of targets
           1 line for verbose header (DEBUG)
           2 lines of targets        (DEBUG)
           1 line for verbose footer (DEBUG)
           1 line for end of report
          --------------------------------------
          5 lines (>=INFO),  9 lines (<=DEBUG)
         */
        if (logger.getLevel().equals(Level.DEBUG)) {
            assertEquals(9, lines.size());
        } else if (logger.getLevel().equals(Level.INFO)) {
            assertEquals(5, lines.size());
        } else {
            throw new RuntimeException("Do not test with level " + loggingLevel.toString()
                    + ". Only use INFO or DEBUG");
        }
        // line 3 contains the first target
        String line3 = lines.get(2);
        assertTrue("should include target name", line3.contains(TARGET1_NAME));
        assertTrue("should contain the oid of the target", line3.contains(TARGET1_OID.toString()));
        assertTrue("should contain the Error Type of the target",
                line3.contains(TARGET1_ERROR_TYPE.toString()));
        assertTrue("should contain the Sub category of the target",
                line3.contains(TARGET1_SUBCATEGORY.toString()));
        // line 4 contains the name of the second target
        String line4 = lines.get(3);
        assertTrue("should include target name", line4.contains(TARGET2_NAME));
        assertTrue("should contain the oid of the target", line4.contains(TARGET2_OID.toString()));
        assertTrue("should contain the Error Type of the target",
                line4.contains(TARGET2_ERROR_TYPE.toString()));
        assertTrue("should contain the Sub category of the target",
                line4.contains(TARGET2_SUBCATEGORY.toString()));
    }

    /**
     * Tests that the logger produces correct reports.
     */
    @Test
    public void shouldProduceCorrectReports() {
        // ACT
        staleDataLoggingConsumer.accept(data);

        // ASSERT
        verifyCorrectLogs();
    }

    /**
     * Tests that the report skips all healthy targets.
     */
    @Test
    public void shouldClearOutHealthyTargets() {
        // ARRANGE
        ImmutableMap<Long, TargetHealth> dataWithHealthy =
                new ImmutableMap.Builder<Long, TargetHealth>()
                        .putAll(data)
                        .put(3L, TargetHealth.newBuilder().setTargetName("target").build())
                        .build();

        // ACT
        staleDataLoggingConsumer.accept(dataWithHealthy);

        // ASSERT
        verifyCorrectLogs();
    }

    /**
     * Tests that if all targets are healthy no report is produced.
     */
    @Test
    public void shouldProduceNoReportIfAllHealthy() {
        staleDataLoggingConsumer.accept(new ImmutableMap.Builder<Long, TargetHealth>()
                .put(1L, TargetHealth.newBuilder().setTargetName("target").build())
                .build());
        assertEquals(0, mockedAppender.getLines().size());
    }

    /**
     * Appender that collects each log line to a list.
     */
    private static class MockedAppender extends AbstractAppender {

        private final List<String> lines = new ArrayList<>();

        protected MockedAppender() {
            super("MockedAppender", null, null);
        }

        public List<String> getLines() {
            return lines;
        }

        public void clear() {
            lines.clear();
        }

        @Override
        public void append(LogEvent logEvent) {
            this.lines.addAll(Arrays.asList(logEvent.getMessage().getFormattedMessage().split("\n")));
        }
    }
}