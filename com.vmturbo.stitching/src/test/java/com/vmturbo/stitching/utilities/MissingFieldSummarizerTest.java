package com.vmturbo.stitching.utilities;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Tests class for missing fields summaries.
 */
public class MissingFieldSummarizerTest {

    private static final List<Triplet<EntityType, String, String>> toSummarize = Arrays.asList(
            new Triplet<>(EntityType.VIRTUAL_MACHINE, "ipAddress", "id::1"),
            new Triplet<>(EntityType.VIRTUAL_MACHINE, "ipAddress", "id::2"),
            new Triplet<>(EntityType.VIRTUAL_MACHINE, "ipAddress", "id::3"),
            new Triplet<>(EntityType.VIRTUAL_MACHINE, "ipAddress", "id::4"),
            new Triplet<>(EntityType.STORAGE, "storageId", "id::5"),
            new Triplet<>(EntityType.STORAGE, "storageId", "id::6"),
            new Triplet<>(EntityType.STORAGE, "storageId", "id::7"),
            new Triplet<>(EntityType.PHYSICAL_MACHINE, "numCpuCores", "id::8"),
            new Triplet<>(EntityType.PHYSICAL_MACHINE, "numCpuCores", "id::9"));

    private static final List<Pair<EntityType, String>> emptyList = Collections.emptyList();

    private MissingFieldSummarizer summarizer;

    @Mock
    private Logger logger;

    /**
     * Initialize mocks and summarizer.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        logger = mock(Logger.class);
        // Mock the target log line.
        // We have to use Object.class to disambiguate between Supplier and Throwable
        doNothing().when(logger).warn(anyString(), any(Long.class));
        // Mock the summary lines.
        doNothing().when(logger).warn(anyString(), anyObject(), anyObject(), anyObject());

        summarizer = MissingFieldSummarizer.getInstance(logger);
        summarizer.clear();
    }

    /**
     * Test that a summary is producing the correct results.
     */
    @Test
    public void testNormalSummary() {
        summarizer.setTarget(123456789L);

        unreachableCodeThatNeedsToSummarize(toSummarize);
        summarizer.dump();

        verify(logger, times(1)).warn(anyString(), any(Long.class));
        verify(logger, times(3)).warn(anyString(), anyObject(), anyObject(), anyObject());
    }

    /**
     * Test that an empty summary produces no output.
     */
    @Test
    public void testEmptySummary() {
        summarizer.setTarget(123456789L);

        unreachableCodeThatNeedsToSummarize(toSummarize);
        summarizer.dump();

        verify(logger, times(0)).warn(anyString(), any(Long.class));
    }

    private void unreachableCodeThatNeedsToSummarize(List<Triplet<EntityType, String, String>> data) {
        MissingFieldSummarizer summarizer = MissingFieldSummarizer.getInstance();
        data.forEach(t -> summarizer.append(t.getValue0(), t.getValue1(), t.getValue2()));
    }
}