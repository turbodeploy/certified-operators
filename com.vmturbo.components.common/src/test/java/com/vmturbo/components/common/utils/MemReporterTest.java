package com.vmturbo.components.common.utils;

import static com.vmturbo.components.common.utils.MemReporter.defaultItemCount;
import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.INFO;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory2;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.hamcrest.text.MatchesPattern;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.components.common.utils.MemReporter.MemReport.MemReportItem;
import com.vmturbo.components.common.utils.MemReporter.SimpleMemReporter;

/**
 * Tests of {@link MemReporter} class.
 */
public class MemReporterTest {

    private final Logger logger = mock(Logger.class);
    private final List<String> logLines = new ArrayList<>();
    private final MessageFactory2 msgFactory = new ParameterizedMessageFactory();

    /**
     * Set up to capture logging data, for tests of memory report logging.
     */
    @Before
    public void before() {
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            final Message logLine = msgFactory.newMessage((String)args[1],
                    args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
            logLines.add(logLine.getFormattedMessage());
            return null;
        }).when(logger).log(any(Level.class), anyString(),
                any(), any(), any(), any(), any(), any(), any());
        doReturn(true).when(logger).isEnabled(Level.INFO);
        doReturn(false).when(logger).isEnabled(Level.DEBUG);
    }

    /**
     * Test that the default item counter works correctly.
     */
    @Test
    public void testDefaultItemCount() {
        assertThat(defaultItemCount(this), nullValue());
        assertThat(defaultItemCount(new int[10]), is(10));
        assertThat(defaultItemCount(new Integer[10]), is(10));
        assertThat(defaultItemCount(Collections.emptyList()), is(0));
        assertThat(defaultItemCount(Arrays.asList(1, 2, 3)), is(3));
        assertThat(defaultItemCount(Collections.<String, String>emptyMap()), is(0));
        assertThat(defaultItemCount(ImmutableMap.of("a", 1, "b", 2)), is(2));
        assertThat(defaultItemCount(new LongDataPack()), is(0));
        IDataPack<String> pack = new DataPack<>();
        pack.toIndex("abc");
        pack.toIndex("def");
        assertThat(defaultItemCount(pack), is(2));
    }

    /**
     * Test the default method implementations of {@link MemReporter}.
     */
    @Test
    public void testDefaultMethods() {
        final MemReporter mr = new MemReporter() {
        };
        assertThat(mr.getMemDescription(), is("MemReporterTest$1"));
        assertThat(new MR().getMemDescription(), is("MemReporterTest.MR"));
        assertThat(mr.getMemExclusions(), empty());
        assertThat(mr.getMemItemCount(), nullValue());
        assertThat(mr.getMemSize(), is(sizeOf(mr)));
        assertThat(mr.getNestedMemReporters(), empty());
    }

    /**
     * Test that exclusions work as expected.
     *
     * @throws NoSuchFieldException   if we have a problem with field reflection
     * @throws IllegalAccessException if we have a probelm with field reflection
     */
    @Test
    public void testExclusions() throws NoSuchFieldException, IllegalAccessException {
        final MemReporter mr = new MemReporter() {
            public final Integer a = 1;

            @Override
            public Collection<Object> getMemExclusions() {
                return Collections.singletonList(a);
            }
        };
        final long excludedSize = sizeOf(mr.getClass().getField("a").get(mr));
        assertThat(excludedSize, not(0));
        assertThat(mr.getMemSize(), is(sizeOf(mr) - excludedSize));
    }

    /**
     * Test that that sizes are correct when no exclusions are in place.
     */
    @Test
    public void testNoExclusions() {
        final MemReporter mr = new MemReporter() {
            public final Integer a = 1;
        };
        assertThat(mr.getMemSize(), is(sizeOf(mr)));
    }

    /**
     * Test that {@link SimpleMemReporter} class works.
     */
    @Test
    public void testSimpleMemoryReporter() {
        Integer i = 1;
        Integer[] a = new Integer[]{i};
        final SimpleMemReporter smr = new SimpleMemReporter("a", a);
        assertThat(smr.getMemDescription(), is("a"));
        assertThat(smr.getMemItemCount(), is(1));
        assertThat(smr.getMemSize(), is(sizeOf(a)));
        assertThat(smr.getNestedMemReporters(), empty());
        final SimpleMemReporter smrx = new SimpleMemReporter("a", a, Collections.singletonList(i));
        assertThat(smrx.getMemSize(), is(sizeOf(a) - sizeOf(i)));
    }

    /**
     * Test methods of {@link MemReportItem}.
     */
    @Test
    public void testMemReportItem() {
        SimpleMemReporter smr = new SimpleMemReporter("mem-reporter", 1);
        MemReportItem item = new MemReportItem(smr, 0);
        int line = 0;
        item.logTo(logger, INFO, null);
        assertThat(logLines.get(line++), matchesPattern(
                "mem-reporter Memory Report: \\d+ bytes\\(\\d+b\\)"));
        item.logTo(logger, INFO, "tag");
        assertThat(logLines.get(line++), matchesPattern(
                "mem-reporter\\[tag\\] Memory Report: \\d+ bytes\\(\\d+b\\)"));
        item = new MemReportItem(smr, 1);
        item.logTo(logger, INFO, null);
        assertThat(logLines.get(line++), matchesPattern(
                "  mem-reporter: \\d+ bytes\\(\\d+b\\)"));
        smr = new SimpleMemReporter("array", new Integer[10]);
        item = new MemReportItem(smr, 0);
        item.logTo(logger, INFO, null);
        assertThat(logLines.get(line++), matchesPattern(
                "array Memory Report: \\d+ bytes\\(\\d+b\\)\\[#10\\]"));
        // test report item when the reporter does not want to report a size
        MemReporter mr = new MemReporter() {
            @Override
            public Long getMemSize() {
                return null;
            }
        };
        item = new MemReportItem(mr, 0);
        item.logTo(logger, INFO, null);
        assertThat(logLines.get(line++), is("MemReporterTest$4 Memory Report: "));
    }

    /**
     * Test that {@link MemReportItem} works properly when its reporter throws exceptions when asked
     * for size or item count.
     */
    @Test
    public void testMemReportItemWithBrokenReporter() {
        final BrokenMR mr = new BrokenMR();
        final MemReportItem item = new MemReportItem(mr, 0);
        item.logTo(logger, INFO, null);
        assertThat(logLines.get(0), is("MemReporterTest.BrokenMR Memory Report: unknown size"));
    }

    /**
     * Test that generating a logged report creates the expected log lines.
     *
     * <p>Sizes are pattern-matched, since they may be affected by JVM platform.</p>
     */
    @Test
    public void testLoggedReport() {
        final MR mr = new MR();
        int line = 0;
        MemReporter.report(mr).logTo(logger, INFO, "tag");
        assertThat(logLines.get(line++), MatchesPattern.matchesPattern(
                "MemReporterTest.MR\\[tag\\] Memory Report: \\d+ bytes\\(\\d+b\\)"));
        assertThat(logLines.get(line++), matchesPattern("  ints: 0 bytes\\(0b\\)"));
        // alternative API to directly report to logs
        MemReporter.logReport(mr, logger, INFO, "tag");
        assertThat(logLines.get(line++), MatchesPattern.matchesPattern(
                "MemReporterTest.MR\\[tag\\] Memory Report: \\d+ bytes\\(\\d+b\\)"));
        assertThat(logLines.get(line++), matchesPattern("  ints: 0 bytes\\(0b\\)"));
        // alternative API with no tag
        MemReporter.logReport(mr, logger, INFO);
        assertThat(logLines.get(line++), MatchesPattern.matchesPattern(
                "MemReporterTest.MR Memory Report: \\d+ bytes\\(\\d+b\\)"));
        assertThat(logLines.get(line++), matchesPattern("  ints: 0 bytes\\(0b\\)"));
        mr.setInts(new int[]{1, 2, 3, 4, 5});
        MemReporter.report(mr).logTo(logger, INFO, "tag");
        assertThat(logLines.get(line++), MatchesPattern.matchesPattern(
                "MemReporterTest.MR\\[tag\\] Memory Report: \\d+ bytes\\(\\d+b\\)"));
        assertThat(logLines.get(line++), matchesPattern("  ints: \\d+ bytes\\(\\d+b\\)\\[#5\\]"));
        MemReporter.logReport(mr, logger, INFO, "tag");
        assertThat(logLines.get(line++), MatchesPattern.matchesPattern(
                "MemReporterTest.MR\\[tag\\] Memory Report: \\d+ bytes\\(\\d+b\\)"));
        assertThat(logLines.get(line++), matchesPattern("  ints: \\d+ bytes\\(\\d+b\\)\\[#5\\]"));
        // if log level is disabled nothing should be reported
        MemReporter.logReport(mr, logger, DEBUG, "tag");
        assertThat(logLines.size(), is(line));
    }

    private static long sizeOf(Object o) {
        return o == null ? 0 : MemoryMeasurer.measure(o).getTotalSizeBytes();
    }

    /**
     * Simple {@link MemReporter} used by soe tests.
     */
    public static class MR implements MemReporter {
        int[] ints;

        public int[] getInts() {
            return ints;
        }

        public void setInts(final int[] ints) {
            this.ints = ints;
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return Collections.singletonList(new SimpleMemReporter("ints", ints));
        }
    }

    /**
     * A {@link MemReporter} that throws exceptions when asked for size or item count.
     */
    public static class BrokenMR implements MemReporter {
        @Override
        public Long getMemSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer getMemItemCount() {
            throw new UnsupportedOperationException();
        }
    }
}
