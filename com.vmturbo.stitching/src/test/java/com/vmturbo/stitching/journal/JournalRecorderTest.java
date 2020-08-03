package com.vmturbo.stitching.journal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingMetrics;
import com.vmturbo.stitching.journal.JournalRecorder.OutputStreamRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;

public class JournalRecorderTest {

    @Test
    public void testEmptyTextBox() {
        assertEquals(
            "--------------------\n" +
            "|                  |\n" +
            "|                  |\n" +
            "|                  |\n" +
            "--------------------",
            JournalRecorder.textBox("", 20));
    }

    @Test
    public void testOneLineTextBox() {
        assertEquals(
            "----------------------------------------\n" +
            "|                                      |\n" +
            "|             foo bar baz              |\n" +
            "|                                      |\n" +
            "----------------------------------------",
            JournalRecorder.textBox("foo bar      baz", 40));
    }

    @Test
    public void testMultilineTextBox() {
        assertEquals(
            "----------------------------------------\n" +
            "|                                      |\n" +
            "|  This is some text that exceeds the  |\n" +
            "|  number of characters allowed on a   |\n" +
            "|                 line                 |\n" +
            "|                                      |\n" +
            "----------------------------------------",
            JournalRecorder.textBox("This is some text that exceeds the number " +
                "of characters allowed on a line", 40));
    }

    @Test
    public void testVeryLongWordTextBox() {
        final String longWord = StringUtils.repeat('a', 40);

        assertEquals(
            "----------------------------------------\n" +
            "|                                      |\n" +
            "|                 foo                  |\n" +
            "| aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |\n" +
            "|                 bar                  |\n" +
            "| aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |\n" +
            "|                                      |\n" +
            "----------------------------------------",
            JournalRecorder.textBox("foo " + longWord + " bar " + longWord, 40));
    }

    @Test
    public void testTextBoxWithLineBreaks() {
        assertEquals(
                "----------------------------------------\n" +
                "|                                      |\n" +
                "|            Some text and             |\n" +
                "|           some other text            |\n" +
                "|                alone                 |\n" +
                "|                                      |\n" +
                "----------------------------------------",
            JournalRecorder.textBox("Some text and\nsome other text\nalone", 40));
    }

    @Test
    public void testRecordRealtimeTopologyInfo() {
        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);
        final TopologyInfo info = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .setTopologyId(12345L)
            .setTopologyContextId(67890L)
            .build();
        final StitchingMetrics metrics = new StitchingMetrics();
        metrics.add(4, 1, 0, Duration.ofSeconds(3));
        recorder.recordTopologyInfoAndMetrics(info, metrics);

        assertEquals("========================= End of stitching journal for =========================\n" +
                "============================== Topology ID: 12345 ==============================\n" +
                "========================== Topology Context ID: 67890 ==========================\n" +
                "======================== 1/4 changesets shown. Took: 3s ========================\n",
            builder.toString());
    }

    @Test
    public void testRecordPlanTopologyInfo() {
        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);
        final TopologyInfo info = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setTopologyId(12345L)
            .setTopologyContextId(67890L)
            .addAllScopeSeedOids(Arrays.asList(111L, 222L, 333L))
            .build();
        final StitchingMetrics metrics = new StitchingMetrics();
        metrics.add(4, 1, 0, Duration.ofSeconds(3));
        recorder.recordTopologyInfoAndMetrics(info, metrics);

        assertEquals("========================= End of stitching journal for =========================\n" +
                "============================== Topology ID: 12345 ==============================\n" +
                "========================== Topology Context ID: 67890 ==========================\n" +
                "======================== 1/4 changesets shown. Took: 3s ========================\n" +
                "{\n" +
                "  \"topologyId\": \"12345\",\n" +
                "  \"topologyContextId\": \"67890\",\n" +
                "  \"topologyType\": \"PLAN\",\n" +
                "  \"scopeSeedOids\": [\n" +
                "    \"111\",\n" +
                "    \"222\",\n" +
                "    \"333\"\n" +
                "  ]\n" +
                "}\n",
            builder.toString());
    }

    @Test
    public void recordOperationEndWithChanges() {
        final JournalableOperation operation = mock(JournalableOperation.class);
        when(operation.getOperationName()).thenReturn("foo");

        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);

        recorder.recordOperationEnd(operation, Collections.emptyList(), 40, 0, 0, Duration.ofSeconds(1));
        assertEquals("----------------------------------- END: foo -----------------------------------\n" +
                "----------------------- 0/40 changesets shown. Took: 1s ------------------------\n" +
                "\n", builder.toString());
    }

    @Test
    public void recordOperationEndWithDetails() {
        final JournalableOperation operation = mock(JournalableOperation.class);
        when(operation.getOperationName()).thenReturn("foo");

        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);

        recorder.recordOperationEnd(operation, Collections.singleton("bar"), 40, 0, 0, Duration.ofSeconds(1));
        assertEquals("----------------------------------- END: foo -----------------------------------\n" +
            "------------------------------------- bar --------------------------------------\n" +
            "----------------------- 0/40 changesets shown. Took: 1s ------------------------\n" +
            "\n", builder.toString());
    }

    @Test
    public void recordOperationEndWithoutChanges() {
        final JournalableOperation operation = mock(JournalableOperation.class);
        when(operation.getOperationName()).thenReturn("foo");

        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);

        recorder.recordOperationEnd(operation, Collections.emptyList(), 0, 0, 0, Duration.ofSeconds(0));
        assertEquals("----------------------------------- END: foo -----------------------------------\n" +
                "\n",
            builder.toString());
    }

    @Test
    public void recordTargetEntry() {
        final TargetEntry targetEntry = TargetEntry.newBuilder()
            .setTargetId(72434315331516L)
            .setTargetName("hp-dl.345.vmturbo.com")
            .setProbeName("Hyperflex")
            .setEntityCount(123_456)
            .build();

        final StringBuilder builder = new StringBuilder();
        final JournalRecorder recorder = new StringBuilderRecorder(builder);

        recorder.recordTargets(Collections.singletonList(targetEntry));
        assertEquals(
            "+--------------+---------------------+----------+----------+" + System.lineSeparator() +
            "|Target OID    |Target Name          |Type      |EntityCnt |" + System.lineSeparator() +
            "+--------------+---------------------+----------+----------+" + System.lineSeparator() +
            "|72434315331516|hp-dl.345.vmturbo.com|Hyperflex |123,456   |" + System.lineSeparator() +
            "+--------------+---------------------+----------+----------+\n\n",
            builder.toString());
    }

    @Test
    public void testErrorInOutputStreamRecorder() throws IOException {
        final OutputStream os = mock(OutputStream.class);
        final OutputStreamRecorder recorder = new OutputStreamRecorder(os);

        doThrow(new IOException("IO Exception")).when(os).write(Mockito.any());
        recorder.record("foo");
        recorder.record("bar");

        // Even though we call record twice, we should only attempt to write once because
        // we should ignore future attempts to write after an error.
        Mockito.verify(os, Mockito.times(1)).write(Mockito.any());
    }
}