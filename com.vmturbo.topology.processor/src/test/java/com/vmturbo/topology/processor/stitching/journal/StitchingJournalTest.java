package com.vmturbo.topology.processor.stitching.journal;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.commodityType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.ChangeGrouping;
import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.IStitchingJournal.JournalChangeset;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntity;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.journal.JournalableOperation;
import com.vmturbo.stitching.journal.SemanticDiffer;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * Tests for stitching journal.
 */
public class StitchingJournalTest {

    final JournalRecorder stringRecorder = new StringBuilderRecorder();
    final TopologyEntity entity = TopologyEntityUtils.topologyEntityBuilder(
        11111, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();
    final TopologyEntity otherEntity = TopologyEntityUtils.topologyEntityBuilder(
        22222, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();
    final SemanticDiffer<TopologyEntity> semanticDiffer = new TopologyEntitySemanticDiffer(
        Verbosity.LOCAL_CONTEXT_VERBOSITY);

    private static final JournalOptions COMPLETE_VERBOSITY = JournalOptions.newBuilder()
        .setVerbosity(Verbosity.COMPLETE_VERBOSITY)
        .build();
    private static final JournalOptions LOCAL_CONTEXT_VERBOSITY = JournalOptions.newBuilder()
        .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
        .build();
    private static final JournalOptions PREAMBLE_ONLY_VERBOSITY = JournalOptions.newBuilder()
        .setVerbosity(Verbosity.PREAMBLE_ONLY_VERBOSITY)
        .build();
    private static final JournalOptions CHANGES_ONLY_VERBOSITY = JournalOptions.newBuilder()
        .setVerbosity(Verbosity.CHANGES_ONLY_VERBOSITY)
        .build();

    private static final JournalableOperation prettyOperation = mock(JournalableOperation.class);
    private static final JournalableOperation compactOperation = mock(JournalableOperation.class);
    
    private final JournalFilter filter = new IncludeAllFilter();
    private final Clock clock = mock(Clock.class);

    @Before
    public void setup() {
        entity.getTopologyEntityDtoBuilder()
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(commodityType(CommodityDTO.CommodityType.VSTORAGE, "storage-key"))
                    .setCapacity(1000.0)
                    .setUsed(123.4)
                    .setPeak(400.0))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(commodityType(CommodityDTO.CommodityType.VCPU))
                    .setCapacity(2000.0)
                    .setUsed(567.8)
                    .setPeak(900.0))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(commodityType(CommodityDTO.CommodityType.VMEM))
                    .setCapacity(3000.0)
                    .setUsed(999.9)
                    .setPeak(2000.0));

        when(prettyOperation.getFormatRecommendation()).thenReturn(FormatRecommendation.PRETTY);
        when(compactOperation.getFormatRecommendation()).thenReturn(FormatRecommendation.COMPACT);
        when(clock.millis()).thenReturn(12345L);

        when(prettyOperation.getOperationName()).thenReturn("pretty operation");
        when(compactOperation.getOperationName()).thenReturn("compact operation");
    }

    @Test
    public void testChangesetCreation() throws Exception {
        final AtomicInteger index = new AtomicInteger();
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>();
        journal.recordChangeset("foo", (changeset) -> index.set(changeset.getChangesetIndex()));
        journal.recordChangeset("bar", (changeset) ->
            assertThat(index.get(), is(lessThan(changeset.getChangesetIndex()))));
    }

    /**
     * Should include preamble only
     */
    @Test
    public void testPreambleOnlyVerbosity() throws Exception {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordChangeset("change", this::makeChange);
        assertEquals(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    change                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------\n",
            stringRecorder.toString());
    }

    /**
     * Should only include changes
     */
    @Test
    public void testChangesOnlyVerbosity() throws Exception {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            CHANGES_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordChangeset("change", this::makeChange);
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    change                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------\n"));
        assertThat(stringRecorder.toString(), containsString("VIRTUAL_MACHINE oid-1"));
        assertThat(stringRecorder.toString(), containsString("++\"displayName\": \"changed-name\""));
        assertThat(stringRecorder.toString(), not(containsString("\"entityType\": 10 [[VIRTUAL_MACHINE]],")));
        assertThat(stringRecorder.toString(), not(containsString("... 2 additional fields ...")));
    }

    /**
     * Should omit certain details but include local context
     */
    @Test
    public void testInitialDetailsVerbosity() throws Exception {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            LOCAL_CONTEXT_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordChangeset("change", this::makeChange);
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    change                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------\n"));
        assertThat(stringRecorder.toString(), containsString("VIRTUAL_MACHINE oid-1"));
        assertThat(stringRecorder.toString(), containsString("\"entityType\": 10 [[VIRTUAL_MACHINE]],"));
        assertThat(stringRecorder.toString(), containsString("++\"displayName\": \"changed-name\","));
        assertThat(stringRecorder.toString(), containsString("... 2 additional fields ..."));
    }

    /**
     * Should contain all details
     */
    @Test
    public void testCompleteVerbosity() throws Exception {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordChangeset("change", this::makeChange);
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    change                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------\n"));

        assertThat(stringRecorder.toString(), containsString("VIRTUAL_MACHINE oid-1"));
        assertThat(stringRecorder.toString(), containsString("++\"displayName\": \"changed-name\","));
        assertThat(stringRecorder.toString(), containsString("\"entityType\": 10 [[VIRTUAL_MACHINE]],"));
        assertThat(stringRecorder.toString(), containsString("\"commoditySoldList\": ["));
        assertThat(stringRecorder.toString(), containsString("\"type\": 65 [[VSTORAGE]]"));
        assertThat(stringRecorder.toString(), not(containsString("... 2 additional fields ...")));
    }

    @Test
    public void testOperationStartRecordedWhenChanges() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        journal.recordOperationBeginning(prettyOperation);
        journal.recordChangeset("change", this::makeChange);
        journal.recordOperationEnding();

        assertThat(stringRecorder.toString(), containsString("START: pretty operation"));
        assertThat(stringRecorder.toString(), containsString("END: pretty operation"));
    }

    @Test
    public void testOperationStartNotRecordedWhenEmpty() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        journal.recordOperationBeginning(prettyOperation);
        journal.recordOperationEnding();

        assertThat(stringRecorder.toString(), not(containsString("START: pretty operation")));
        assertThat(stringRecorder.toString(), not(containsString("END: pretty operation")));
    }

    @Test
    public void testOperationRecordedWhenOptionSet() {
        // Even when empty the operation should be recorded if record_empty_operations is set.
        final JournalOptions options = JournalOptions.newBuilder()
            .setRecordEmptyOperations(true)
            .build();

        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            options, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        journal.recordOperationBeginning(prettyOperation);
        journal.recordOperationEnding();

        assertThat(stringRecorder.toString(), containsString("START: pretty operation"));
        assertThat(stringRecorder.toString(), containsString("END: pretty operation"));
    }

    @Test
    public void testCurrentOperationCompleteVerbosity() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordOperationBeginning(prettyOperation);
        assertEquals(Verbosity.COMPLETE_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();

        journal.recordOperationBeginning(compactOperation);
        assertEquals(Verbosity.COMPLETE_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();
    }

    @Test
    public void testCurrentOperationLocalContextVerbosity() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            LOCAL_CONTEXT_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordOperationBeginning(prettyOperation);
        assertEquals(Verbosity.LOCAL_CONTEXT_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();

        journal.recordOperationBeginning(compactOperation);
        assertEquals(Verbosity.CHANGES_ONLY_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();
    }

    @Test
    public void testCurrentOperationChangesOnlyVerbosity() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            CHANGES_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordOperationBeginning(prettyOperation);
        assertEquals(Verbosity.CHANGES_ONLY_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();

        journal.recordOperationBeginning(compactOperation);
        assertEquals(Verbosity.CHANGES_ONLY_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();
    }

    @Test
    public void testCurrentOperationPreambleOnlyVerbosity() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.recordOperationBeginning(prettyOperation);
        assertEquals(Verbosity.PREAMBLE_ONLY_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();

        journal.recordOperationBeginning(compactOperation);
        assertEquals(Verbosity.PREAMBLE_ONLY_VERBOSITY, journal.currentOperationVerbosity());
        journal.recordOperationEnding();
    }

    @Test
    public void testCurrentPrettyOperationFormat() {
        final StitchingJournal<TopologyEntity> complete = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> localContext = new StitchingJournal<>(filter,
            LOCAL_CONTEXT_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> changesOnly = new StitchingJournal<>(filter,
            CHANGES_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> preambleOnly = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        Stream.of(complete, localContext, changesOnly, preambleOnly).forEach(journal -> {
                journal.recordOperationBeginning(prettyOperation);
                assertEquals(FormatRecommendation.PRETTY, journal.currentOperationFormat());
            }
        );
    }

    @Test
    public void testCurrentCompactOperationFormat() {
        final StitchingJournal<TopologyEntity> complete = new StitchingJournal<>(filter,
            COMPLETE_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> localContext = new StitchingJournal<>(filter,
            LOCAL_CONTEXT_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> changesOnly = new StitchingJournal<>(filter,
            CHANGES_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        final StitchingJournal<TopologyEntity> preambleOnly = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        Stream.of(complete).forEach(journal -> {
                journal.recordOperationBeginning(compactOperation);
                assertEquals(FormatRecommendation.PRETTY, journal.currentOperationFormat());
            }
        );
        Stream.of(localContext, changesOnly, preambleOnly).forEach(journal -> {
                journal.recordOperationBeginning(compactOperation);
                assertEquals(FormatRecommendation.COMPACT, journal.currentOperationFormat());
            }
        );
    }

    @Test
    public void testOperationChangesetLimit() {
        final JournalOptions options = JournalOptions.newBuilder()
            .setMaxChangesetsPerOperation(0)
            .setChangeGrouping(ChangeGrouping.SEPARATE_DISCRETE_CHANGES)
            .build();
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            options, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        when(prettyOperation.getOperationName()).thenReturn("Operation");
        journal.recordOperationBeginning(prettyOperation);
        journal.recordChangeset("foo", (changeset) -> makeChange(changeset, entity, "foo"));
        journal.recordChangeset("bar", (changeset) -> makeChange(changeset, entity, "bar"));
        journal.recordOperationEnding();

        assertThat(stringRecorder.toString(), CoreMatchers.containsString("foo"));
        assertThat(stringRecorder.toString(), not(CoreMatchers.containsString("bar")));
    }

    @Test
    public void testRecordingMetrics() {
        final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
            .addOids(entity.getOid())
            .build());

        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            JournalOptions.getDefaultInstance(), semanticDiffer,
            Collections.singletonList(stringRecorder), clock);
        when(prettyOperation.getOperationName()).thenReturn("Operation");
        journal.recordOperationBeginning(prettyOperation);
        journal.recordChangeset("foo", (changeset) -> makeChange(changeset, entity, "foo"));
        journal.recordChangeset("bar", (changeset) -> makeChange(changeset, otherEntity, "bar"));
        journal.recordOperationEnding();

        assertEquals(2, journal.getMetrics().getTotalChangesetsGenerated());
        assertEquals(1, journal.getMetrics().getTotalChangesetsIncluded());
        assertEquals(0, journal.getMetrics().getTotalEmptyChangests());
    }

    @Test
    public void testRecordTargetsEnabled() {
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        @SuppressWarnings("unchecked")
        final Supplier<List<TargetEntry>> entrySupplier = mock(Supplier.class);
        when(entrySupplier.get()).thenReturn(Collections.emptyList());

        journal.recordTargets(entrySupplier);
        verify(entrySupplier).get();
    }

    @Test
    public void testRecordTargetsDisabled() {
        final JournalOptions options = JournalOptions.newBuilder()
            .setIncludeTargetList(false)
            .build();
        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            options, semanticDiffer, Collections.singletonList(stringRecorder), clock);
        @SuppressWarnings("unchecked")
        final Supplier<List<TargetEntry>> entrySupplier = mock(Supplier.class);
        when(entrySupplier.get()).thenReturn(Collections.emptyList());

        journal.recordTargets(entrySupplier);
        verify(entrySupplier, never()).get();
    }

    @Test
    public void testDumpOnlyDumpsEntitiesPassingFilter() {
        final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
            .addOids(entity.getOid())
            .build());

        final StitchingJournal<TopologyEntity> journal = new StitchingJournal<>(filter,
            PREAMBLE_ONLY_VERBOSITY, semanticDiffer, Collections.singletonList(stringRecorder), clock);

        journal.dumpTopology(Stream.of(entity, otherEntity));
        assertThat(stringRecorder.toString(), CoreMatchers.containsString(Long.toString(entity.getOid())));
        assertThat(stringRecorder.toString(), not(CoreMatchers.containsString(Long.toString(otherEntity.getOid()))));
    }

    private void makeChange(@Nonnull final JournalChangeset<TopologyEntity> changeset) {
        makeChange(changeset, entity, "changed-name");
    }

    private void makeChange(@Nonnull final JournalChangeset<TopologyEntity> changeset,
                            @Nonnull final TopologyEntity entity,
                            @Nonnull final String newName) {
        changeset.beforeChange(entity);
        entity.getTopologyEntityDtoBuilder().setDisplayName(newName);
    }
}