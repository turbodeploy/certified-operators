package com.vmturbo.topology.processor.stitching.journal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.IStitchingJournal.JournalChangeset;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingMetrics;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.topology.processor.stitching.journal.ChangesetMerger.ChangesetUnion;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

public class ChangesetMergerTest {
    private final JournalFilter filter = new IncludeAllFilter();
    private final StitchingMetrics stitchingMetrics = new StitchingMetrics();

    final JournalChangeset<TopologyEntity> changeset1 = new JournalChangeset<>("remove", filter,
        stitchingMetrics,1);
    final JournalChangeset<TopologyEntity> changeset2 = new JournalChangeset<>("change", filter,
        stitchingMetrics,2);
    final JournalChangeset<TopologyEntity> changeset3 = new JournalChangeset<>("merge", filter,
        stitchingMetrics,3);

    final ChangesetUnion<TopologyEntity> union1 = new ChangesetMerger.ChangesetUnion<>(changeset1);
    final ChangesetUnion<TopologyEntity> union2 = new ChangesetMerger.ChangesetUnion<>(changeset2);
    final ChangesetUnion<TopologyEntity> union3 = new ChangesetMerger.ChangesetUnion<>(changeset3);

    final JournalRecorder stringRecorder = new StringBuilderRecorder();
    final IStitchingJournal<TopologyEntity> journal = new StitchingJournal<>(
        new TopologyEntitySemanticDiffer(Verbosity.LOCAL_CONTEXT_VERBOSITY), stringRecorder);

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    final TopologyEntity entity1 = TopologyEntityUtils.topologyEntityBuilder(
        1, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();
    final TopologyEntity entity2 = TopologyEntityUtils.topologyEntityBuilder(
        2, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();
    final TopologyEntity entity3 = TopologyEntityUtils.topologyEntityBuilder(
        3, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();
    final TopologyEntity entity4 = TopologyEntityUtils.topologyEntityBuilder(
        4, EntityType.VIRTUAL_MACHINE, Collections.emptyList()).build();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testChangesetUnionIsRepresentativeMember() {
        assertTrue(union1.isRepresentativeMember());
        assertTrue(union2.isRepresentativeMember());

        union1.unionByRank(union2);

        assertEquals(union1.find() == union1, union1.isRepresentativeMember());
        assertEquals(union2.find() == union2, union2.isRepresentativeMember());
    }

    @Test
    public void testChangesetUnionAndFind() {
        assertNotEquals(union1.find(), union2.find());
        assertNotEquals(union1.find(), union3.find());
        assertNotEquals(union2.find(), union3.find());

        union1.unionByRank(union2);
        assertEquals(union1.find(), union2.find());
        assertEquals(union2.find(), union1.find());
        assertNotEquals(union1.find(), union3.find());
        assertNotEquals(union2.find(), union3.find());

        union1.unionByRank(union3);
        assertEquals(union1.find(), union2.find());
        assertEquals(union1.find(), union3.find());
        assertEquals(union2.find(), union3.find());
    }

    @Test
    public void testChangesetMerging() {
        // Add 1,2 to CS1
        // Add 2,3 to CS2
        // Add 3,4 to CS3
        changeset1.beforeChange(entity1);
        changeset1.beforeChange(entity2);
        changeset2.beforeChange(entity2);
        changeset2.beforeChange(entity3);
        changeset3.beforeChange(entity3);
        changeset3.beforeChange(entity4);

        entity1.getTopologyEntityDtoBuilder().setDisplayName("changed");

        // All changesets should be merged.
        final ChangesetMerger<TopologyEntity> merger = new ChangesetMerger<>();
        merger.add(changeset1);
        merger.add(changeset2);
        merger.add(changeset3);

        merger.recordAll(journal);
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                  remove and                                  |\n" +
            "|                                  change and                                  |\n" +
            "|                                    merge                                     |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------"));
    }

    @Test
    public void testChangesetsNotMerged() {
        // Add 1 to CS1
        // Add 2 to CS2
        // Add 3 to CS3
        changeset1.beforeChange(entity1);
        changeset2.beforeChange(entity2);
        changeset3.beforeChange(entity3);

        entity1.getTopologyEntityDtoBuilder().setDisplayName("changed-1");
        entity2.getTopologyEntityDtoBuilder().setDisplayName("changed-2");
        entity3.getTopologyEntityDtoBuilder().setDisplayName("changed-3");

        final ChangesetMerger<TopologyEntity> merger = new ChangesetMerger<>();
        merger.add(changeset1);
        merger.add(changeset2);
        merger.add(changeset3);

        // No changesets should be merged.
        merger.recordAll(journal);
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    remove                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------"));
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    change                                    |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------"));
        assertThat(stringRecorder.toString(), containsString(
            "--------------------------------------------------------------------------------\n" +
            "|                                                                              |\n" +
            "|                                    merge                                     |\n" +
            "|                                                                              |\n" +
            "--------------------------------------------------------------------------------\n"));
    }

    @Test
    public void testClear() {
        // Add 1 to CS1
        // Add 2 to CS2
        // Add 3 to CS3
        changeset1.beforeChange(entity1);
        changeset2.beforeChange(entity2);
        changeset3.beforeChange(entity3);

        final ChangesetMerger<TopologyEntity> merger = new ChangesetMerger<>();
        merger.add(changeset1);
        merger.add(changeset2);
        merger.add(changeset3);

        merger.clear();
        assertThat(stringRecorder.toString(), isEmptyOrNullString());
    }
}