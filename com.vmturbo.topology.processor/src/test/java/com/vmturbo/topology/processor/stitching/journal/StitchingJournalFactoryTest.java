package com.vmturbo.topology.processor.stitching.journal;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.Stitching.ChangeGrouping;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.RandomEntitySelector;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.RandomEntitySelector.TargetEntitySectionWeight;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.RandomEntityStitchingJournalFactory;

public class StitchingJournalFactoryTest {

    private final ConfigurableStitchingJournalFactory configurableJournalFactory =
        StitchingJournalFactory.configurableStitchingJournalFactory(Clock.systemUTC());

    private final Clock clock = mock(Clock.class);

    private final RandomEntityStitchingJournalFactory randomEntityJournalFactory =
        StitchingJournalFactory.randomEntityStitchingJournalFactory(clock, 5, 5, 1);

    private final StitchingContext stitchingContext = mock(StitchingContext.class);

    @Before
    public void setup() {
        when(clock.millis()).thenReturn(100L);
    }

    @Test
    public void testRecorders() throws Exception {
        assertEquals(0, configurableJournalFactory.getRecorders().size());

        final JournalRecorder recorder = mock(JournalRecorder.class);
        configurableJournalFactory.addRecorder(recorder);

        assertEquals(1, configurableJournalFactory.getRecorders().size());
        assertEquals(recorder, configurableJournalFactory.getRecorders().iterator().next());

        assertTrue(configurableJournalFactory.removeRecorder(recorder));
        assertEquals(0, configurableJournalFactory.getRecorders().size());
    }

    @Test
    public void testJournalOptions() throws Exception {
        assertEquals(JournalOptions.getDefaultInstance(), configurableJournalFactory.getJournalOptions());

        final JournalOptions options = JournalOptions.newBuilder()
            .setChangeGrouping(ChangeGrouping.SEPARATE_DISCRETE_CHANGES)
            .setVerbosity(Verbosity.COMPLETE_VERBOSITY)
            .build();

        configurableJournalFactory.setJournalOptions(options);
        assertEquals(options, configurableJournalFactory.getJournalOptions());
    }

    @Test
    public void testRandomFactoryDoesNotShareBuffers() {
        // Ensure that creating separate journals do not share recorders that re-use the same buffer.
        final IStitchingJournal<StitchingEntity> journal1 =
            randomEntityJournalFactory.stitchingJournal(stitchingContext);
        journal1.recordMessage("foo");

        final IStitchingJournal<StitchingEntity> journal2 =
            randomEntityJournalFactory.stitchingJournal(stitchingContext);
        journal2.recordMessage("bar");

        assertEquals("foo\n", journal1.getRecorders().iterator().next().toString());
        assertEquals("bar\n", journal2.getRecorders().iterator().next().toString());
    }

    @Test
    public void testGetTargetEntityProbabilities() {
        // Ensure that the list of generated target entity probabilities sectionWeightTop
        // values are monotonically increasing.
        final TopologyStitchingEntity mockEntity = mock(TopologyStitchingEntity.class);
        when(stitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(
            ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE,
                    ImmutableMap.of(
                        1234L, Arrays.asList(mockEntity, mockEntity, mockEntity),
                        5678L, Collections.singletonList(mockEntity)),
                EntityType.PHYSICAL_MACHINE,
                ImmutableMap.of(
                    1234L, Arrays.asList(mockEntity, mockEntity),
                    5678L, Collections.singletonList(mockEntity)),
                EntityType.STORAGE,
                ImmutableMap.of(
                    1234L, Arrays.asList(mockEntity, mockEntity),
                    5678L, Collections.emptyList())
            ));

        final RandomEntitySelector selector = new RandomEntitySelector(stitchingContext);
        final List<TargetEntitySectionWeight> probabilities = selector.getTargetEntitySectionWeights();

        double previousTop = 0;
        for (TargetEntitySectionWeight probability : probabilities) {
            assertThat(previousTop, lessThan(probability.sectionWeightTop));
            previousTop = probability.sectionWeightTop;
        }

        previousTop = 0;
        double greatestProbability = 0;
        TargetEntitySectionWeight biggest = null;
        for (TargetEntitySectionWeight probability : probabilities) {
            if (biggest == null) {
                greatestProbability = probability.sectionWeightTop - previousTop;
                biggest = probability;
            } else if (greatestProbability < probability.sectionWeightTop - previousTop) {
                greatestProbability = probability.sectionWeightTop - previousTop;
                biggest = probability;
            }

            previousTop = probability.sectionWeightTop;
        }

        assertNotNull(biggest);
        assertEquals(EntityType.VIRTUAL_MACHINE, biggest.entityType);
        assertEquals(1234L, biggest.targetId);
    }
}