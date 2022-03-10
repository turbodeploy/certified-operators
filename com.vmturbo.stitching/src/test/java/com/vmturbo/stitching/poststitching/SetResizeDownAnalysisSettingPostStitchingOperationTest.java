package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class SetResizeDownAnalysisSettingPostStitchingOperationTest {

    private final Clock clock = Mockito.mock(Clock.class);

    private final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private UnitTestResultBuilder resultBuilder;

    private final long oid = 72453759857430L;

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private TopologyEntity vmEntity = PostStitchingTestUtilities.makeTopologyEntityBuilder(oid,
            EntityType.VIRTUAL_MACHINE_VALUE, Collections.emptyList(), Collections.emptyList()).build();

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void testEnableResizeDown() {
        final long milliseconds = IdentityGenerator.toMilliTime(oid);
        Mockito.when(clock.millis()).thenReturn(milliseconds);
        SetResizeDownAnalysisSettingPostStitchingOperation resizeDownOperation =
                new SetResizeDownAnalysisSettingPostStitchingOperation(0, clock);
        resizeDownOperation.performOperation(Stream.of(vmEntity), settingsCollection, resultBuilder);
        assertEquals(1, resultBuilder.getChanges().size());
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertTrue(vmEntity.getTopologyEntityImpl()
                .getAnalysisSettings()
                .getIsEligibleForResizeDown());
    }

    @Test
    public void testDisableResizeDown() {
        final long milliseconds = IdentityGenerator.toMilliTime(oid);
        Mockito.when(clock.millis()).thenReturn(milliseconds);
        SetResizeDownAnalysisSettingPostStitchingOperation resizeDownOperation =
                new SetResizeDownAnalysisSettingPostStitchingOperation(1, clock);
        resizeDownOperation.performOperation(Stream.of(vmEntity), settingsCollection, resultBuilder);
        assertEquals(0, resultBuilder.getChanges().size());
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(vmEntity.getTopologyEntityImpl()
                .getAnalysisSettings()
                .getIsEligibleForResizeDown());
    }
}
