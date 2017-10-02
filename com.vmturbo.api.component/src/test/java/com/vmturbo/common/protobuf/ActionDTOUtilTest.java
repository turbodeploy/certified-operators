package com.vmturbo.common.protobuf;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

import static org.junit.Assert.assertEquals;

public class ActionDTOUtilTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMapNormalSeverity() throws Exception {
        // It must be BELOW the Normal value to map to Normal.
        assertEquals(Severity.NORMAL, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapMinorSeverity() throws Exception {
        // This is slightly weird, but if importance maps exactly to the Normal value it is considered Minor.
        assertEquals(Severity.MINOR, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD));

        assertEquals(Severity.MINOR, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MINOR_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapMajorSeverity() throws Exception {
        assertEquals(Severity.MAJOR, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MINOR_SEVERITY_THRESHOLD));

        assertEquals(Severity.MAJOR, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapCriticalSeverity() throws Exception {
        assertEquals(Severity.CRITICAL, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD));
        assertEquals(Severity.CRITICAL, ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD + 1.0));
    }

    @Test
    public void testMapIllegalImportance() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        ActionDTOUtil.mapImportanceToSeverity(Double.NaN);
    }
}