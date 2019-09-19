package com.vmturbo.reports.component.data;

import static com.vmturbo.reports.component.data.TestHelper.resizeActionSpec;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.reports.component.data.ReportDataUtils.RightSizingInfo;

public class ReportDataUtilsTest {
    @Test
    public void testGetRightSizingInfo() throws Exception {

        final RightSizingInfo rightSizingInfo = ReportDataUtils.getRightSizingInfo(resizeActionSpec(1L));
        assertEquals("0", rightSizingInfo.getSavings());
        // "dummy name" is not used but required to populate for reporting
        assertEquals("dummy name", rightSizingInfo.getTargetName());
        assertEquals("Resize VMem on the entity from 0 GB to 0 GB", rightSizingInfo.getExplanation());
        assertEquals("0 GB", rightSizingInfo.getFrom());
        assertEquals("0 GB", rightSizingInfo.getTo());
        assertFalse(rightSizingInfo.getCloudAction());
    }
}
