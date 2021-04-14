package com.vmturbo.auth.component.policy;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.auth.component.licensing.LicenseCheckService;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Test {@link ReportPolicy} logic.
 */
public class ReportPolicyTest {

    private LicenseCheckService licenseCheckService = mock(LicenseCheckService.class);

    private ReportPolicy reportPolicy = new ReportPolicy(licenseCheckService, false);

    /**
     * Test that count of report editors equals the default value if there is no applied grafana
     * licenses.
     */
    @Test
    public void testMaxNumberOfAllowedReportEditorsWithoutSummary() {
        assertThat(reportPolicy.getAllowedMaximumEditors(),
            is(LicenseSummary.getDefaultInstance().getMaxReportEditorsCount()));
    }

    /**
     * Test that count of report editors equals the max count of report editors configured in
     * grafana license.
     */
    @Test
    public void testMaxNumberOfAllowedReportEditorsWithSummary() {
        final int numEditors = 100;
        when(licenseCheckService.getLastSummary()).thenReturn(LicenseSummary.newBuilder()
            .setMaxReportEditorsCount(numEditors)
            .build());
        assertThat(reportPolicy.getAllowedMaximumEditors(),
                is(numEditors));
    }
}
