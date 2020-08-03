package com.vmturbo.reports.component.data;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.reporting.api.ReportingConstants;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reports.component.data.ReportDataUtils.MetaGroup;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert missing report data to vmtdb database.
 */
public class ReportsDataGenerator {
    private final Logger logger = LogManager.getLogger();
    private final ReportsDataContext context;
    private final Map<Long, ReportTemplate> reportMap;


    public ReportsDataGenerator(@Nonnull final ReportsDataContext context,
                                @Nonnull final Map<Long, ReportTemplate> reportMap
                                ) {
        this.context = Objects.requireNonNull(context);
        this.reportMap = Objects.requireNonNull(reportMap);
    }

    /**
     * Generate data based on report request.
     *
     * @param request report generation request.
     * @return newly generated group id from entities table.
     */
    public Optional<String> generateDataByRequest(final GenerateReportRequest request) {
        final long id = request.getTemplate().getId();
        // Always need to clean up "fake_vm_group"
        cleanUpFakeVmGroup();
        if (reportMap.containsKey(id)) {
            final Optional<Long> selectedUuid = (request.getParametersMap() != null &&
                request.getParametersMap().containsKey(ReportingConstants.ITEM_UUID_PROPERTY)) ?
                Optional.ofNullable(Long.valueOf(request.getParametersMap()
                    .get(ReportingConstants.ITEM_UUID_PROPERTY))) : Optional.empty();
            final ReportTemplate reportTemplate = reportMap.get(id);
            try (DataMetricTimer timer = Metrics.REPORT_DATA_GENERATION_DURATION_CALCULATION
                .labels(reportTemplate.getClass().getSimpleName()).startTimer()) {
                logger.info("Start generating report data for template with id: {}", id);
                final Optional<String> groupIdOptional = reportTemplate.generateData(context, selectedUuid);
                logger.info("Generated report data for template with id: {}", id);
                return groupIdOptional;
            } catch (DbException e) {
                logger.error("Failed to generate report data for template id: " + id);
                Metrics.REPORT_DATA_GENERATION_ERROR_COUNTS_SUMMARY
                    .labels(reportTemplate.getClass().getSimpleName())
                    .increment();
                return Optional.empty();
            }
        }
        logger.info("The template id ({}) is not defined skip data generation.", id);
        return Optional.empty();
    }

    // Some report required tactical "fake_vm_group" in vmtdb to work propertly, but we don't want
    // this group to show up in other reports.
    private void cleanUpFakeVmGroup() {
        context.getReportDataWriter().cleanGroup(MetaGroup.FAKE_VM_GROUP);
    }

    /**
     * Get supported reports.
     * @return supported report Map
     */
    public Map<Long,ReportTemplate> getSupportedReport() {
        return Collections.unmodifiableMap(reportMap);
    }

    private static class Metrics {
        private static final DataMetricSummary REPORT_DATA_GENERATION_DURATION_CALCULATION =
            DataMetricSummary.builder()
                .withName("report_data_generation_calculation_seconds")
                .withHelp("Time taken to generate report data in report component.")
                .withLabelNames("report_template_name")
                .build()
                .register();

        private static final DataMetricCounter REPORT_DATA_GENERATION_ERROR_COUNTS_SUMMARY =
            DataMetricCounter.builder()
                .withName("report_data_generation_error_count")
                .withHelp("Number of report data generation errors.")
                .withLabelNames("report_template_name")
                .build()
                .register();
    }
}
