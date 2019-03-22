package com.vmturbo.reports.component.data;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert missing report data to vmtdb database.
 */
public class ReportsDataGenerator {
    private final Logger logger = LogManager.getLogger();
    private final ReportsDataContext context;
    private final Map<Integer, ReportTemplate> reportMap;

    public ReportsDataGenerator(@Nonnull final ReportsDataContext context,
                                @Nonnull final Map<Integer, ReportTemplate> reportMap) {
        this.context = Objects.requireNonNull(context);
        this.reportMap = Objects.requireNonNull(reportMap);
    }

    /**
     * Generate data based on report template id.
     *
     * @param id report template id.
     * @return true if the data generation is successfully.
     */
    public boolean generateByTemplateId(final int id) {
        if (reportMap.containsKey(id)) {
            final ReportTemplate reportTemplate = reportMap.get(id);
            try (DataMetricTimer timer = Metrics.REPORT_DATA_GENERATION_DURATION_CALCULATION
                .labels(reportTemplate.getClass().getSimpleName()).startTimer()) {
                reportTemplate.generateData(context);
            } catch (DbException e) {
                logger.error("Failed to generate report data for template id: " + id);
                Metrics.REPORT_DATA_GENERATION_ERROR_COUNTS_SUMMARY
                    .labels(reportTemplate.getClass().getSimpleName())
                    .increment();
                return false;
            }
            logger.info("Generated report data for template with id: " + id);
        }
        logger.info("The template id (" + id + ") is not defined skip data generation.");
        return true;
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
