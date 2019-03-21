package com.vmturbo.reports.component.data;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;
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
     * @param id report template id.
     * @return true if the data generation is successfully.
     */
    public boolean generateByTemplateId(final int id) {
        if (reportMap.containsKey(id)) {
            try {
                reportMap.get(id).generateData(context);
            } catch (DbException e) {
                logger.error("Failed to generate report data for template id: " + id);
                return false;
            }
            logger.info("Generated report data for template with id: " + id);
        }
        logger.info("The template id (" + id + ") is not defined skip data generation.");
        return true;
    }
}
