package com.vmturbo.reports.component.templates;

import static com.vmturbo.history.schema.abstraction.tables.OnDemandReports.ON_DEMAND_REPORTS;
import static com.vmturbo.history.schema.abstraction.tables.ReportAttrs.REPORT_ATTRS;
import static com.vmturbo.history.schema.abstraction.tables.StandardReports.STANDARD_REPORTS;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.history.schema.abstraction.tables.records.OnDemandReportsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * DAO for on-demand report templates.
 */
@ThreadSafe
public class OnDemandTemplatesDao extends AbstractDbTemplateDao<OnDemandReportsRecord> {

    /**
     * Constructs on-demand report templates DAO with the specific DSL context.
     *
     * @param dsl DB access context
     */
    public OnDemandTemplatesDao(@Nonnull DSLContext dsl) {
        super(dsl);
    }

    @Override
    @Nonnull
    public ReportType getReportType() {
        return ReportType.BIRT_ON_DEMAND;
    }

    @Nonnull
    @Override
    public Collection<TemplateWrapper> getAllTemplates() throws DbException {
        getLogger().debug("Getting all the report templates");
        final Map<OnDemandReportsRecord, Result<Record>> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.select()
                        .from(ON_DEMAND_REPORTS)
                        .leftJoin(REPORT_ATTRS)
                        .on(REPORT_ATTRS.REPORT_ID.eq(ON_DEMAND_REPORTS.ID)
                                .and(REPORT_ATTRS.REPORT_TYPE.eq(getReportType())))
                        .orderBy(ON_DEMAND_REPORTS.ID, REPORT_ATTRS.ID)
                        .fetchGroups(ON_DEMAND_REPORTS);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching all the reports", e);
        }
        getLogger().debug("Successfully fetched {} on-demand template records", records::size);
        return mapResults(records);
    }

    @Override
    @Nonnull
    protected TemplateWrapper wrapTemplate(@Nonnull OnDemandReportsRecord template,
            @Nonnull List<ReportAttrsRecord> attributes) {
        return new OnDemandTemplateWrapper(template, attributes);
    }

    @Nonnull
    @Override
    public Optional<TemplateWrapper> getTemplateById(int templateId) throws DbException {
        getLogger().debug("Getting template by id {}", templateId);
        final Map<OnDemandReportsRecord, Result<Record>> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.select()
                        .from(ON_DEMAND_REPORTS)
                        .leftJoin(REPORT_ATTRS)
                        .on(ON_DEMAND_REPORTS.ID.eq(REPORT_ATTRS.REPORT_ID)
                                .and(REPORT_ATTRS.REPORT_TYPE.eq(getReportType())))
                        .where(ON_DEMAND_REPORTS.ID.eq(templateId))
                        .orderBy(REPORT_ATTRS.ID)
                        .fetchGroups(ON_DEMAND_REPORTS);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching on-demand reporting template " + templateId, e);
        }
        return mapOneLineResult(records);
    }
}
