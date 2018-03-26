package com.vmturbo.reports.component.templates;

import static com.vmturbo.history.schema.abstraction.tables.ReportAttrs.REPORT_ATTRS;
import static com.vmturbo.history.schema.abstraction.tables.StandardReports.STANDARD_REPORTS;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Templates DAO with database backend.
 */
public class StandardTemplatesDaoImpl extends AbstractDbTemplateDao<StandardReportsRecord> {

    /**
     * Creates report templates DAO associated with the specific Jooq context.
     *
     * @param dsl Jooq context to use
     */
    public StandardTemplatesDaoImpl(@Nonnull DSLContext dsl) {
        super(dsl);
    }

    @Override
    @Nonnull
    public ReportType getReportType() {
        return ReportType.BIRT_STANDARD;
    }

    @Nonnull
    @Override
    public Collection<TemplateWrapper> getAllTemplates() throws DbException {
        getLogger().debug("Getting all the report templates");
        final Map<StandardReportsRecord, Result<Record>> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.select()
                        .from(STANDARD_REPORTS)
                        .leftJoin(REPORT_ATTRS)
                        .on(REPORT_ATTRS.REPORT_ID.eq(STANDARD_REPORTS.ID)
                                .and(REPORT_ATTRS.REPORT_TYPE.eq(getReportType())))
                        .orderBy(STANDARD_REPORTS.ID, REPORT_ATTRS.ID)
                        .fetchGroups(STANDARD_REPORTS);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching all the reports", e);
        }
        getLogger().debug("Successfully fetched {} standard template records", records::size);
        return mapResults(records);
    }

    @Nonnull
    @Override
    protected TemplateWrapper wrapTemplate(@Nonnull StandardReportsRecord template,
            @Nonnull List<ReportAttrsRecord> attributes) {
        return new StandardTemplateWrapper(template, attributes);
    }

    @Nonnull
    @Override
    public Optional<TemplateWrapper> getTemplateById(int templateId) throws DbException {
        getLogger().debug("Getting template by id {}", templateId);
        final Map<StandardReportsRecord, Result<Record>> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.select()
                        .from(STANDARD_REPORTS)
                        .leftJoin(REPORT_ATTRS)
                        .on(REPORT_ATTRS.REPORT_ID.eq(STANDARD_REPORTS.ID)
                                .and(REPORT_ATTRS.REPORT_TYPE.eq(getReportType())))
                        .where(STANDARD_REPORTS.ID.eq(templateId))
                        .orderBy(REPORT_ATTRS.ID)
                        .fetchGroups(STANDARD_REPORTS);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching standard reporting template " + templateId, e);
        }
        return mapOneLineResult(records);
    }
}
