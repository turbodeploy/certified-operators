package com.vmturbo.reports.component.templates;

import static com.vmturbo.reports.db.abstraction.tables.StandardReports.STANDARD_REPORTS;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
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

    @Nonnull
    @Override
    public Collection<StandardReportsRecord> getAllTemplates() throws DbException {
        return getAllReports(StandardReportsRecord.class, STANDARD_REPORTS);
    }

    @Nonnull
    @Override
    public Optional<StandardReportsRecord> getTemplateById(int templateId) throws DbException {
        getLogger().debug("Getting template by id {}", templateId);
        final List<StandardReportsRecord> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(STANDARD_REPORTS)
                        .where(STANDARD_REPORTS.ID.eq(templateId))
                        .fetch()
                        .into(StandardReportsRecord.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching standard reporting template " + templateId, e);
        }
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0));
    }
}
