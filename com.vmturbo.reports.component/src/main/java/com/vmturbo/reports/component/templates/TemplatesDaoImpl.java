package com.vmturbo.reports.component.templates;

import static com.vmturbo.reports.db.abstraction.tables.StandardReports.STANDARD_REPORTS;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Templates DAO with database backend.
 */
public class TemplatesDaoImpl implements TemplatesDao {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Creates report templates DAO associated with the specific Jooq context.
     *
     * @param dsl Jooq context to use
     */
    public TemplatesDaoImpl(@Nonnull DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Nonnull
    @Override
    public Collection<StandardReportsRecord> getAllReports() throws DbException {
        logger.debug("Getting all the report templates");
        final List<StandardReportsRecord> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(STANDARD_REPORTS)
                        .fetch()
                        .into(StandardReportsRecord.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching all the reports", e);
        }
        return records;
    }

    @Nonnull
    @Override
    public Optional<StandardReportsRecord> getTemplateById(int templateId) throws DbException {
        logger.debug("Getting template by id {}", templateId);
        final List<StandardReportsRecord> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(STANDARD_REPORTS)
                        .where(STANDARD_REPORTS.ID.eq(templateId))
                        .fetch()
                        .into(StandardReportsRecord.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching reporting template " + templateId, e);
        }
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0));
    }
}
