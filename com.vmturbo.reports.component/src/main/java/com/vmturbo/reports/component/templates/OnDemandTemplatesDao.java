package com.vmturbo.reports.component.templates;

import static com.vmturbo.reports.db.abstraction.tables.OnDemandReports.ON_DEMAND_REPORTS;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.reports.db.abstraction.tables.records.OnDemandReportsRecord;
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

    @Nonnull
    @Override
    public Collection<OnDemandReportsRecord> getAllTemplates() throws DbException {
        return super.getAllReports(OnDemandReportsRecord.class, ON_DEMAND_REPORTS);
    }

    @Nonnull
    @Override
    public Optional<OnDemandReportsRecord> getTemplateById(int templateId) throws DbException {
        getLogger().debug("Getting template by id {}", templateId);
        final List<OnDemandReportsRecord> records;
        try {
            records = getDsl().transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(ON_DEMAND_REPORTS)
                        .where(ON_DEMAND_REPORTS.ID.eq(templateId))
                        .fetch()
                        .into(OnDemandReportsRecord.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching on-demand reporting template " + templateId, e);
        }
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0));
    }
}
