package com.vmturbo.cost.component.db;

import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;
import static com.vmturbo.cost.component.db.tables.AccountExpensesByMonth.ACCOUNT_EXPENSES_BY_MONTH;
import static com.vmturbo.cost.component.db.tables.AccountExpensesRetentionPolicies.ACCOUNT_EXPENSES_RETENTION_POLICIES;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.jooq.PurgeEvent;
import com.vmturbo.sql.utils.jooq.PurgeProcedure;

/**
 * Spring configuration for topology ingestion components.
 */
@Configuration
@Import({DbAccessConfig.class})
@Conditional(DbEndpointCondition.class)
public class StoredProcedureConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    /**
     * Event for purging the data in percentile_blobs table regularly.
     *
     * @return purge event
     */
    @Bean
    PurgeEvent purgeExpiredAccountExpensesData() {
        try {
            final DSLContext dsl = dsl();
            final PurgeProcedure purgeAE = new PurgeProcedure(dsl,
                    ACCOUNT_EXPENSES, ACCOUNT_EXPENSES.EXPENSE_DATE,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES.POLICY_NAME,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES.RETENTION_PERIOD,
                    "retention_days", DatePart.DAY);
            final PurgeProcedure purgeAEByMonth = new PurgeProcedure(dsl,
                    ACCOUNT_EXPENSES_BY_MONTH, ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES.POLICY_NAME,
                    ACCOUNT_EXPENSES_RETENTION_POLICIES.RETENTION_PERIOD,
                    "retention_months", DatePart.MONTH);
            return createPurgeEvent("purge_expired_account_expenses_data", () -> {
                purgeAE.run();
                purgeAEByMonth.run();
            }).schedule(1, 24);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create purgeExpiredAccountExpensesData", e);
        }
    }

    /**
     * Get the dsl context, which can be overridden by testing.
     *
     * @return {@link DSLContext}
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DSLContext dsl() throws SQLException, UnsupportedDialectException, InterruptedException {
        return dbAccessConfig.dsl();
    }

    /**
     * Create the purge event, which can be overridden by testing.
     *
     * @param name name of the event
     * @param runnable operation to perform
     * @return {@link PurgeEvent}
     */
    public PurgeEvent createPurgeEvent(String name, Runnable runnable) {
        return new PurgeEvent(name, runnable);
    }
}
