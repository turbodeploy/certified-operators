package db.migration;

import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.BaseJdbcMigration;

/**
 * Migration to populate newly-created (by migration V1.35) available_timestamps table, with existing
 * timestamps found in the corresponding stats tables.
 *
 * <p>This will will permit much more efficient execution of some frequently used queries.</p>
 *
 * <p>This is a copy of migration V1.27.1, created during the merge of 7.17 and 7.21 parallel
 * development branches in order to handle migration sequence gaps.</p>
 */
public class V1_35_1__Initialize_available_history_times_table extends BaseJdbcMigration {
    private static Logger logger = LogManager.getLogger();

    @Override
    public void migrate(final Connection connection) throws Exception {
        // we can just use the existing V1.27 migration to do all the work
        new V1_28_1__Initialize_available_history_times_table(logger).migrate(connection);
    }
}

