package db.migrations.history.common;

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
public class V1v35v1InitializeAvailableHistoryTimesTable extends BaseJdbcMigration {

    private final Logger logger = LogManager.getLogger();

    @Override
    public void migrate(final Connection connection) throws Exception {
        // we can just use the existing V1.27 migration to do all the work
        new V1v28v1InitializeAvailableHistoryTimesTable(logger).migrate(connection);
    }
}

