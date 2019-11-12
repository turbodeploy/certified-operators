package com.vmturbo.history;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;

/**
 * Handle the creation and update of the DB.
 **/
public class HistoryDbMigration {

    private final static Logger log = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final Optional<String> migrationLocationOverride;

    /**
     * Create a handler for the creation and update of the DB.
     *
     * @param historydbIO - used to perform RDB operations.
     * @param migrationLocationOverride If set, the path in the classpath to look for migrations.
     */
    public HistoryDbMigration(HistorydbIO historydbIO, Optional<String> migrationLocationOverride) {
        this.historydbIO = historydbIO;
        this.migrationLocationOverride = migrationLocationOverride;
    }

    /**
     * Perform the DB initialization and migration, if required.
     *
     * @throws VmtDbException if there is an error initializing the VmtDB database
     */
    public void migrate() throws VmtDbException {
        log.info("Starting DB migration");
        historydbIO.init(false, null, historydbIO.getDbSchemaName(), migrationLocationOverride);
        log.info("DB Migration complete");
    }
}
