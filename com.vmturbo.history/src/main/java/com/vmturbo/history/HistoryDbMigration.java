package com.vmturbo.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;

/**
 * Handle the creation and update of the DB.
 **/
public class HistoryDbMigration {

    private final static Logger log = LogManager.getLogger();

    private HistorydbIO historydbIO;

    /**
     * Create a handler for the creation and update of the DB.
     *
     * @param historydbIO - used to perform RDB operations.
     */
    public HistoryDbMigration(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Perform the DB initialization and migration, if required.
     *
     * @throws VmtDbException if there is an error initializing the VmtDB database
     */
    public void migrate() throws VmtDbException {
        log.info("Starting DB migration");
        historydbIO.init(false, null, historydbIO.getDatabaseName());
        log.info("DB Migration complete");
    }
}
