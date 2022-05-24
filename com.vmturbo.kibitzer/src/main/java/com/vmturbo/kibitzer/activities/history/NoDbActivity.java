package com.vmturbo.kibitzer.activities.history;

import java.time.Duration;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kibitzer.KibitzerComponent;
import com.vmturbo.kibitzer.KibitzerDb.DbMode;
import com.vmturbo.kibitzer.activities.KibitzerActivity;

/**
 * A very simple activity that can be used in manual testing, without using a database.
 */
public class NoDbActivity extends KibitzerActivity<Void, Void> {
    private static final Logger logger = LogManager.getLogger();
    private static final String NO_DB_ACTIVITY_NAME = "no_db";

    /**
     * Create a new instance.
     *
     * @throws KibitzerActivityException if there's a problem creating the instance
     */
    public NoDbActivity() throws KibitzerActivityException {
        super(KibitzerComponent.ANY, NO_DB_ACTIVITY_NAME);
        config.setDefault(DESCRIPTION_CONFIG_KEY, "Simple activity with no DB needed");
        config.setDefault(DB_MODE_CONFIG_KEY, DbMode.NONE);
        config.setDefault(RUNS_CONFIG_KEY, 10);
        config.setDefault(SCHEDULE_CONFIG_KEY, Duration.ofSeconds(2));
    }

    @Override
    public KibitzerActivity<Void, Void> newInstance() throws KibitzerActivityException {
        return new NoDbActivity();
    }

    @Override
    public Optional<Void> run(int repetition) {
        logger.info("NODB activity repetition #{}", repetition);
        return Optional.empty();
    }
}
