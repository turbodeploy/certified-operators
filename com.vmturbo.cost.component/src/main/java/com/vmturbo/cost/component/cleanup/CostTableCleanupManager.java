package com.vmturbo.cost.component.cleanup;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableCleanupInfo;
import com.vmturbo.cost.component.cleanup.TableCleanupWorker.TableCleanupWorkerFactory;

/**
 * Class for trimming and cleaning up the Cost Stats tables (RI utilization and coverage, entity costs etc)
 * based on retention periods. Runs on a scheduler.
 */
public class CostTableCleanupManager {

    private static final Logger logger = LogManager.getLogger();

    private final TaskScheduler workerScheduler;

    private final Map<Table<?>, TableCleanupWorker> cleanupWorkerMap;

    /**
     * Constructor for the Cost stats cleanup manager.
     * @param cleanupWorkerFactory The cleanup worker factory.
     * @param workerScheduler A task scheduler for cleanup workers.
     * @param tableCleanups The table cleanups.
     */
    public CostTableCleanupManager(@Nonnull TableCleanupWorkerFactory cleanupWorkerFactory,
                                   @Nonnull TaskScheduler workerScheduler,
                                   @Nonnull final List<CostTableCleanup> tableCleanups) {

        this.workerScheduler = Objects.requireNonNull(workerScheduler);
        this.cleanupWorkerMap = tableCleanups.stream()
                .collect(ImmutableMap.toImmutableMap(
                        tableCleanup -> tableCleanup.tableInfo().table(),
                        cleanupWorkerFactory::createWorker,
                        (w1, w2) -> {
                            logger.warn("Two table cleanups found for table '{}'. Using first configuration.",
                                    w1.cleanupInfo().table());
                            return w1;
                        }));

        initializeCleanupSchedule();
    }

    /**
     * Checks whether there is currently a long running cleanup task for the specified {@code table}.
     * @param table The target table.
     * @return True, if a currenlty running cleanup is long running.
     */
    public boolean isTableCleanupLongRunning(@Nonnull Table<?> table) {
        return cleanupWorkerMap.containsKey(table) && cleanupWorkerMap.get(table).isLongRunning();
    }


    private void initializeCleanupSchedule() {

        cleanupWorkerMap.values().forEach(cleanupWorker -> {

            final TableCleanupInfo cleanupInfo = cleanupWorker.cleanupInfo();

            logger.info("Scheduling clean up of '{}' at intervals of '{}'",
                    cleanupInfo.shortTableName(), cleanupInfo.cleanupRate());
            workerScheduler.scheduleAtFixedRate(cleanupWorker::cleanupTable, cleanupInfo.cleanupRate());
        });
    }
}
