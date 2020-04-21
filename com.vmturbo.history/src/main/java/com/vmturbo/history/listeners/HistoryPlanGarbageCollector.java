package com.vmturbo.history.listeners;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

/**
 * Responsible for cleaning data of deleted plans from the history component.
 */
public class HistoryPlanGarbageCollector implements PlanGarbageCollector {
    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    /**
     * Constructor.
     *
     * @param historydbIO Used to actually access plan data.
     */
    public HistoryPlanGarbageCollector(final HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    @Nonnull
    @Override
    public List<ListExistingPlanIds> listPlansWithData() {
        return Collections.singletonList(historydbIO::listPlansWithStats);
    }

    @Override
    public void deletePlanData(final long planId) {
        try {
            historydbIO.deletePlanStats(planId);
        } catch (VmtDbException e) {
            logger.error("Failed to delete plan stats for " + planId, e);
        }
    }
}
