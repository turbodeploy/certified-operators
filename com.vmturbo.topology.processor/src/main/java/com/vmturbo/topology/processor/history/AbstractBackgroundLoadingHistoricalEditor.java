package com.vmturbo.topology.processor.history;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

/**
 * Historical commodities editor with cached commodity data state that initiates data from
 * persistent store in background.
 * While background loading is in progress, related historical data are not set into the broadcast.
 *
 * @param <DbValue> the pre-calculated element of per-field data as retrieved from the persistent store
 * @param <HistoryData> per-commodity field historical data to cache that wraps DbValue with runtime info
 * @param <HistoryLoadingTask> loader of DbValue's from the persistent store
 * @param <Config> per-editor type configuration values holder
 * @param <Stub> type of history component stub
 */
public abstract class AbstractBackgroundLoadingHistoricalEditor<HistoryData extends IHistoryCommodityData<Config, DbValue>,
            HistoryLoadingTask extends IHistoryLoadingTask<Config, DbValue>,
            Config extends BackgroundLoadingHistoricalEditorConfig,
            DbValue,
            Stub extends io.grpc.stub.AbstractStub<Stub>>
        extends AbstractCachingHistoricalEditor<HistoryData, HistoryLoadingTask, Config, DbValue, Stub> {
    boolean isRunning = false;

    /**
     * Construct the instance.
     *
     * @param config per-type configuration
     * @param statsHistoryClient history db client
     * @param historyLoadingTaskCreator create an instance of a db value loading task
     * @param historyDataCreator create an instance of cached history element
     */
    protected AbstractBackgroundLoadingHistoricalEditor(Config config, Stub statsHistoryClient,
                    Function<Stub, HistoryLoadingTask> historyLoadingTaskCreator,
                    Supplier<HistoryData> historyDataCreator) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, historyDataCreator);
    }

    @Override
    public void initContext(@Nonnull GraphWithSettings graph,
                            @Nonnull ICommodityFieldAccessor accessor,
                            @Nonnull List<EntityCommodityReference> eligibleComms,
                            boolean isPlan)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(graph, accessor, eligibleComms, isPlan);

        // determine once per broadcast processing at this point
        isRunning = isRunning();

        if (isRunning || eligibleComms.size() > getConfig().getBackgroundLoadThreshold()) {
            // TODO call super.createPreparationTasks() and submit to local executor
            // TODO submit only those that have not been submitted yet by previous broadcasts
            isRunning = true;
        }
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull List<EntityCommodityReference> commodityRefs) {
        if (isRunning) {
            return Collections.emptyList();
        } else {
            // synchronously load the small portion of commodities during this broadcast
            return super.createPreparationTasks(commodityRefs);
        }
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<Void>>>
           createCalculationTasks(@Nonnull List<EntityCommodityReference> commodityRefs) {
        if (isRunning) {
            // return no data in the broadcast
            // TODO log warning
            return Collections.emptyList();
        } else {
            return super.createCalculationTasks(commodityRefs);
        }
    }

    private boolean isRunning() {
        // TODO implement depending on executor state - false if no submitted or running or failed tasks
        return false;
    }
}
