package com.vmturbo.ml.datastore.influx;

import com.google.common.annotations.VisibleForTesting;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist.ActionType;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Writes action-related metrics to influx. Writes are batched according to the batch size configured
 * in the influx connection.
 *
 * See https://vmturbo.atlassian.net/wiki/spaces/Home/pages/827850762/Metron+data+collection#Metrondatacollection-Theschema
 * for a high-level description of the influx data schema used for metrics in influx.
 *
 * Based on the type of action we would have multiple number of fields written into influx.
 * ie, for a CompoundMove, we would have a field for every constituent move
 * while for a deactivate/activate we would have fields for every triggering commodity.
 *
 */
public class InfluxActionsWriter extends InfluxMetricsWriter implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger();

    static final String ACTION_TYPE = "action_type";
    static final String EXECUTABLE = "executable";
    static final String ACTION_ID = "action_id";
    static final String TARGET = "target";
    static final String SOURCE = "source";
    static final String DESTINATION = "destination";
    static final String PROVISIONED_SELLER = "provisioned_seller";

    @VisibleForTesting
    InfluxActionsWriter(@Nonnull final InfluxDB influxConnection,
                        @Nonnull final String database,
                        @Nonnull final String retentionPolicy,
                        @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist) {
        super(influxConnection, database, retentionPolicy, metricsStoreWhitelist);
    }

    /**
     * Write topology metrics to influx
     *
     * @param actionPlan all actions
     * @param actionStatistics A map of names of metrics to how many statistics of that type were written.
     *                         This map will be mutated by this method as it writes additional metrics to
     *                         count metrics written. This is the map for commodity bought statistics.
     *
     * @return The number of data points written.
     */
    public int writeMetrics(@Nonnull final ActionPlan actionPlan,
                                    @Nonnull final Map<String, Long> actionStatistics,
                                    @Nonnull final ActionState actionState) {
        Map<InfluxStatType, Map<String, Long>> stats = new HashMap<>();
        stats.put(InfluxStatType.ACTIONS, actionStatistics);
        final WriterContext context = new WriterContext(influxConnection,
            stats,
            metricsStoreWhitelist,
            actionPlan.getAnalysisCompleteTimestamp());

        int numWritten = 0;
        for (Action action : actionPlan.getActionList()) {
            numWritten += writeActionMetrics(action, context, actionState);
        }
        return numWritten;
    }

    /**
     * Write metrics for a given {@link com.vmturbo.common.protobuf.action.ActionDTO.Action} to influx.
     * Use the given {@link WriterContext} to do the writing and to track statistics about the data points
     * and fields written.
     *
     * @param action The action that is to be written
     * @param context The {@link WriterContext} to be used to write to influx and track
     *                metric statistics.
     * @param actionState tells weather this is a RECOMMENDED or COMPLETED action
     * @return 1 if the action was written to influx and 0 otherwise.
     *
     */
    private int writeActionMetrics(@Nonnull final Action action,
                                    @Nonnull final WriterContext context,
                                    @Nonnull final ActionState actionState) {
        final ActionTypeCase actionType = action.getInfo().getActionTypeCase();
        String actionTypeString = actionType.name().toLowerCase();
        Point.Builder point = null;
        MetricWriter metricWriter;
        // check if the action needs to be saved in influx
        if (!whitelisted(ActionType.valueOf(actionType.toString()), context)
                        || !whitelisted(actionState, context)) {
            return 0;
        }
        switch (actionType) {
            case MOVE:
                Move move = action.getInfo().getMove();
                // name - recommended/ successful
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        // tag for actionType
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(move.getTarget().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                // there can be multiple moves across the same entityType (multiple DS moves for the same target VM)
                // eg, VM moves from (H1, DS1, DS1) -> (H2, DS2, DS2). We need to mark source and dest of a move with a suffix
                int count = 0;
                for (ChangeProvider cp : move.getChangesList()) {
                    // start action has no source
                    if (cp.hasSource()) {
                        metricWriter.field(SOURCE + "_" + count, Long.toString(cp.getSource().getId()));
                    }
                    metricWriter.field(DESTINATION + "_" + count, Long.toString(cp.getDestination().getId()));
                    count++;
                }
                return metricWriter.write();

            case RESIZE:
                Resize resize = action.getInfo().getResize();
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(resize.getTarget().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                CommodityType commodity = CommodityType.forNumber(resize.getCommodityType().getType());
                // OLD_CPUTypeId_CAPACITY
                metricWriter.field("old_"
                        + commodity.name() + "_"
                        + resize.getCommodityAttribute().name().toLowerCase(), Float.toString(resize.getOldCapacity()));
                metricWriter.field("new_"
                        + commodity.name() + "_"
                        + resize.getCommodityAttribute().name().toLowerCase(), Float.toString(resize.getNewCapacity()));
                return metricWriter.write();

            case DEACTIVATE:
                Deactivate deactivate = action.getInfo().getDeactivate();
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(deactivate.getTarget().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                return metricWriter.write();

            case PROVISION:
                Provision provision = action.getInfo().getProvision();
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(provision.getEntityToClone().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                metricWriter.field(PROVISIONED_SELLER, Long.toString(provision.getProvisionedSeller()));
                return metricWriter.write();

            case ACTIVATE:
                Activate activate = action.getInfo().getActivate();
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(activate.getTarget().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                return metricWriter.write();
            case RECONFIGURE:
                ActionDTO.Reconfigure reconfigure = action.getInfo().getReconfigure();
                point = Point.measurement(actionState.name().toLowerCase())
                        .time(context.timeMs, TimeUnit.MILLISECONDS)
                        .tag(ACTION_TYPE, actionTypeString)
                        .tag(TARGET, Long.toString(reconfigure.getTarget().getId()))
                        .tag(EXECUTABLE, action.getExecutable() ? "true" : "false");
                metricWriter = new MetricWriter(context,
                        point, InfluxStatType.ACTIONS);

                metricWriter.field(ACTION_ID, Long.toString(action.getId()));
                metricWriter.field(SOURCE, Long.toString(reconfigure.getSource().getId()));
                return metricWriter.write();
            default:
                break;
        }
        return 0;
    }

    /**
     * Write metrics for a given {@link ActionSuccess} notification to influx.
     * Use the given {@link WriterContext} to do the writing and to track statistics about the data points
     * and fields written.
     *
     * @param action The action that is to be written
     * @return 1 if the action was written to influx and 0 otherwise.
     *
     */
    public int writeCompletedActionMetrics(@Nonnull final ActionSuccess action) {
        Map<InfluxStatType, Map<String, Long>> stats = new HashMap<>();
        stats.put(InfluxStatType.ACTIONS, new HashMap<>());
        // since we dont have the timestapm when the action succeeded,
        // save current system time in actionContext
        final WriterContext context = new WriterContext(influxConnection,
                stats,
                metricsStoreWhitelist,
                System.currentTimeMillis());
        // check if we have whitelisted completed actions before logging them
        if (!whitelisted(ActionState.COMPLETED_ACTIONS, context)) {
            return 0;
        }
        Point.Builder point = Point.measurement(ActionState.COMPLETED_ACTIONS.toString().toLowerCase())
                .time(context.timeMs, TimeUnit.MILLISECONDS)
                .tag(ACTION_ID, Long.toString(action.getActionId()));
        MetricWriter writer = new MetricWriter(context,
                point, InfluxStatType.ACTIONS);
        // point with no field cannot be written to influx.
        // hence setting field to value same as the tag
        writer.field(ACTION_ID, Long.toString(action.getActionId()));
        return writer.write();
    }

    /**
     * Determine if a particular {@link ActionType} has been whitelisted to be written to influx.
     * If not, the action will not be written.
     *
     * @param actionType type of the action to check.
     * @return Whether the action type should be written to influx.
     */
    private boolean whitelisted(@Nonnull final ActionType actionType,
                               @Nonnull final WriterContext writerContext) {
        return writerContext.actionTypesWhitelist.contains(actionType);
    }

    /**
     * Determine if a particular {@link ActionState} has been whitelisted to be written to influx.
     * If not, the action will not be written.
     *
     * @param actionState state of the action to check.
     * @return Whether the action type should be written to influx.
     */
    private boolean whitelisted(@Nonnull final ActionState actionState,
                               @Nonnull final WriterContext writerContext) {
        return writerContext.actionStateWhitelist.contains(actionState);
    }
}
