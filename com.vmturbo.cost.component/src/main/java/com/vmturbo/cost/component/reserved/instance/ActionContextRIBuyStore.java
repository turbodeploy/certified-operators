package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.common.protobuf.utils.StringConstants.CREATE_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.DATA;
import static com.vmturbo.common.protobuf.utils.StringConstants.TEMPLATE_TYPE;
import static com.vmturbo.cost.component.db.Tables.ACTION_CONTEXT_RI_BUY;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.db.tables.records.ActionContextRiBuyRecord;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisRecommendation;

/**
 * The function of this class is to perform database operation on the cost.action_context_ri_buy table.
 */
public class ActionContextRIBuyStore {

    private static final Logger logger = LogManager.getLogger();

    private static final int NUM_HOURS_IN_A_DAY = 24;

    private static final int NUM_DAYS_IN_A_WEEK = 7;

    private static final int NUM_HOURS_IN_A_WEEK = NUM_HOURS_IN_A_DAY * NUM_DAYS_IN_A_WEEK;

    private final DSLContext dsl;

    public ActionContextRIBuyStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Performs the insert.
     * @param actionToRecommendationMapping mapping from action to RI buy recommendations.
     */
    public void insertIntoActionContextRIBuy(@Nonnull final Map<ActionDTO.Action, ReservedInstanceAnalysisRecommendation>
                                                     actionToRecommendationMapping, final long topologyContextId) {
        List<ActionContextRiBuyRecord> actionContextRiBuyRecords =
                createActionContextRIBuys(actionToRecommendationMapping, topologyContextId);
        dsl.batchInsert(actionContextRiBuyRecords).execute();
    }

    /**
     * Inserts the {@code instanceDemandLIst} into the table.
     * @param instanceDemandList The list of {@link RIBuyInstanceDemand} records to insert.
     */
    public void insertRIBuyInstanceDemand(@Nonnull List<RIBuyInstanceDemand> instanceDemandList) {

        Preconditions.checkNotNull(instanceDemandList);

        final List<ActionContextRiBuyRecord> actionContextRIBuyRecords =
                createActionContextRIBuys(instanceDemandList);

        dsl.batchInsert(actionContextRIBuyRecords).execute();
    }

    /**
     * Get the IDs of all topology contexts that have data.
     *
     * @return The set of topology context ids.
     */
    @Nonnull
    public Set<Long> getContextsWithData() {
        return dsl.selectDistinct(ACTION_CONTEXT_RI_BUY.PLAN_ID)
            .from(ACTION_CONTEXT_RI_BUY)
            .fetch().stream()
            .map(Record1::value1)
            .collect(Collectors.toSet());
    }

    /**
     * Creates a list of ActionContextRiBuyRecord records.
     * @param actionToRecommendationMapping mapping from action to RI buy recommendations.
     */
    private List<ActionContextRiBuyRecord> createActionContextRIBuys(final Map<ActionDTO.Action, ReservedInstanceAnalysisRecommendation>
                                                                            actionToRecommendationMapping, long topologyContextId) {
        final List<ActionContextRiBuyRecord> actionContextRiBuyRecords = new ArrayList<>();
        for (ActionDTO.Action action : actionToRecommendationMapping.keySet()) {
            final ReservedInstanceAnalysisRecommendation recommendation = actionToRecommendationMapping.get(action);
            LocalDateTime now = LocalDateTime.now();
            Calendar cal = Calendar.getInstance();
            int actionHour = ((cal.get(Calendar.DAY_OF_WEEK) - 1) * NUM_HOURS_IN_A_DAY)
                    + cal.get(Calendar.HOUR_OF_DAY);

            for (Entry<TopologyEntityDTO, float[]> e : recommendation.getTemplateTypeHourlyDemand().entrySet()) {
                float[] weeklyDemand = e.getValue();
                TopologyEntityDTO topologyEntityDTO = e.getKey();
                final ActionContextRiBuyRecord actionContextRiBuyRecord = dsl.newRecord(ACTION_CONTEXT_RI_BUY);
                actionContextRiBuyRecord.setActionId(action.getId());
                actionContextRiBuyRecord.setPlanId(topologyContextId);
                actionContextRiBuyRecord.setCreateTime(now);
                actionContextRiBuyRecord.setDemandType(DemandType.TYPICAL_DEMAND.ordinal());
                actionContextRiBuyRecord.setTemplateType(topologyEntityDTO.getDisplayName());
                actionContextRiBuyRecord.setTemplateFamily(topologyEntityDTO.getTypeSpecificInfo()
                        .getComputeTier().getFamily());
                // The old RI buy analysis always has 1 hour demand intervals
                actionContextRiBuyRecord.setDatapointInterval(Duration.ofHours(1).toString());
                List<Float> weeklyDemandList = new ArrayList<>();
                for (float f : weeklyDemand) {
                    weeklyDemandList.add(f);
                }
                // Rotates the weekly demand to align with the recommended time for displaying the
                // demand on the UI.
                Collections.rotate(weeklyDemandList, NUM_HOURS_IN_A_WEEK - actionHour - 1);
                final String demand = weeklyDemandList.toString()
                        .substring(1, weeklyDemandList.toString().length() - 1);
                actionContextRiBuyRecord.setData(demand.getBytes());
                actionContextRiBuyRecords.add(actionContextRiBuyRecord);
            }
        }
        return actionContextRiBuyRecords;
    }

    private List<ActionContextRiBuyRecord> createActionContextRIBuys(@Nonnull List<RIBuyInstanceDemand> instanceDemandList) {

        final ImmutableList.Builder recordsList = ImmutableList.builder();

        for (RIBuyInstanceDemand instanceDemand : instanceDemandList) {

            final ActionContextRiBuyRecord actionContextRiBuyRecord = dsl.newRecord(ACTION_CONTEXT_RI_BUY);

            actionContextRiBuyRecord.setActionId(instanceDemand.actionId());
            actionContextRiBuyRecord.setPlanId(instanceDemand.topologyContextId());
            actionContextRiBuyRecord.setCreateTime(instanceDemand.lastDatapointTime());
            actionContextRiBuyRecord.setDemandType(instanceDemand.demandType().ordinal());
            actionContextRiBuyRecord.setDatapointInterval(instanceDemand.datapointInterval().toString());
            actionContextRiBuyRecord.setTemplateType(instanceDemand.instanceType());

            final String demand = instanceDemand.datapoints().stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            actionContextRiBuyRecord.setData(demand.getBytes());

            recordsList.add(actionContextRiBuyRecord);
        }

        return recordsList.build();
    }

    /**
     * Retrieves records from ACTION_CONTEXT_RI_BUY table for a given action ID.
     * @param actionId the action ID for which we want to obtain the records from.
     * @return List<ActionContextRiBuyRecord>.
     */
    public List<RIBuyInstanceDemand> getRecordsFromActionContextRiBuy(String actionId) {
        return dsl.selectFrom(ACTION_CONTEXT_RI_BUY)
                .where(ACTION_CONTEXT_RI_BUY.ACTION_ID.eq(Long.parseLong(actionId)))
                .fetchStream()
                .map(this::convertRecordToInstanceDemand)
                .collect(ImmutableList.toImmutableList());
    }

    private RIBuyInstanceDemand convertRecordToInstanceDemand(@Nonnull ActionContextRiBuyRecord record) {
        return RIBuyInstanceDemand.builder()
                .actionId(record.getActionId())
                .topologyContextId(record.getPlanId())
                .demandType(DemandType.fromInteger(record.getDemandType()))
                .datapointInterval(Duration.parse(record.getDatapointInterval()))
                .instanceType(record.getTemplateType())
                .lastDatapointTime(record.getCreateTime())
                .datapoints(Stream.of(new String(record.getData()).split(","))
                        .map(String::trim)
                        .map(Float::parseFloat)
                        .collect(ImmutableList.toImmutableList()))
                .build();
    }

    /**
     * Get historical context data for a given RI Buy action.
     * @param actionId the action id.
     * @return List of Stats.StatSnapshot.
     */
    public List<Stats.StatSnapshot> getHistoricalContextForRIBuyAction(String actionId) {

        // needs to return List<EntitySnapshot> like in classic
        if (actionId == null) {
            return Collections.emptyList();
        }

        // run query
        final List<RIBuyInstanceDemand> instanceDemandList = getRecordsFromActionContextRiBuy(actionId);

        final ImmutableList.Builder<StatSnapshot> statsList = ImmutableList.builder();

        for (RIBuyInstanceDemand instanceDemand : instanceDemandList) {

            final Duration dataTimeSpan = instanceDemand.datapointInterval()
                    .multipliedBy(instanceDemand.datapoints().size() - 1);
            ZonedDateTime currentTime = instanceDemand.lastDatapointTime().atZone(ZoneId.systemDefault())
                    .minus(dataTimeSpan);

            for (Float demandDatapoint : instanceDemand.datapoints()) {

                final Stats.StatSnapshot.StatRecord.StatValue demandValStat = Stats.StatSnapshot.StatRecord.StatValue
                        .newBuilder()
                        .setAvg(demandDatapoint)
                        .build();

                final Stats.StatSnapshot.StatRecord statRecord = Stats.StatSnapshot.StatRecord.newBuilder()
                        .setValues(demandValStat)
                        .setStatKey(instanceDemand.instanceType())
                        .setUnits(instanceDemand.demandType().toString())
                        .build();

                final Stats.StatSnapshot stat = Stats.StatSnapshot.newBuilder()
                        .setSnapshotDate(currentTime.toInstant().toEpochMilli())
                        .addStatRecords(statRecord)
                        .build();

                statsList.add(stat);
                currentTime = currentTime.plus(instanceDemand.datapointInterval());

            }
        }

        return statsList.build();
    }

    /**
     * Delete all entries from the action context ri buy table for a topologyContextId.
     * @param topologyContextId The topologyContextId for which we want to delete all entries from
     *                          action context ri buy table.
     */
    public int deleteRIBuyContextData(Long topologyContextId) {
        logger.info("Deleting data from action context for topologyContextId : " + topologyContextId);
        int rowsDeleted = dsl.deleteFrom(ACTION_CONTEXT_RI_BUY).where(ACTION_CONTEXT_RI_BUY.PLAN_ID
                .eq(topologyContextId)).execute();
        return rowsDeleted;
    }

    /**
     * An immmutable implemenation of {@link ActionContextRiBuyRecord}.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface RIBuyInstanceDemand {

        /**
         * The associated action ID.
         * @return The associated action ID.
         */
        long actionId();

        /**
         * The associated topology context ID.
         * @return The associated topology context ID.
         */
        long topologyContextId();

        /**
         * The demand type.
         * @return The demand type.
         */
        @Nonnull
        DemandType demandType();

        /**
         * The timestamp for the last data point in {@link #datapoints()}.
         * @return The timestamp for the last datapoint in {@link #datapoints()}.
         */
        @Nonnull
        LocalDateTime lastDatapointTime();

        /**
         * The time interval between data points in {@link #datapoints()}.
         * @return The time interval between data points in {@link #datapoints()}.
         */
        @Default
        @Nonnull
        default Duration datapointInterval() {
            return Duration.ofHours(1);
        }

        /**
         * The instance type of the demand.
         * @return The instance type of the demand.
         */
        @Nonnull
        String instanceType();

        /**
         * The list of demand data points.
         * @return The list of demand data points.
         */
        @Nonnull
        List<Float> datapoints();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link RIBuyInstanceDemand} instances.
         */
        class Builder extends ImmutableRIBuyInstanceDemand.Builder {}
    }

    /**
     * The types of demand stored within this store.
     */
    public enum DemandType {

        /**
         * Typical demand represents the weighted average demand collected for RI buy 1.0.
         */
        TYPICAL_DEMAND,
        /**
         * Observed demand corresponds to the direct recorded demand at the associated time (i.e. it
         * is an approximation of the billed usage). Observed demand is used by CCA.
         */
        OBSERVED_DEMAND;

        /**
         * Converts {@code value} to {@link DemandType}.
         * @param value The value to convert.
         * @return The {@link DemandType} associated with {@code value} or null, if no demand type
         * is associated with the value.
         */
        public static DemandType fromInteger(int value) {
            switch(value) {
                case 0:
                    return TYPICAL_DEMAND;
                case 1:
                    return OBSERVED_DEMAND;
            }
            return null;
        }
    }
}
