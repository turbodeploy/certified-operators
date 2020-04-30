package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.common.protobuf.utils.StringConstants.CREATE_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.DATA;
import static com.vmturbo.common.protobuf.utils.StringConstants.TEMPLATE_TYPE;
import static com.vmturbo.cost.component.db.Tables.ACTION_CONTEXT_RI_BUY;

import java.sql.Timestamp;
import java.time.LocalDateTime;
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

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.stats.Stats;
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
    public List<ActionContextRiBuyRecord> createActionContextRIBuys(final Map<ActionDTO.Action, ReservedInstanceAnalysisRecommendation>
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
                actionContextRiBuyRecord.setTemplateType(topologyEntityDTO.getDisplayName());
                actionContextRiBuyRecord.setTemplateFamily(topologyEntityDTO.getTypeSpecificInfo()
                        .getComputeTier().getFamily());
                List<Float> weeklyDemandList = new ArrayList<>();
                for (float f : weeklyDemand) {
                    weeklyDemandList.add(f);
                }
                // Rotates the weekly demand to align with the recommended time.
                Collections.rotate(weeklyDemandList, NUM_HOURS_IN_A_WEEK - actionHour);
                final String demand = weeklyDemandList.toString()
                        .substring(1, weeklyDemandList.toString().length() - 1);
                actionContextRiBuyRecord.setData(demand);
                actionContextRiBuyRecords.add(actionContextRiBuyRecord);
            }
        }
        return actionContextRiBuyRecords;
    }

    /**
     * Retrieves records from ACTION_CONTEXT_RI_BUY table for a given action ID.
     * @param actionId the action ID for which we want to obtain the records from.
     * @return List<ActionContextRiBuyRecord>.
     */
    public List<ActionContextRiBuyRecord> getRecordsFromActionContextRiBuy(String actionId) {
        return dsl.selectFrom(ACTION_CONTEXT_RI_BUY)
                .where(ACTION_CONTEXT_RI_BUY.ACTION_ID.eq(Long.parseLong(actionId))).fetch();
    }

    /**
     * Get historical context data for a given RI Buy action.
     * @param actionId the action id.
     * @return List of Stats.StatSnapshot.
     */
    public List<Stats.StatSnapshot> getHistoricalContextForRIBuyAction(String actionId) {
        // needs to return List<EntitySnapshot> like in classic
        if (actionId == null) {
            return new ArrayList<>();
        }
        // run query
        List<ActionContextRiBuyRecord> records = getRecordsFromActionContextRiBuy(actionId);

        final Map<String, Stats.StatSnapshot> result = Maps.newLinkedHashMap();
        for (Record record : records) {
            // set properties in proto
            final Long currentTime = record.getValue(CREATE_TIME, Timestamp.class).getTime();
            List<String> demandData = new ArrayList<>(Arrays.asList((record.getValue(DATA, String.class))
                    .split("\\s*,\\s*")));
            String templateType = record.getValue(TEMPLATE_TYPE, String.class);
            List<Stats.StatSnapshot.StatRecord> allRecords = new ArrayList<>();
            for (String demandPoint : demandData) {
                Float demandValue = Float.parseFloat(demandPoint);
                // create statValue for demand values
                Stats.StatSnapshot.StatRecord.StatValue demandValStat = Stats.StatSnapshot.StatRecord.StatValue
                        .newBuilder()
                        .setAvg(demandValue)
                        .build();

                final Stats.StatSnapshot.StatRecord statRecord = Stats.StatSnapshot.StatRecord.newBuilder()
                        .setValues(demandValStat)
                        .setStatKey(templateType)
                        .build();
                allRecords.add(statRecord);
            }
            Stats.StatSnapshot stat = Stats.StatSnapshot.newBuilder()
                    .addAllStatRecords(allRecords).setSnapshotDate(currentTime).build();
            result.put(templateType, stat);
        }
        return new ArrayList<>(result.values());
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
}
