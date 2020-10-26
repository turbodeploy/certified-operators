package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;

/**
 * The results of the reserved instance recommendation algorithms: a set of recommended actions, plus
 * some contextual information about the analysis done.
 */
public class ReservedInstanceAnalysisResult {

    private static final Logger logger = LogManager.getLogger();

    // Buy RI store
    private final BuyReservedInstanceStore buyRiStore;

    private final ActionContextRIBuyStore actionContextRIBuyStore;

    /**
     * This class describes the context of the recommended actions, to aid in understanding the
     * recommendations (eg, when was the analysis run, with what input configuration, etc).
     * It is grouped together for easy serialization.
     */
    public class Manifest {
        // The timestamp when the analysis was run.
        private final long analysisStartTime;

        // The timestamp when the analysis was run.
        private final long analysisCompletionTime;

        // The scope of the analysis, eg which platforms, regions, tenancies, etc. were considered.
        private final ReservedInstanceAnalysisScope analysisScope;

        // The  constraints specifying what kind of reservations may be purchased, eg
        // the user wants standard 1-year all up front reservations. The constraints are
        // mapped to the service provider display name.
        private final Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints;

        // Id of the topology on which the analysis was run.
        private final long topologyContextId;

        // A count of the number of separate contexts that were analyzed -- separate
        // combinations of region, tenancy, platform, and instance type or family
        // (depending on whether instance size flexible rules applied).
        private final int contextsAnalyzed;

        public Manifest(long analysisStartTime,
                        long analysisCompletionTime,
                        @Nonnull ReservedInstanceAnalysisScope analysisScope,
                        @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints,
                        long topologyContextId,
                        int contextsAnalyzed) {
            this.analysisStartTime = analysisStartTime;
            this.analysisCompletionTime = analysisCompletionTime;
            this.analysisScope = Objects.requireNonNull(analysisScope);
            this.purchaseConstraints = Objects.requireNonNull(purchaseConstraints);
            this.topologyContextId = topologyContextId;
            this.contextsAnalyzed = contextsAnalyzed;
        }

        public long getAnalysisStartTime() {
            return analysisStartTime;
        }

        public long getAnalysisCompletionTime() {
            return analysisCompletionTime;
        }

        @Nonnull
        public ReservedInstanceAnalysisScope getAnalysisScope() {
            return analysisScope;
        }

        @Nonnull
        public Map<String, ReservedInstancePurchaseConstraints> getPurchaseConstraints() {
            return purchaseConstraints;
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }

        public int getContextsAnalyzed() { return contextsAnalyzed; }
    }

    // The manifest data describing the context of the analysis that was run.
    private final Manifest manifest;

    // The resulting recommended actions, eg buy various kinds of reserved instances.
    private final ImmutableList<ReservedInstanceAnalysisRecommendation> recommendations;

    public ReservedInstanceAnalysisResult(@Nonnull ReservedInstanceAnalysisScope analysisScope,
                          @Nonnull Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints,
                          @Nonnull List<ReservedInstanceAnalysisRecommendation> recommendations,
                          long topologyId,
                          long analysisStartTime,
                          long analysisCompletionTime,
                          int contextsAnalyzed,
                          @Nonnull BuyReservedInstanceStore buyRiStore,
                          @Nonnull ActionContextRIBuyStore actionContextRIBuyStore) {

        Objects.requireNonNull(analysisScope);
        Objects.requireNonNull(purchaseConstraints);
        Objects.requireNonNull(recommendations);
        this.manifest = new Manifest(analysisStartTime, analysisCompletionTime, analysisScope,
                purchaseConstraints, topologyId, contextsAnalyzed);
        this.recommendations = ImmutableList.copyOf(recommendations);
        this.buyRiStore = buyRiStore;
        this.actionContextRIBuyStore = actionContextRIBuyStore;
    }

    @Nonnull
    public Manifest getManifest() {
        return manifest;
    }

    @Nonnull
    public ImmutableList<ReservedInstanceAnalysisRecommendation> getRecommendations() {
        return recommendations;
    }

    /**
     * Write the recommendations to a stream in CSV format.
     *
     * @param stream the stream to print the recommendations to.
     */
    public void writeRecommendations(PrintStream stream) {
        stream.println(ReservedInstanceAnalysisRecommendation.getCSVHeader());

        for (ReservedInstanceAnalysisRecommendation recommendation : recommendations) {
            stream.println(recommendation.toCSVString());
        }
    }

    /**
     * Write the recommendations to a named file in CSV format.
     *
     * @param filename the name of the file to be written.
     */
    public void writeRecommendations(String filename) throws FileNotFoundException {
        FileOutputStream fileStream = new FileOutputStream(filename);
        PrintStream printStream = new PrintStream(fileStream);

        writeRecommendations(printStream);

        printStream.close();
    }

    /**
     * Convert this result to a string.
     *
     * @return A CSV representation of the recommendations.
     */
    @Override
    @Nonnull
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        writeRecommendations(ps);
        String csv = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        ps.close();

        return csv;
    }

    /**
     * Creates ActionItem from the RI recommendations.
     */
    public ActionPlan createActionPlan() {

        List<Action> actions = new ArrayList<>();
        Map<Action, ReservedInstanceAnalysisRecommendation> actionToRecommendationMapping = new HashMap<>();
        for (ReservedInstanceAnalysisRecommendation recommendation : recommendations) {
            final Action action = recommendation.createAction();
            actionToRecommendationMapping.put(action, recommendation);
            actions.add(action);
        }

        insertActionsIntoContextTable(actionToRecommendationMapping);

        return ActionPlan.newBuilder()
            .setId(IdentityGenerator.next())
            .setAnalysisStartTimestamp(manifest.getAnalysisStartTime())
            .setAnalysisCompleteTimestamp(manifest.getAnalysisCompletionTime())
            .setInfo(ActionPlanInfo.newBuilder()
                .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                    .setTopologyContextId(manifest.getTopologyContextId())))
            .addAllAction(actions)
            .build();
    }

    /**
     * Creates RI Bought from the RI recommendations.
     * Also insert RI Buy recommendations into action_context_ri_buy table.
     */
    public void persistResults() {

        final Set<ReservedInstanceData> recommendationDataSet = recommendations.stream()
                .map(ReservedInstanceAnalysisRecommendation::asReservedInstanceData)
                .collect(ImmutableSet.toImmutableSet());

        buyRiStore.updateBuyReservedInstances(recommendationDataSet, manifest.getTopologyContextId());

    }

    /**
     * Insert actions into action context ri buy table
     * @param actionToRecommendationMapping
     */
    public void insertActionsIntoContextTable(Map<Action, ReservedInstanceAnalysisRecommendation> actionToRecommendationMapping) {
        actionContextRIBuyStore.insertIntoActionContextRIBuy(actionToRecommendationMapping,
                manifest.getTopologyContextId());
    }
}
