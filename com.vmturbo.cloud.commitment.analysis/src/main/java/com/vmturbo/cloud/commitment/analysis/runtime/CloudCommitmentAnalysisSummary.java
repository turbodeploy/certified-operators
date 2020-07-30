package com.vmturbo.cloud.commitment.analysis.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.Status;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.Status.State;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * Class representing the Cloud commitment analysis summary.
 */
public class CloudCommitmentAnalysisSummary {

    /**
     * The template used translate the analysis summary to a string.
     */
    private static final String ANALYSIS_SUMMARY_TEMPLATE =
            "======== Pipeline Summary ========\n"
                    + "Analysis ID: <analysisId>\n"
                    + "Analysis Tag: <analysisTag>\n"
                    + "Creation Time: <creationTime>\n"
                    + "State: <state>\n"
                    + "Status Message: <statusMessage>\n"
                    + "Current Stages: <currentStages;separator=\", \">\n"
                    + "Analysis Started: <startTime>\n"
                    + "Analysis Completed: <endTime>\n"
                    + "Analysis Duration: <duration>\n"
                    + "======== Stage Breakdown ========\n"
                    + "<stages;separator=\"------------------------------\\n\">"
                    + "=================================\n";

    private Instant startTime = null;

    private Instant endTime = null;

    private Status status = Status.ready();

    // A linked hash map is used to maintain the insertion order of the stages
    private final Map<Long, StageSummary> stageSummaryMap = Collections.synchronizedMap(Maps.newLinkedHashMap());

    private final CloudCommitmentAnalysisInfo ccaInfo;

    private final CloudCommitmentAnalysisConfig analysisConfig;

    /**
     * Constructor for the CloudCommitmentAnalysisSummary.
     *
     * @param ccaInfo The cloud commitment analysis info.
     * @param analysisConfig The analysis configuration.
     * @param analysisPipeline The analysis pipeline.
     */
    public CloudCommitmentAnalysisSummary(@Nonnull CloudCommitmentAnalysisInfo ccaInfo,
                                          @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                                          @Nonnull AnalysisPipeline analysisPipeline) {

        this.ccaInfo = Objects.requireNonNull(ccaInfo);
        this.analysisConfig = Objects.requireNonNull(analysisConfig);

        Objects.requireNonNull(analysisPipeline).stages()
                .forEach(s -> stageSummaryMap.put(s.id(), StageSummary.newSummary(s)));
    }

    /**
     * Workflow for when an analysis stage starts.
     *
     * @param stage The stage.
     */
    public synchronized void onStageStart(@Nonnull AnalysisStage stage) {

        if (stageSummaryMap.containsKey(stage.id())) {

            if (status.state() == State.READY) {
                startTime = Instant.now();
                status = Status.running();
            } else if (status.state() != State.RUNNING) {
                throw new IllegalStateException();
            }

            final StageSummary stageSummary = stageSummaryMap.get(stage.id());
            stageSummary.onStart();
        } else {
            throw new IllegalArgumentException();
        }

    }

    /**
     * Workflow for stage completion.
     *
     * @param stage The stage.
     * @param stageResult The stage result.
     */
    public synchronized void onStageCompletion(@Nonnull AnalysisStage stage,
                                               @Nonnull AnalysisStage.StageResult<?> stageResult) {

        if (stageSummaryMap.containsKey(stage.id())) {

            if (status.state() == State.RUNNING) {

                final StageSummary stageSummary = stageSummaryMap.get(stage.id());
                stageSummary.onCompletion(stageResult);

                final boolean isAnalysisComplete = stageSummaryMap.values()
                        .stream()
                        .allMatch(StageSummary::isComplete);

                if (isAnalysisComplete) {
                    endTime = Instant.now();
                    status = Status.complete("");
                }

            } else {
                throw new IllegalStateException();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Workflow for stage failure.
     *
     * @param stage The stage.
     * @param failureMessage The failure message
     */
    public synchronized void onStageFailure(@Nonnull AnalysisStage stage,
                                            @Nonnull String failureMessage) {

        if (stageSummaryMap.containsKey(stage.id())) {

            if (status.state() == State.RUNNING) {

                final StageSummary stageSummary = stageSummaryMap.get(stage.id());
                stageSummary.onFailure(failureMessage);

                endTime = Instant.now();
                status = Status.fail(failureMessage);

            } else {
                throw new IllegalStateException();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public synchronized String toString() {
        final ST template = new ST(ANALYSIS_SUMMARY_TEMPLATE);

        template.add("analysisId", ccaInfo.getOid());
        template.add("analysisTag", ccaInfo.getAnalysisTag());
        template.add("creationTime", Instant.ofEpochMilli(ccaInfo.getCreationTime()));

        final List<String> currentStages = stageSummaryMap.values().stream()
                .filter(StageSummary::isRunning)
                .map(StageSummary::stageName)
                .collect(Collectors.toList());
        template.add("state", status.state());
        template.add("statusMessage", status.message());
        template.add("currentStages", currentStages);

        final boolean hasStarted = status.state() != State.READY;
        final boolean hasFinished = status.state() == State.COMPLETED || status.state() == State.FAILED;
        template.add("startTime", hasStarted ? startTime : "Not Started");
        template.add("endTime", hasStarted ? (hasFinished ? endTime : "Still Running") : "Not Started");
        // If the analysis has completed, duration is simply the time between start and end. If the
        // analysis has started and is still running, the duration will represent the time from
        // start to now.
        template.add("duration", hasStarted
                ? Duration.between(startTime, hasFinished ? endTime : Instant.now()) : "Not Available");

        template.add("stages", stageSummaryMap.values());

        return template.render();
    }

    /**
     * Static class representing the stage summary.
     */
    private static class StageSummary {

        private static final String STAGE_SUMMARY_TEMPLATE =
                "<stageName> -------------- <state>\n"
                        + "    Start Time: <startTime>\n"
                        + "    End Time: <endTime>\n"
                        + "    Duration: <duration>\n"
                        + "    Results Summary:\n"
                        + "        <resultsSummary>\n";

        @Nonnull
        private final AnalysisStage stage;

        @Nullable
        private Instant startTime = null;

        @Nullable
        private Instant endTime = null;

        @Nullable
        private AnalysisStage.StageResult stageResult = null;

        @Nonnull
        private CloudCommitmentAnalysis.Status status = Status.ready();


        StageSummary(@Nonnull AnalysisStage stage) {
            this.stage = Objects.requireNonNull(stage);
        }

        public static StageSummary newSummary(@Nonnull AnalysisStage stage) {
            return new StageSummary(stage);
        }

        public boolean isRunning() {
            return status.state() == State.RUNNING;
        }

        public boolean isComplete() {
            return status.state() == State.COMPLETED;
        }

        @Nonnull
        public String stageName() {
            return stage.stageName();
        }

        public void onStart() {
            if (status.state() == State.READY) {
                startTime = Instant.now();
                status = Status.running();
            } else {
                throw new IllegalStateException();
            }
        }

        public void onCompletion(@Nonnull AnalysisStage.StageResult<?> stageResult) {
            if (status.state() == State.RUNNING) {
                endTime = Instant.now();
                this.stageResult = stageResult;

                status = Status.complete(stageResult.resultSummary());
            } else {
                throw new IllegalStateException();
            }
        }

        public void onFailure(@Nonnull String message) {
            if (status.state() == State.RUNNING) {
                endTime = Instant.now();
                status = Status.fail(message);
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public String toString() {
            final ST template = new ST(STAGE_SUMMARY_TEMPLATE);

            template.add("stageName", stage.stageName());
            template.add("state", status.state());

            final boolean hasStarted = status.state() != State.READY;
            final boolean hasFinished = status.state() == State.COMPLETED || status.state() == State.FAILED;
            template.add("startTime", hasStarted ? startTime : "Not Started");
            template.add("endTime", hasFinished ? endTime : "Still Running");
            template.add("duration", hasStarted
                    ? Duration.between(startTime, hasFinished ? endTime : Instant.now()) : "Not Available");

            // Format the results with the correct indent
            final String results = hasFinished
                    ? stageResult.resultSummary().replaceAll("\n", "        \n")
                    : "<Not Available>";
            template.add("resultsSummary", results);

            return template.render();
        }
    }


}
