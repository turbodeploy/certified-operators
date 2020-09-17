package com.vmturbo.cloud.commitment.analysis;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * A manager of {@link CloudCommitmentAnalysis} instances. The manager is responsible for creating,
 * running, and tracking each individual analysis.
 */
public class CloudCommitmentAnalysisManager {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, CloudCommitmentAnalysis> runningAnalysesById = new ConcurrentHashMap<>();

    private final Map<Long, CloudCommitmentAnalysis> completedAnalysesById = CacheBuilder.newBuilder()
            .maximumSize(10)
            .<Long, CloudCommitmentAnalysis>build()
            .asMap();

    private final AnalysisFactory analysisFactory;

    private final ExecutorService analysisExecutorService;

    /**
     * Construct a new manager instance.
     *
     * @param analysisFactory A factory for creating {@link CloudCommitmentAnalysis} instances.
     * @param taskScheduler A {@link TaskScheduler}, used to periodically log the status of running
     *                      analyses.
     * @param logAnalysisStatusInterval The interval at which running analyses should be logged.
     * @param maxConcurrentAnalysis The max number of concurrent analyses allowed. Any subsequent
     *                              requests will be queued until a running analysis finishes.
     */
    public CloudCommitmentAnalysisManager(@Nonnull AnalysisFactory analysisFactory,
                                          @Nonnull TaskScheduler taskScheduler,
                                          @Nonnull Duration logAnalysisStatusInterval,
                                          int maxConcurrentAnalysis) {

        Preconditions.checkArgument(maxConcurrentAnalysis >= 1);

        this.analysisFactory = Objects.requireNonNull(analysisFactory);
        analysisExecutorService = Executors.newFixedThreadPool(
                maxConcurrentAnalysis,
                new ThreadFactoryBuilder()
                        .setNameFormat("cloud-commitment-analysis-runner-%d")
                        .build());

        Objects.requireNonNull(taskScheduler)
                .scheduleWithFixedDelay(this::logAnalysisStatus, logAnalysisStatusInterval);
    }

    /**
     * Creates and starts a new {@link CloudCommitmentAnalysis}, based on the {@code analysisConfig}
     * provided.
     *
     * @param analysisConfig The {@link CloudCommitmentAnalysisConfig} to be used by the analysis.
     * @return The {@link CloudCommitmentAnalysisInfo}, containing status information on the newly
     * created analysis and the analysis OID. The OID can be used to query the status of the analysis
     * at a later point.
     */
    public CloudCommitmentAnalysisInfo startAnalysis(@Nonnull CloudCommitmentAnalysisConfig analysisConfig) {

        final CloudCommitmentAnalysis cloudCommitmentAnalysis = analysisFactory.createAnalysis(analysisConfig);
        final CloudCommitmentAnalysisInfo analysisInfo = cloudCommitmentAnalysis.info();

        runningAnalysesById.put(analysisInfo.getOid(), cloudCommitmentAnalysis);
        analysisExecutorService.submit(handleAnalysisExecution(cloudCommitmentAnalysis));

        return analysisInfo;
    }


    private Runnable handleAnalysisExecution(@Nonnull CloudCommitmentAnalysis analysis) {

        return () -> {
            final CloudCommitmentAnalysisInfo analysisInfo = analysis.info();
            try {
                analysis.run();
            } catch (Exception e) {
                logger.error("Exception caught during analysis (Analysis OID={})",
                        analysisInfo.getOid(), e);
            } finally {
                markAnalysisComplete(analysisInfo.getOid());
            }
        };
    }


    private void markAnalysisComplete(long analysisOid) {
        final CloudCommitmentAnalysis analysis = runningAnalysesById.get(analysisOid);
        if (analysis != null) {
            completedAnalysesById.put(analysisOid, analysis);
            runningAnalysesById.remove(analysisOid);
        }

    }

    private void logAnalysisStatus() {

        // We do not lock the running analyses map - there is no guarantee all running analyses
        // will be logged in a single invocation.
        if (!runningAnalysesById.isEmpty()) {
            logger.info("Running cloud commitment analyses:");
            runningAnalysesById.values().forEach(analysis -> logger.info("{}", analysis));
        }

    }

}
