package com.vmturbo.topology.processor.cpucapacity;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuScaleFactorRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
 * Provide access to the capacity performance analysis library 'com.turbonomic.cpucapacity'
 * hosted on the Plan Orchestrator.
 **/
public class RemoteCpuCapacityStore implements CpuCapacityStore {

    private static final Logger logger = LogManager.getLogger();

    /**
     * RPC for fetching scaleFactor info for different cpu_model strings.
     */
    private final CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub;

    /**
     * Cache the 'cpu_model -> scaleFactor' lookup here. Lifetime of the cache entries is externally set.
     */
    private final Cache<String, Optional<Double>> scaleFactorCache;

    /**
     * Leave space for 1000 CPU model cache entries.
     */
    private static final long SCALE_FACTOR_CACHE_MAX_SIZE = 1000L;

    RemoteCpuCapacityStore(@Nonnull final CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub,
                           final int scaleFactorCacheTimeoutHr) {
        this.cpuCapacityServiceBlockingStub = cpuCapacityServiceBlockingStub;
        scaleFactorCache = CacheBuilder.newBuilder()
                .expireAfterWrite(scaleFactorCacheTimeoutHr, TimeUnit.HOURS)
                .maximumSize(SCALE_FACTOR_CACHE_MAX_SIZE)
                .build();
    }

    /**
     * <p/>We use a cache to store each individual mapping from cpuModel -> scaleFactor. The cache
     * is initially empty. To populate each entry we issue an RPC call to get the corresponding
     * scaleFactor. A call is required as the cpu speed estimation library will do a pattern match
     * on the cpuModel string to return a value of a similar cpu if the given cpuModel is not
     * listed in the database.
     * <p/>Further, we believe that there will be a very small number of unique CPU models in
     * a customer's topology, and so having an individual call for each CPU model the first time
     * we see it is very cost-effective.
     * <p/>Finally, given that the post-stitching step receives an Iterable of the items to process,
     * running through the list once to derive a list of unique CPU names, which would require only
     * a single RPC to populate, would instead require constructing a list of TopologyEntities
     * to "fix up" at the end of the iteration.
     * @inheritDoc
     *
     */
    @Override
    public Optional<Double> getScalingFactor(@Nonnull final String cpuModel) {
        Objects.requireNonNull(cpuModel);
        try {
            return scaleFactorCache.get(cpuModel, () -> {
                final CpuModelScaleFactorResponse scaleFactors = cpuCapacityServiceBlockingStub
                        .getCpuScaleFactors(CpuScaleFactorRequest.newBuilder().addCpuModelNames(cpuModel)
                                .build());
                return Optional.ofNullable(scaleFactors.getScaleFactorByCpuModelMap().get(cpuModel));
            });
        } catch (ExecutionException e) {
            logger.error("Error fetching CPU Model for: " + cpuModel, e);
            return Optional.empty();
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                // This is a gRPC StatusRuntimeException
                Status status = ((StatusRuntimeException)e).getStatus();
                logger.warn("Unable to fetch CPU model for {}: {} caused by {}.",
                        cpuModel, status.getDescription(), status.getCause());
            } else {
                logger.error("Error fetching CPU Model for {}. ", cpuModel, e);
            }
            return Optional.empty();
        }
    }
}
