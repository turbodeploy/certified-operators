package com.vmturbo.plan.orchestrator.cpucapacity;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Suppliers;
import com.turbonomic.cpucapacity.CPUCapacityEstimator;
import com.turbonomic.cpucapacity.CPUCatalog;
import com.turbonomic.cpucapacity.CPUInfo;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuScaleFactorRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceImplBase;

/**
 * Implement the gRPC services for accessing the Cpu Capacity library. This includes fetching the
 * list of known cpu_model strings and calculating the scalingFactor for a list of cpu_model.
 **/
public class CpuCapacityRpcService extends CpuCapacityServiceImplBase {

    /**
     * An instance of the Cpu Capacity catalog. Call to fetch all available CPU Info.
     */
    private final CPUCatalog cpuCatalog;

    /**
     * An instance of the Cpu Capacity Estimator. Call to estimate per-core capacity
     * or per-core MHz multiplier for each of a list of cpu_model names.
     */
    private final CPUCapacityEstimator cpuCapacityEstimator;

    /**
     * Create a time-based cache for the CPU Catalog - a set of CpuInfo values.
     */
    private Supplier<CpuModelListResponse> cpuInfoCatalogCache;

    public CpuCapacityRpcService(@Nonnull final CPUCatalog cpuCatalog,
                                 @Nonnull final CPUCapacityEstimator cpuCapacityEstimator,
                                 final int cpuCatalogLifeHours) {
        this.cpuCatalog = Objects.requireNonNull(cpuCatalog);
        // todo: create a cache for cpu MHZ multipliers
        this.cpuCapacityEstimator = Objects.requireNonNull(cpuCapacityEstimator);
        // create a cache for the CPU Info - it doesn't change very often
        cpuInfoCatalogCache = Suppliers.memoizeWithExpiration(this::buildCatalogResponse,
            cpuCatalogLifeHours, TimeUnit.HOURS);

    }

    @Override
    public void getCpuModelList(final CpuModelListRequest request,
                                final StreamObserver<CpuModelListResponse> responseObserver) {
        // as the CPU Info catalog changes rarely, the response is cached.
        responseObserver.onNext(cpuInfoCatalogCache.get());
        responseObserver.onCompleted();
    }

    @Override
    public void getCpuScaleFactors(final CpuScaleFactorRequest cpuScaleFactorRequest,
                                   final StreamObserver<CpuModelScaleFactorResponse> responseObserver) {
        final CpuModelScaleFactorResponse.Builder resultBuilder = CpuModelScaleFactorResponse.newBuilder();
        cpuScaleFactorRequest.getCpuModelNamesList().forEach(
            cpuModelName -> resultBuilder.putScaleFactorByCpuModel(cpuModelName,
                cpuCapacityEstimator.estimateMHzCoreMultiplier(cpuModelName)));
        responseObserver.onNext(resultBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * Build a CpuModelListResponse response protobuf containing the cpu_model names
     * from the Cpu Catalog.
     *
     * @return a CpuModelListResponse populated with all the cpu_model names known in the catalog.
     */
    private CpuModelListResponse buildCatalogResponse() {
        List<CpuModelListResponse.CPUInfo> cpuInfoList = cpuCatalog.getCatalog().stream()
            .map(cpuInfo->CpuModelListResponse.CPUInfo.newBuilder()
                .setCpuModelName(cpuInfo.getCpuModel())
                .setCores(cpuInfo.getCores())
                .setMhz(cpuInfo.getMhz())
                .setScalingFactor(cpuInfo.getScalingFactor())
                .build())
            .collect(Collectors.toList());
        return CpuModelListResponse.newBuilder()
            .addAllCpuInfo(cpuInfoList)
            .build();
    }
}
