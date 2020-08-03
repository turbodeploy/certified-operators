package com.vmturbo.plan.orchestrator.cpucapacity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSortedSet;
import com.turbonomic.cpucapacity.CPUCapacityEstimator;
import com.turbonomic.cpucapacity.CPUCatalog;
import com.turbonomic.cpucapacity.CPUInfo;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuScaleFactorRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit Tests for the {@link CpuCapacityRpcService} Entry Points
 **/
public class CpuCapacitylRpcTest {
    public static final String CPU_MODEL_1 = "cpu_model_1";
    private static final Integer CPU_CORES_1 = 4;
    private static final Integer CPU_MHZ_1 = 500;
    private static final double CPU_MULTIPLIER_1 = 123.0D;
    public static final String CPU_MODEL_2 = "cpu_model_2";
    private static final Integer CPU_CORES_2 = 6;
    private static final Integer CPU_MHZ_2 = 1000;
    private static final double CPU_MULTIPLIER_2 = 456.0D;
    private static final int CPU_CATALOG_LIFE = 1;

    private CPUCatalog cpuCatalogMock = Mockito.mock(CPUCatalog.class);

    private CPUCapacityEstimator cpuCapacityEstimatorMock = Mockito.mock(CPUCapacityEstimator.class);

    public CpuCapacityRpcService cpuCapacityRpcService = new CpuCapacityRpcService(cpuCatalogMock,
        cpuCapacityEstimatorMock, CPU_CATALOG_LIFE);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(cpuCapacityRpcService);

    private CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub;

    @Before
    public void init() {
        cpuCapacityServiceBlockingStub = CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Test the gRPC call to fetch the entire cpu_model name list.
     */
    @Test
    public void testGetCpuModelList() {
        // Arrange
        CPUInfo cpuInfo1 = createMockCpuInfo(CPU_MODEL_1, CPU_CORES_1, CPU_MHZ_1, CPU_MULTIPLIER_1);
        CPUInfo cpuInfo2 = createMockCpuInfo(CPU_MODEL_2, CPU_CORES_2, CPU_MHZ_2, CPU_MULTIPLIER_2);
        final ImmutableSortedSet.Builder<CPUInfo> setBuilder = ImmutableSortedSet
            .orderedBy(Comparator.comparing(CPUInfo::getCpuModel));
        setBuilder.add(cpuInfo1, cpuInfo2);
        final SortedSet<CPUInfo> cpuInfoList = setBuilder.build();
        final CpuModelListResponse.CPUInfo[] cpuInfoExpected = {
            createExpectedCPUInfo(CPU_MODEL_1, CPU_CORES_1, CPU_MHZ_1, CPU_MULTIPLIER_1),
            createExpectedCPUInfo(CPU_MODEL_2, CPU_CORES_2, CPU_MHZ_2, CPU_MULTIPLIER_2)
        };
        when(cpuCatalogMock.getCatalog()).thenReturn(cpuInfoList);
        final CpuModelListRequest modelListRequest = CpuModelListRequest.newBuilder().build();
        // Act
        final CpuModelListResponse response = cpuCapacityServiceBlockingStub.getCpuModelList(modelListRequest);
        // Assert
        assertThat(response.getCpuInfoCount(), equalTo(cpuInfoList.size()));
        assertThat(response.getCpuInfoList(), Matchers.containsInAnyOrder(cpuInfoExpected));
    }

    /**
     * Test the gRPC call to fetch the CPU Scale Factor for each of a given list of cpu_model names.
     */
    @Test
    public void testGetCpuScaleFactors() {
        // Arrange
        final CpuScaleFactorRequest scaleFactorsRequest = CpuScaleFactorRequest.newBuilder()
            .addCpuModelNames(CPU_MODEL_1)
            .addCpuModelNames(CPU_MODEL_2)
            .build();
        when(cpuCapacityEstimatorMock.estimateMHzCoreMultiplier(CPU_MODEL_1))
            .thenReturn(CPU_MULTIPLIER_1);
        when(cpuCapacityEstimatorMock.estimateMHzCoreMultiplier(CPU_MODEL_2))
            .thenReturn(CPU_MULTIPLIER_2);
        // Act
        final CpuModelScaleFactorResponse response = cpuCapacityServiceBlockingStub
            .getCpuScaleFactors(scaleFactorsRequest);
        // Assert
        final Map<String, Double> responseMap = response.getScaleFactorByCpuModelMap();
        assertThat(responseMap.size(), equalTo(scaleFactorsRequest.getCpuModelNamesCount()));
        assertThat(responseMap.get(CPU_MODEL_1), equalTo(CPU_MULTIPLIER_1));
        assertThat(responseMap.get(CPU_MODEL_2), equalTo(CPU_MULTIPLIER_2));
    }

    /**
     * Create a mock CPUInfo structure.
     *
     * @param cpuModel the name of the CPU model
     * @param cpuCores the number of cores in this CPU model
     * @param cpuMhz the clock speed for this CPU model
     * @param cpuScaling the performance scale factor for this CPU model relative to a baseline
     * @return a mock CPUInfo set up to return the given values
     */
    private CPUInfo createMockCpuInfo(final String cpuModel, final int cpuCores,
                                      final int cpuMhz, final double cpuScaling) {
        CPUInfo cpuInfo1 = Mockito.mock(CPUInfo.class);
        when(cpuInfo1.getCpuModel()).thenReturn(cpuModel);
        when(cpuInfo1.getCores()).thenReturn(cpuCores);
        when(cpuInfo1.getMhz()).thenReturn(cpuMhz);
        when(cpuInfo1.getScalingFactor()).thenReturn(cpuScaling);
        return cpuInfo1;
    }

    /**
     * Build a CpuModelListResponse.CPUInfo protobuf initialized with the given fields.
     *
     * @param cpuModel the string name of the CPU model
     * @param cpuCores the number of cores in this CPU model
     * @param cpuMhz the clock speed of this CPU model
     * @param cpuScaling the performance of this CPU model, per core, relative to a basline
     * @return a CpuModelListResponse.CPUInfo initialized with the expected values
     */
    private CpuModelListResponse.CPUInfo createExpectedCPUInfo(final String cpuModel, final int cpuCores,
                                                               final int cpuMhz, final double cpuScaling) {
        return CpuModelListResponse.CPUInfo.newBuilder()
            .setCpuModelName(cpuModel)
            .setCores(cpuCores)
            .setMhz(cpuMhz)
            .setScalingFactor(cpuScaling)
            .build();
    }

}