package com.vmturbo.topology.processor.cpucapacity;

import static org.hibernate.validator.internal.util.Contracts.assertTrue;

import java.io.IOException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuScaleFactorRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
 * Test the wrapper class that fetches Cpu Capacity scalingFactor values from Plan Orchestrator.
 **/
public class RemoteCpuCapacityStoreTest {

    private static final String CPU_MODEL="cpu_model_1";
    private static final Double SCALE_FACTOR=1.717D;
    private static final Double EPSILON=0.0000001;
    private static final CpuModelScaleFactorResponse CPU_MODEL_RESPONSE =
            CpuModelScaleFactorResponse.newBuilder()
                    .putScaleFactorByCpuModel(CPU_MODEL, SCALE_FACTOR)
                    .build();
    private static final int SCALE_FACTOR_TIMEOUT_CACHE_HR = 8;

    private CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub;

    @Before
    public void setup() throws IOException {
        TestCpuCapacityService cpuCapacityService = Mockito.spy(new TestCpuCapacityService());
        final GrpcTestServer grpcServer = GrpcTestServer.newServer(cpuCapacityService);
        grpcServer.start();
        cpuCapacityServiceBlockingStub = CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }
    @Test
    public void testCpuCapacityStore() {
        // Arrange
        CpuCapacityStore testStore = new RemoteCpuCapacityStore(cpuCapacityServiceBlockingStub,
                SCALE_FACTOR_TIMEOUT_CACHE_HR);
        // Act
        final Optional<Double> response = testStore.getScalingFactor(CPU_MODEL);
        // assert
        final Double responseScaleFactor = response.orElseThrow(() ->
                new RuntimeException("response not present"));
        assertTrue((Math.abs(responseScaleFactor - SCALE_FACTOR)) < EPSILON,
                "'scaleFactor' doesn't match");
    }

    /**
     * local mock for the CpuCapacityService
     */
    public class TestCpuCapacityService extends CpuCapacityServiceImplBase {
        @Override
        public void getCpuScaleFactors(final CpuScaleFactorRequest request,
                                    final StreamObserver<CpuModelScaleFactorResponse> responseObserver) {
            responseObserver.onNext(CPU_MODEL_RESPONSE);
            responseObserver.onCompleted();
        }
    }
}
