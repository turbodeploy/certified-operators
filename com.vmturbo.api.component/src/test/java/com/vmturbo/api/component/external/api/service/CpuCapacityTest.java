package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.CpuInfoMapper;
import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.template.CpuModelApiDTO;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse.CPUInfo;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceImplBase;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test the service that fetches the list of CPU Models and converts to CpuModelApiDTO's
 */
public class CpuCapacityTest {

    private static final String CPU_MODEL_1 = "model1";
    private static final Integer CORES_1 = 4;
    private static final Integer SPEED_1 = 3000;
    private static final double SCALING_FACTOR_1 = 1.0;
    private static final String CPU_MODEL_2 = "model2";
    private static final Integer CORES_2 = 8;
    private static final Integer SPEED_2 = 4000;
    private static final double SCALING_FACTOR_2 = 1.6;

    private static Gson gson = new GsonBuilder().create();

    /**
     * Build the expected DTOs and convert to JSON to compare with results
     */
    private final static String[] EXPECTED_API_DTO_JSON = {
        gson.toJson(buildCpuModelApiDTO(CPU_MODEL_1,
            CORES_1,
            SPEED_1,
            SCALING_FACTOR_1)),
        gson.toJson(buildCpuModelApiDTO(CPU_MODEL_2,
            CORES_2,
            SPEED_2,
            SCALING_FACTOR_2))
    };

    /**
     * Response from the gRPC call
     */
    private final static CpuModelListResponse CPU_MODEL_RESPONSE = CpuModelListResponse.newBuilder()
        .addCpuInfo(CPUInfo.newBuilder()
            .setCpuModelName(CPU_MODEL_1)
            .setCores(CORES_1)
            .setMhz(SPEED_1)
            .setScalingFactor(SCALING_FACTOR_1)
            .build())
        .addCpuInfo(CPUInfo.newBuilder()
            .setCpuModelName(CPU_MODEL_2)
            .setCores(CORES_2)
            .setMhz(SPEED_2)
            .setScalingFactor(SCALING_FACTOR_2)
            .build())
        .build();

    private TemplatesService templatesService;

    public static final int CPU_CATALOG_LIFE_HOURS = 1;

    private final TestCpuCapacityService cpuCapacityService = Mockito.spy(new TestCpuCapacityService());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(cpuCapacityService);

    private TemplatesUtils templatesUtils = mock(TemplatesUtils.class);

    @Before
    public void setup() {
        final CpuInfoMapper cpuInfoMapper = new CpuInfoMapper();
        templatesService = new TemplatesService(
            TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            new TemplateMapper(), TemplateSpecServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel()), cpuInfoMapper,
            templatesUtils,
            CPU_CATALOG_LIFE_HOURS);
    }

    /**
     * Test that the Service returns information and that (at least one field of) the result
     * is not empty.

     */
    @Test
    public void testGetCpuList() {

        // Arrange
        // Act
        List<CpuModelApiDTO> cpuList = templatesService.getCpuList();

        // Assert
        assertEquals("Result list size mismatch", CPU_MODEL_RESPONSE.getCpuInfoCount(), cpuList.size());
        // there is no 'equals()' for ApiDTOs, so we convert the response to JSON and compare
        List<String> convertedModelInfo = cpuList.stream()
            .map(cpuInfo -> gson.toJson(cpuInfo))
            .collect(Collectors.toList());
        assertThat(convertedModelInfo, containsInAnyOrder(EXPECTED_API_DTO_JSON));
    }

    /**
     * Construct a sample CpuModelApiDTO that would be returned via the REST API.
     *
     * @param cpuModel string name of this cpu model
     * @param cores number of cores in this cpu model
     * @param speed cycle time (mhz) of this cpu model
     * @param scalingFactor relative performance of this
     * @return a new CpuModelApiDTO populated with the given values
     */
    private static CpuModelApiDTO buildCpuModelApiDTO(
        final String cpuModel, final Integer cores, final Integer speed, final double scalingFactor) {
        final CpuModelApiDTO answer = new CpuModelApiDTO();
        answer.setModelName(cpuModel);
        answer.setNumCores(cores);
        answer.setSpeed(speed);
        answer.setScalingFactor(scalingFactor);
        return answer;
    }


    /**
     * local mock for the CpuCapacityService
     */
    public class TestCpuCapacityService extends CpuCapacityServiceImplBase {
        @Override
        public void getCpuModelList(final CpuModelListRequest request,
                                    final StreamObserver<CpuModelListResponse> responseObserver) {
            responseObserver.onNext(CPU_MODEL_RESPONSE);
            responseObserver.onCompleted();
        }
    }
}
