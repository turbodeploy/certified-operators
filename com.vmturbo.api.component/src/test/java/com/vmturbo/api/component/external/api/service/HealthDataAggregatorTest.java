package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.service.util.HealthDataAggregator;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.HealthCategoryReponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthCheckCategory;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.api.enums.healthCheck.TargetCheckSubcategory;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.common.protobuf.target.TargetDTOMoles.TargetsServiceMole;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Tests the work of {@link HealthDataAggregator}.
 */
public class HealthDataAggregatorTest {

    private TargetsServiceMole targetBackend = spy(TargetsServiceMole.class);
    private SettingServiceMole settingBackend = spy(SettingServiceMole.class);

    /**
     * gRPC test server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(targetBackend, settingBackend);

    private HealthDataAggregator healthDataAggregator;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        healthDataAggregator = new HealthDataAggregator(TargetsServiceGrpc.newBlockingStub(
            testServer.getChannel()), SettingServiceGrpc.newBlockingStub(
            testServer.getChannel()));
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealth() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealth(TargetHealthSubCategory.VALIDATION, "fineValidated"));
        healthOfTargets.put(1L, makeHealth(TargetHealthSubCategory.DISCOVERY, "fineDiscovered"));
        healthOfTargets.put(2L, makeHealth(TargetHealthSubCategory.VALIDATION, "pendingValidationA",
                        "Pending Validation."));
        healthOfTargets.put(3L, makeHealth(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryA",
                        ErrorType.DATA_IS_MISSING, "Data is missing.", 1_000_000, 2));
        healthOfTargets.put(4L, makeHealth(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryB",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 4));
        healthOfTargets.put(5L, makeHealth(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryC",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 3));
        mockTargetHealth(healthOfTargets);

        HealthCheckCategory checkCategory = HealthCheckCategory.TARGET;
        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(checkCategory);
        Assert.assertEquals(1, aggregatedData.size());
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(checkCategory, responseDTO.getHealthCheckCategory());
        Assert.assertEquals(HealthState.CRITICAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(2, responseItems.size());
        for (AggregatedHealthResponseDTO rItem : responseItems) {
            if (TargetCheckSubcategory.DISCOVERY.toString().equals(rItem.getSubcategory())) {
                Assert.assertEquals(HealthState.CRITICAL, rItem.getHealthState());
                Assert.assertEquals(3, rItem.getNumberOfItems());
                Assert.assertEquals(2, rItem.getRecommendations().size());
            } else {
                Assert.assertEquals(HealthState.MINOR, rItem.getHealthState());
                Assert.assertEquals(1, rItem.getNumberOfItems());
                Assert.assertEquals(0, rItem.getRecommendations().size());
            }
        }
    }

    private void mockTargetHealth(Map<Long, TargetHealth> health) {
        doAnswer(invocationOnMock -> {
            GetTargetDetailsResponse.Builder respBuilder = GetTargetDetailsResponse.newBuilder();
            health.forEach((targetId, healthDeets) -> {
                respBuilder.putTargetDetails(targetId, TargetDetails.newBuilder()
                        .setTargetId(targetId)
                        .setHealthDetails(healthDeets)
                        .build());
            });
            return respBuilder.build();
        }).when(targetBackend).getTargetDetails(any());
    }

    private TargetHealth makeHealth(final TargetHealthSubCategory category, final String targetDisplayName,
            final ErrorType errorType, final String errorText,
            final long failureTime, final int failureTimes) {
        return TargetHealth.newBuilder()
                .setSubcategory(category)
                .setTargetName(targetDisplayName)
                .setErrorText(errorText)
                .setErrorType(errorType)
                .setTimeOfFirstFailure(failureTime)
                .setConsecutiveFailureCount(failureTimes)
                .build();
    }

    private TargetHealth makeHealth(final TargetHealthSubCategory category, final String targetDisplayName, final String errorText) {
        return TargetHealth.newBuilder()
                .setSubcategory(category)
                .setTargetName(targetDisplayName)
                .setErrorText(errorText)
                .build();
    }

    private TargetHealth makeHealth(final TargetHealthSubCategory category, final String targetDisplayName) {
        return TargetHealth.newBuilder()
                .setSubcategory(category)
                .setTargetName(targetDisplayName)
                .build();
    }

    /**
     * Tests getting the aggregated health data for the default health check category in the case
     * when there's only the TARGET check category supported and the health state is MINOR.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealthJustMinor() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealth(TargetHealthSubCategory.VALIDATION, "fineValidated"));
        healthOfTargets.put(1L, makeHealth(TargetHealthSubCategory.DISCOVERY, "fineDiscovered"));
        healthOfTargets.put(2L, makeHealth(TargetHealthSubCategory.VALIDATION, "pendingValidationA", "Pending Validation."));
        mockTargetHealth(healthOfTargets);

        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(null);
        Assert.assertEquals(1, aggregatedData.size());
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthCheckCategory.TARGET, responseDTO.getHealthCheckCategory());
        Assert.assertEquals(HealthState.MINOR, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(2, responseItems.size());
        for (AggregatedHealthResponseDTO rItem : responseItems) {
            if (TargetCheckSubcategory.DISCOVERY.toString().equals(rItem.getSubcategory())) {
                Assert.assertEquals(HealthState.NORMAL, rItem.getHealthState());
                Assert.assertEquals(1, rItem.getNumberOfItems());
                Assert.assertEquals(0, rItem.getRecommendations().size());
            } else {
                Assert.assertEquals(HealthState.MINOR, rItem.getHealthState());
                Assert.assertEquals(1, rItem.getNumberOfItems());
                Assert.assertEquals(0, rItem.getRecommendations().size());
            }
        }
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category
     * when there's no data about validations but there have been normal discoveries.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testNoValidationButNormalTargetsDiscoveries() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealth(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredA"));
        healthOfTargets.put(1L, makeHealth(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredB"));
        mockTargetHealth(healthOfTargets);

        HealthCategoryReponseDTO responseDTO = healthDataAggregator.getAggregatedHealth(
                        HealthCheckCategory.TARGET).get(0);
        Assert.assertEquals(HealthState.NORMAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());

        AggregatedHealthResponseDTO response = responseItems.get(0);
        Assert.assertEquals(TargetCheckSubcategory.DISCOVERY.toString(), response.getSubcategory());
        Assert.assertEquals(HealthState.NORMAL, response.getHealthState());
        Assert.assertEquals(2, response.getNumberOfItems());
        Assert.assertEquals(0, response.getRecommendations().size());
    }
}
