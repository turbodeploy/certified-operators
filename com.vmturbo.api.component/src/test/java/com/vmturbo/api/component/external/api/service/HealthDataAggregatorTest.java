package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.component.external.api.service.util.HealthDataAggregator;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.HealthCategoryReponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthCheckCategory;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.api.enums.healthCheck.TargetCheckSubcategory;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.topology.processor.api.ITargetHealthInfo;
import com.vmturbo.topology.processor.api.ITargetHealthInfo.TargetHealthSubcategory;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetHealthInfo;

/**
 * Tests the work of {@link HealthDataAggregator}.
 */
public class HealthDataAggregatorTest {

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private HealthDataAggregator healthDataAggregator = new HealthDataAggregator(topologyProcessor);

    /**
     * Tests getting the aggregated health data for the TARGET health check category.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealth() throws CommunicationException {
        Set<ITargetHealthInfo> healthOfTargets = new HashSet<>();
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.VALIDATION, 0L, "fineValidated"));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 1L, "fineDiscovered"));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.VALIDATION, 2L, "pendingValidationA",
                        "Pending Validation."));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 3L, "failedDiscoveryA",
                        ErrorType.DATA_IS_MISSING, "Data is missing.", LocalDateTime.now(), 2));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 4L, "failedDiscoveryB",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", LocalDateTime.now(), 4));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 5L, "failedDiscoveryC",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", LocalDateTime.now(), 3));
        when(topologyProcessor.getAllTargetsHealth()).thenReturn(healthOfTargets);

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

    /**
     * Tests getting the aggregated health data for the default health check category in the case
     * when there's only the TARGET check category supported and the health state is MINOR.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealthJustMinor() throws CommunicationException {
        Set<ITargetHealthInfo> healthOfTargets = new HashSet<>();
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.VALIDATION, 0L, "fineValidated"));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 1L, "fineDiscovered"));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.VALIDATION, 2L, "pendingValidationA",
                        "Pending Validation."));
        when(topologyProcessor.getAllTargetsHealth()).thenReturn(healthOfTargets);

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
        Set<ITargetHealthInfo> healthOfTargets = new HashSet<>();
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 0L, "fineDiscoveredA"));
        healthOfTargets.add(new TargetHealthInfo(TargetHealthSubcategory.DISCOVERY, 1L, "fineDiscoveredB"));
        when(topologyProcessor.getAllTargetsHealth()).thenReturn(healthOfTargets);

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
