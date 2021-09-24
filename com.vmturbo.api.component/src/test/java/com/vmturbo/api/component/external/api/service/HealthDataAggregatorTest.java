package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.HealthChecksTestBase;
import com.vmturbo.api.component.external.api.mapper.HealthDataMapper;
import com.vmturbo.api.component.external.api.service.util.HealthDataAggregator;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO.Recommendation;
import com.vmturbo.api.dto.admin.HealthCategoryReponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthCheckCategory;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.api.enums.healthCheck.TargetCheckSubcategory;
import com.vmturbo.api.enums.healthCheck.TargetErrorType;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
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
public class HealthDataAggregatorTest extends HealthChecksTestBase {

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
        healthDataAggregator = new HealthDataAggregator(TargetsServiceGrpc.newBlockingStub(testServer.getChannel()));
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealth() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealthNormal(TargetHealthSubCategory.VALIDATION, "fineValidated"));
        healthOfTargets.put(1L, makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscovered"));
        healthOfTargets.put(2L, makeHealthMinor(TargetHealthSubCategory.VALIDATION, "pendingValidationA",
                        "Pending Validation."));
        healthOfTargets.put(3L, makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryA",
                        ErrorType.DATA_IS_MISSING, "Data is missing.", 1_000_000, 2));
        healthOfTargets.put(4L, makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryB",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 4));
        healthOfTargets.put(5L, makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryC",
                        ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 3));
        defaultMockTargetHealth(healthOfTargets);

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
     * Tests getting the aggregated health data for the TARGET health check category with considering
     * hidden derived targets.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testParentAndHiddenTargetsBothNormal() throws CommunicationException {
        List<Long> ids = Lists.newArrayList(0L, 1L);
        List<TargetHealth> healths = Lists.newArrayList(
            makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredParent"),
            makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredDerived"));
        List<List<Long>> derived = Lists.newArrayList(Lists.newArrayList(1L), Lists.newArrayList());
        List<List<Long>> parents = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList(0L));
        List<Boolean> hidden = Lists.newArrayList(false, true);
        hiddenMockTargetHealth(ids, healths, derived, parents, hidden);
        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(HealthCheckCategory.TARGET);
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthState.NORMAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(0, responseItems.size());
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category with considering
     * hidden derived targets.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testHiddenTargetValidationTrumpsDiscovery() throws CommunicationException {
        List<Long> ids = Lists.newArrayList(0L, 1L);
        List<TargetHealth> healths = Lists.newArrayList(
            makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredParent"),
            makeHealthMinor(TargetHealthSubCategory.VALIDATION, "pendingValidationDerived", "Validation pending."));
        List<List<Long>> derived = Lists.newArrayList(Lists.newArrayList(1L), Lists.newArrayList());
        List<List<Long>> parents = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList(0L));
        List<Boolean> hidden = Lists.newArrayList(false, true);
        hiddenMockTargetHealth(ids, healths, derived, parents, hidden);

        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(HealthCheckCategory.TARGET);
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthState.MINOR, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());
        AggregatedHealthResponseDTO rItem = responseItems.get(0);
        Assert.assertEquals(TargetCheckSubcategory.VALIDATION.toString(), rItem.getSubcategory());
        Assert.assertEquals(HealthState.MINOR, rItem.getHealthState());
        Assert.assertEquals(1, rItem.getNumberOfItems());
        Assert.assertEquals(1, rItem.getRecommendations().size());
        Recommendation recommendation = rItem.getRecommendations().get(0);
        Assert.assertEquals(TargetErrorType.INTERNAL_PROBE_ERROR.toString(), recommendation.getErrorType());
        Assert.assertTrue(recommendation.getDescription().endsWith(HealthDataMapper.HIDDEN_TARGET_MESSAGE_SUFFIX));
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category with considering
     * hidden derived targets.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testHiddenTargetParentAggregateRecommendations() throws CommunicationException {
        List<Long> ids = Lists.newArrayList(0L, 1L, 2L);
        List<TargetHealth> healths = Lists.newArrayList(
            makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryParent",
                ErrorType.DATA_IS_MISSING, "Data is missing.", 1_000_000, 3),
            makeHealthCritical(TargetHealthSubCategory.VALIDATION, "failedDiscoveryDerivedA",
                ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 4),
            makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryDerivedB",
                ErrorType.INTERNAL_PROBE_ERROR, "Probe error.", 1_000_000, 3));
        List<List<Long>> derived = Lists.newArrayList(Lists.newArrayList(1L, 2L), Lists.newArrayList(), Lists.newArrayList());
        List<List<Long>> parents = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList(0L), Lists.newArrayList(0L));
        List<Boolean> hidden = Lists.newArrayList(false, true, true);
        hiddenMockTargetHealth(ids, healths, derived, parents, hidden);
        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(HealthCheckCategory.TARGET);
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthState.CRITICAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());
        AggregatedHealthResponseDTO rItem = responseItems.get(0);
        Assert.assertEquals(TargetCheckSubcategory.DISCOVERY.toString(), rItem.getSubcategory());
        Assert.assertEquals(HealthState.CRITICAL, rItem.getHealthState());
        Assert.assertEquals(1, rItem.getNumberOfItems());
        Assert.assertEquals(3, rItem.getRecommendations().size());
        Assert.assertTrue(rItem.getRecommendations().stream()
            .anyMatch(rec -> rec.getDescription().contains("(Hidden Target)")));
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category with considering
     * hidden derived targets.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testHiddenTargetParentNonNormalTrumpsDerived() throws CommunicationException {
        List<Long> ids = Lists.newArrayList(0L, 1L);
        List<TargetHealth> healths = Lists.newArrayList(
            makeHealthMinor(TargetHealthSubCategory.VALIDATION, "pendingValidationParent", "Pending Validation."),
            makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryDerived",
                ErrorType.CONNECTION_TIMEOUT, "Connection timeout.", 1_000_000, 4));
        List<List<Long>> derived = Lists.newArrayList(Lists.newArrayList(1L), Lists.newArrayList());
        List<List<Long>> parents = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList(0L));
        List<Boolean> hidden = Lists.newArrayList(false, true);
        hiddenMockTargetHealth(ids, healths, derived, parents, hidden);

        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(HealthCheckCategory.TARGET);
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthState.MINOR, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());
        AggregatedHealthResponseDTO rItem = responseItems.get(0);
        Assert.assertEquals(TargetCheckSubcategory.VALIDATION.toString(), rItem.getSubcategory());
        Assert.assertEquals(HealthState.MINOR, rItem.getHealthState());
        Assert.assertEquals(1, rItem.getNumberOfItems());
        Assert.assertEquals(1, rItem.getRecommendations().size());
    }

    /**
     * Tests that the hidden target failure is not double counted and that the parent target problem is reported.
     */
    @Test
    public void testFailingHiddenTargetNoDoubleCount() {
        final String tooOldDataMessage = "The data is too old.";
        List<Long> ids = Lists.newArrayList(0L, 1L);
        List<TargetHealth> healths = Lists.newArrayList(
            makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryParent",
                            ErrorType.DELAYED_DATA, tooOldDataMessage),
            makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "failedDiscoveryDerived",
                            ErrorType.DELAYED_DATA, tooOldDataMessage));
        List<List<Long>> derived = Lists.newArrayList(Lists.newArrayList(1L), Lists.newArrayList());
        List<List<Long>> parents = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList(0L));
        List<Boolean> hidden = Lists.newArrayList(false, true);
        hiddenMockTargetHealth(ids, healths, derived, parents, hidden);

        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(HealthCheckCategory.TARGET);
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthState.CRITICAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());
        AggregatedHealthResponseDTO rItem = responseItems.get(0);
        Assert.assertEquals(HealthState.CRITICAL, rItem.getHealthState());
        Assert.assertEquals(1, rItem.getNumberOfItems());
        Assert.assertEquals(1, rItem.getRecommendations().size());

        Recommendation recommendation = rItem.getRecommendations().get(0);
        Assert.assertEquals(TargetErrorType.DELAYED_DATA.toString(), recommendation.getErrorType());
        Assert.assertFalse(recommendation.getDescription().endsWith(HealthDataMapper.HIDDEN_TARGET_MESSAGE_SUFFIX));
    }

    private void defaultMockTargetHealth(Map<Long, TargetHealth> healthMap) {
        List<TargetDetails> targetDetails = new ArrayList<>();
        healthMap.forEach((id, health) -> {
            targetDetails.add(createTargetDetails(id, health, new ArrayList<>(), new ArrayList<>(), false));
        });
        mockTargetHealth(targetDetails.stream()
            .collect(Collectors.toMap(TargetDetails::getTargetId, Function.identity())));
    }

    private void hiddenMockTargetHealth(List<Long> ids, List<TargetHealth> healths, List<List<Long>> derived,
        List<List<Long>> parents, List<Boolean> hidden) {
        List<TargetDetails> targetDetails = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            targetDetails.add(createTargetDetails(ids.get(i), healths.get(i), derived.get(i),
                parents.get(i), hidden.get(i)));
        }
        mockTargetHealth(targetDetails.stream()
            .collect(Collectors.toMap(TargetDetails::getTargetId, Function.identity())));
    }

    private void mockTargetHealth(Map<Long, TargetDetails> targetDetails) {
        doAnswer(invocationOnMock -> {
            GetTargetDetailsResponse.Builder respBuilder = GetTargetDetailsResponse.newBuilder();
            respBuilder.putAllTargetDetails(targetDetails);
            return respBuilder.build();
        }).when(targetBackend).getTargetDetails(any());
    }

    /**
     * Tests getting the aggregated health data for the default health check category in the case
     * when there's only the TARGET check category supported and the health state is MINOR.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testGetAggregatedTargetsHealthJustMinor() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealthNormal(TargetHealthSubCategory.VALIDATION, "fineValidated"));
        healthOfTargets.put(1L, makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscovered"));
        healthOfTargets.put(2L, makeHealthMinor(TargetHealthSubCategory.VALIDATION, "pendingValidationA", "Pending Validation."));
        defaultMockTargetHealth(healthOfTargets);

        List<HealthCategoryReponseDTO> aggregatedData = healthDataAggregator.getAggregatedHealth(null);
        Assert.assertEquals(1, aggregatedData.size());
        HealthCategoryReponseDTO responseDTO = aggregatedData.get(0);
        Assert.assertEquals(HealthCheckCategory.TARGET, responseDTO.getHealthCheckCategory());
        Assert.assertEquals(HealthState.MINOR, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(1, responseItems.size());

        AggregatedHealthResponseDTO subcategoryResponse = responseItems.get(0);
        Assert.assertEquals(TargetCheckSubcategory.VALIDATION.toString(), subcategoryResponse.getSubcategory());
        Assert.assertEquals(HealthState.MINOR, subcategoryResponse.getHealthState());
        Assert.assertEquals(1, subcategoryResponse.getNumberOfItems());
        Assert.assertEquals(0, subcategoryResponse.getRecommendations().size());
    }

    /**
     * Tests getting the aggregated health data for the TARGET health check category
     * when there's no data about validations but there have been normal discoveries.
     * @throws CommunicationException (is supposed to never happen)
     */
    @Test
    public void testNoValidationButNormalTargetsDiscoveries() throws CommunicationException {
        Map<Long, TargetHealth> healthOfTargets = new HashMap<>();
        healthOfTargets.put(0L, makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredA"));
        healthOfTargets.put(1L, makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "fineDiscoveredB"));
        defaultMockTargetHealth(healthOfTargets);

        HealthCategoryReponseDTO responseDTO = healthDataAggregator.getAggregatedHealth(
                        HealthCheckCategory.TARGET).get(0);
        Assert.assertEquals(HealthState.NORMAL, responseDTO.getCategoryHealthState());
        List<AggregatedHealthResponseDTO> responseItems = responseDTO.getResponseItems();
        Assert.assertEquals(0, responseItems.size());
    }
}
