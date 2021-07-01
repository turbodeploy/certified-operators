package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.vmturbo.api.dto.target.DiscoveryStageApiDTO;
import com.vmturbo.api.dto.target.DiscoveryStageState;
import com.vmturbo.api.dto.target.DiscoveryStageStatusApiDTO;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Verifies the conversion between {@link TargetDetails} and {@link DiscoveryStageApiDTO}.
 */
public class TargetDetailsMapperTest {

    private static final String DATABASE_DESCRIPTION = "Gets the database entities";
    private static final String SAMPLE_STACK_TRACE = "java.io.IOException\n"
            + "\tat com.vmturbo.mediation.appdynamics.AppDynamicsProbe.testValuesPresent("
            + "AppDynamicsProbe.java:382)\n";
    private static final String DATABASE_SHORT_EXPLANATION = "Encountered an exception while grabbing databases";
    private static final String DATABASE_LONG_EXPLANATION = "Encountered an exception while grabbing databases please check the Turbonomic Target Configuration Guide";
    private static final ProbeStageDetails SAMPLE_FAILED_STAGE = ProbeStageDetails.newBuilder()
            .setDescription(DATABASE_DESCRIPTION)
                    .setStatus(StageStatus.FAILURE)
                    .setStackTrace(SAMPLE_STACK_TRACE)
                    .setStatusShortExplanation(DATABASE_SHORT_EXPLANATION)
                    .setStatusLongExplanation(DATABASE_LONG_EXPLANATION)
                    .build();
    private static final TargetDetails SAMPLE_FAILED = TargetDetails.newBuilder()
            .addLastDiscoveryDetails(SAMPLE_FAILED_STAGE)
            .build();

    private TargetDetailsMapper targetDetailsMapper = new TargetDetailsMapper();

    /**
     * All values present in TargetDetails should be populated in DiscoveryStageApiDTO.
     */
    @Test
    public void testValuesPresent() {
        final List<DiscoveryStageApiDTO> actual = targetDetailsMapper.convertToDiscoveryStageDetails(
            SAMPLE_FAILED);
        assertEquals(1, actual.size());
        final DiscoveryStageApiDTO actualStage = actual.get(0);
        assertEquals(DATABASE_DESCRIPTION, actualStage.getDescription());
        assertNotNull(actualStage.getStatus());
        final DiscoveryStageStatusApiDTO actualStatus = actualStage.getStatus();
        assertEquals(DiscoveryStageState.FAILURE, actualStatus.getState());
        assertEquals(DATABASE_LONG_EXPLANATION, actualStatus.getFullExplanation());
        assertEquals(SAMPLE_STACK_TRACE, actualStatus.getStackTrace());
        assertEquals(DATABASE_SHORT_EXPLANATION, actualStatus.getSummary());
    }

    /**
     * Only values that are present in TargetDetails should be set in DiscoveryStageApiDTO.
     */
    @Test
    public void testValuesMissing() {
        final List<DiscoveryStageApiDTO> actual = targetDetailsMapper.convertToDiscoveryStageDetails(
                TargetDetails.newBuilder()
                        .addLastDiscoveryDetails(ProbeStageDetails.newBuilder()
                                .build())
                        .build());
        assertEquals(1, actual.size());
        final DiscoveryStageApiDTO actualStage = actual.get(0);
        assertNull(actualStage.getDescription());
        assertNotNull(actualStage.getStatus());
        final DiscoveryStageStatusApiDTO actualStatus = actualStage.getStatus();
        assertNull(actualStatus.getState());
        assertNull(actualStatus.getFullExplanation());
        assertNull(actualStatus.getStackTrace());
        assertNull(actualStatus.getSummary());
    }

    /**
     * Each StageStatus should be converted to its corresponding DiscoveryStageState.
     */
    @Test
    public void testEnumConversion() {
        checkStageStatus(DiscoveryStageState.FAILURE, StageStatus.FAILURE);
        checkStageStatus(DiscoveryStageState.SUCCESS, StageStatus.SUCCESS);
        checkStageStatus(DiscoveryStageState.DID_NOT_RUN, StageStatus.DID_NOT_RUN);
    }

    private void checkStageStatus(DiscoveryStageState apiState, StageStatus internalState) {
        final List<DiscoveryStageApiDTO> actual = targetDetailsMapper.convertToDiscoveryStageDetails(
                // replace the the sample with the state being checked
                TargetDetails.newBuilder()
                    .addLastDiscoveryDetails(SAMPLE_FAILED_STAGE.toBuilder()
                        .setStatus(internalState)
                        .build())
                    .build());
        assertEquals(1, actual.size());
        final DiscoveryStageApiDTO actualStage = actual.get(0);
        assertNotNull(actualStage.getStatus());
        final DiscoveryStageStatusApiDTO actualStatus = actualStage.getStatus();
        assertEquals(apiState, actualStatus.getState());
    }

    /**
     * All stages in TargetDetails should be converted.
     */
    @Test
    public void testMultipleStages() {
        TargetDetails.Builder multiStageDetails = TargetDetails.newBuilder();
        for (int i = 0; i < 3; i++) {
            ProbeStageDetails probeStageDetails = ProbeStageDetails.newBuilder()
                    .setStatusLongExplanation(DATABASE_LONG_EXPLANATION + i)
                    .setStatusShortExplanation(DATABASE_SHORT_EXPLANATION + i)
                    .setStackTrace(SAMPLE_STACK_TRACE + i)
                    .setStatus(StageStatus.FAILURE)
                    .setDescription(DATABASE_DESCRIPTION + i)
                    .build();
            multiStageDetails.addLastDiscoveryDetails(probeStageDetails);
        }
        final List<DiscoveryStageApiDTO> actual = targetDetailsMapper.convertToDiscoveryStageDetails(
                multiStageDetails.build());
        assertEquals(multiStageDetails.getLastDiscoveryDetailsCount(), actual.size());
        for (int i = 0; i < multiStageDetails.getLastDiscoveryDetailsCount(); i++) {
            final DiscoveryStageApiDTO actualStage = actual.get(i);
            assertEquals(DATABASE_DESCRIPTION + i, actualStage.getDescription());
            assertNotNull(actualStage.getStatus());
            final DiscoveryStageStatusApiDTO actualStatus = actualStage.getStatus();
            assertNotNull(actualStage.getStatus());
            assertEquals(DiscoveryStageState.FAILURE, actualStatus.getState());
            assertEquals(DATABASE_LONG_EXPLANATION + i, actualStatus.getFullExplanation());
            assertEquals(SAMPLE_STACK_TRACE + i, actualStatus.getStackTrace());
            assertEquals(DATABASE_SHORT_EXPLANATION + i, actualStatus.getSummary());
        }
    }

    /**
     * No stages in TargetDetails should be converted to an emtpy list.
     */
    @Test
    public void testZeroStages() {
        final List<DiscoveryStageApiDTO> actual = targetDetailsMapper.convertToDiscoveryStageDetails(
                TargetDetails.newBuilder()
                        .build());
        assertTrue(actual.isEmpty());
    }
}