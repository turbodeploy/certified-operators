package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.target.TargetOperationStageApiDTO;
import com.vmturbo.api.dto.target.TargetOperationStageState;
import com.vmturbo.api.dto.target.TargetOperationStageStatusApiDTO;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Handles the conversion between {@link TargetDetails} and a list of {@link TargetOperationStageApiDTO}.
 */
public class TargetDetailsMapper {

    /**
     * Converts a {@link TargetDetails} to a list of {@link TargetOperationStageApiDTO}.
     *
     * @param targetDetails the details to be converted.
     * @return the discovery stages converted from the target details.
     */
    @Nonnull
    public List<TargetOperationStageApiDTO> convertToTargetOperationStages(
            @Nonnull final TargetDetails targetDetails) {
        return targetDetails.getLastDiscoveryDetailsList().stream()
                .map(this::convertToDiscoveryStageDetail)
                .collect(Collectors.toList());
    }

    private TargetOperationStageApiDTO convertToDiscoveryStageDetail(ProbeStageDetails probeStageDetails) {
        TargetOperationStageApiDTO discoveryStageApiDTO = new TargetOperationStageApiDTO();
        if (probeStageDetails.hasDescription()) {
            discoveryStageApiDTO.setDescription(probeStageDetails.getDescription());
        }

        TargetOperationStageStatusApiDTO discoveryStageStatusApiDTO = new TargetOperationStageStatusApiDTO();
        discoveryStageApiDTO.setStatus(discoveryStageStatusApiDTO);
        if (probeStageDetails.hasStatus()) {
            discoveryStageStatusApiDTO.setState(convertToDiscoveryStageState(probeStageDetails.getStatus()));
        }
        if (probeStageDetails.hasStackTrace()) {
            discoveryStageStatusApiDTO.setStackTrace(probeStageDetails.getStackTrace());
        }
        if (probeStageDetails.hasStatusShortExplanation()) {
            discoveryStageStatusApiDTO.setSummary(probeStageDetails.getStatusShortExplanation());
        }
        if (probeStageDetails.hasStatusLongExplanation()) {
            discoveryStageStatusApiDTO.setFullExplanation(probeStageDetails.getStatusLongExplanation());
        }
        return discoveryStageApiDTO;
    }

    private TargetOperationStageState convertToDiscoveryStageState(StageStatus stageStatus) {
        switch (stageStatus) {
            case FAILURE:
                return TargetOperationStageState.FAILURE;
            case DID_NOT_RUN:
                return TargetOperationStageState.DID_NOT_RUN;
            case SUCCESS:
                return TargetOperationStageState.SUCCESS;
            default:
                throw new IllegalArgumentException(stageStatus + " could not be converted to DiscoveryStageState");
        }
    }
}
