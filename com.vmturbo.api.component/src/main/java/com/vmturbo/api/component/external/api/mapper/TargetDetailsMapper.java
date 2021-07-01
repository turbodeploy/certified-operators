package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.target.DiscoveryStageApiDTO;
import com.vmturbo.api.dto.target.DiscoveryStageState;
import com.vmturbo.api.dto.target.DiscoveryStageStatusApiDTO;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails.StageStatus;

/**
 * Handles the conversion between {@link TargetDetails} and a list of {@link DiscoveryStageApiDTO}.
 */
public class TargetDetailsMapper {

    /**
     * Converts a {@link TargetDetails} to a list of {@link DiscoveryStageApiDTO}.
     *
     * @param targetDetails the details to be converted.
     * @return the discovery stages converted from the target details.
     */
    @Nonnull
    public List<DiscoveryStageApiDTO> convertToDiscoveryStageDetails(
            @Nonnull final TargetDetails targetDetails) {
        return targetDetails.getLastDiscoveryDetailsList().stream()
                .map(this::convertToDiscoveryStageDetail)
                .collect(Collectors.toList());
    }

    private DiscoveryStageApiDTO convertToDiscoveryStageDetail(ProbeStageDetails probeStageDetails) {
        DiscoveryStageApiDTO discoveryStageApiDTO = new DiscoveryStageApiDTO();
        if (probeStageDetails.hasDescription()) {
            discoveryStageApiDTO.setDescription(probeStageDetails.getDescription());
        }

        DiscoveryStageStatusApiDTO discoveryStageStatusApiDTO = new DiscoveryStageStatusApiDTO();
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

    private DiscoveryStageState convertToDiscoveryStageState(StageStatus stageStatus) {
        switch (stageStatus) {
            case FAILURE:
                return DiscoveryStageState.FAILURE;
            case DID_NOT_RUN:
                return DiscoveryStageState.DID_NOT_RUN;
            case SUCCESS:
                return DiscoveryStageState.SUCCESS;
            default:
                throw new IllegalArgumentException(stageStatus + " could not be converted to DiscoveryStageState");
        }
    }
}
