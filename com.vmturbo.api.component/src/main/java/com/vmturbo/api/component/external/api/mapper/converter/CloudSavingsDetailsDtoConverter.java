package com.vmturbo.api.component.external.api.mapper.converter;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Class for converting from {@link CloudSavingsDetails} to {@link CloudResizeActionDetailsApiDTO}.
 */
public class CloudSavingsDetailsDtoConverter {

    private final EntityUptimeDtoConverter entityUptimeDtoConverter;

    /**
     * Constructor.
     *
     * @param entityUptimeDtoConverter the {@link EntityUptimeDtoConverter}
     */
    public CloudSavingsDetailsDtoConverter(
            @Nonnull final EntityUptimeDtoConverter entityUptimeDtoConverter) {
        this.entityUptimeDtoConverter = entityUptimeDtoConverter;
    }

    /**
     * Converts from {@link CloudSavingsDetails} to {@link CloudResizeActionDetailsApiDTO}.
     *
     * @param cloudSavingsDetails the {@link CloudResizeActionDetailsApiDTO}
     * @return the {@link CloudResizeActionDetailsApiDTO}
     */
    @Nonnull
    public CloudResizeActionDetailsApiDTO convert(
            @Nonnull final CloudSavingsDetails cloudSavingsDetails) {
        final CloudResizeActionDetailsApiDTO dto = new CloudResizeActionDetailsApiDTO();
        final TierCostDetails sourceTierCostDetails =
                cloudSavingsDetails.getSourceTierCostDetails();
        final TierCostDetails projectedTierCostDetails =
                cloudSavingsDetails.getProjectedTierCostDetails();

        final CurrencyAmount sourceOnDemandCost = sourceTierCostDetails.getOnDemandCost();
        if (sourceOnDemandCost.hasAmount()) {
            dto.setOnDemandCostBefore((float)sourceOnDemandCost.getAmount());
        }

        final CurrencyAmount projectedOnDemandCost = projectedTierCostDetails.getOnDemandCost();
        if (projectedOnDemandCost.hasAmount()) {
            dto.setOnDemandCostAfter((float)projectedOnDemandCost.getAmount());
        }

        final CurrencyAmount sourceOnDemandRate = sourceTierCostDetails.getOnDemandRate();
        if (sourceOnDemandRate.hasAmount()) {
            dto.setOnDemandRateBefore((float)sourceOnDemandRate.getAmount());
        }

        final CurrencyAmount projectedOnDemandRate = projectedTierCostDetails.getOnDemandRate();
        if (projectedOnDemandRate.hasAmount()) {
            dto.setOnDemandRateAfter((float)projectedOnDemandRate.getAmount());
        }

        if (sourceTierCostDetails.hasCloudCommitmentCoverage()) {
            dto.setRiCoverageBefore(convertCloudCommitmentCoverageToStatApiDto(
                    sourceTierCostDetails.getCloudCommitmentCoverage()));
        }

        if (projectedTierCostDetails.hasCloudCommitmentCoverage()) {
            dto.setRiCoverageAfter(convertCloudCommitmentCoverageToStatApiDto(
                    projectedTierCostDetails.getCloudCommitmentCoverage()));
        }

        if (cloudSavingsDetails.hasEntityUptime()) {
            dto.setEntityUptime(
                    entityUptimeDtoConverter.convert(cloudSavingsDetails.getEntityUptime()));
        }
        return dto;
    }

    @Nonnull
    private StatApiDTO convertCloudCommitmentCoverageToStatApiDto(
            @Nonnull final CloudCommitmentCoverage cloudCommitmentCoverage) {
        final StatApiDTO dto = new StatApiDTO();

        final CloudCommitmentAmount cloudCommitmentCoverageCapacity =
                cloudCommitmentCoverage.getCapacity();
        if (cloudCommitmentCoverageCapacity.hasCoupons()) {
            final StatValueApiDTO capacity = new StatValueApiDTO();
            capacity.setAvg((float)cloudCommitmentCoverageCapacity.getCoupons());
            dto.setCapacity(capacity);
        }

        final CloudCommitmentAmount cloudCommitmentCoverageUsed = cloudCommitmentCoverage.getUsed();
        if (cloudCommitmentCoverageUsed.hasCoupons()) {
            dto.setValue((float)cloudCommitmentCoverageUsed.getCoupons());
        }
        return dto;
    }
}
