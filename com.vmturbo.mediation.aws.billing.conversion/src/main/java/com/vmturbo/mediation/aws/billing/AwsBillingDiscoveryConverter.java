package com.vmturbo.mediation.aws.billing;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Helper class to convert discovery response from classic AwsBilling format to the new one.
 */
public class AwsBillingDiscoveryConverter {
    private static final Logger logger = LogManager.getLogger();
    private final DiscoveryResponse.Builder discoveryResponseBuilder;

    /**
     * Constructor.
     *
     * @param discoveryResponse Discovery response to be converted
     */
    public AwsBillingDiscoveryConverter(@Nonnull final DiscoveryResponse discoveryResponse) {
        this.discoveryResponseBuilder = discoveryResponse.toBuilder();
    }

    /**
     * Do conversion.
     *
     * @return Converted discovery response
     */
    public DiscoveryResponse convert() {
        cloneVMs();
        return discoveryResponseBuilder.build();
    }

    /**
     * Clone VMs discovered by this probe and add the to the top level EntityDTO in discovery
     * response.
     */
    private void cloneVMs() {
        List<CommonDTO.EntityDTO> convertedVMs = getDiscoveredVMs().stream().map(item ->
            item.toBuilder()
                .clone()
                .setOrigin(EntityOrigin.PROXY)
                .setKeepStandalone(false)
        ).map(this::removeCouponCommodityIfPresent)
                .map(Builder::build)
                .collect(Collectors.toList());
        if (logger.isDebugEnabled()) {
            convertedVMs.forEach(e ->
                logger.debug("VM {} has guest name {}", e.getId(),
                    e.getVirtualMachineData().getGuestName()));
        }
        discoveryResponseBuilder.addAllEntityDTO(convertedVMs);
    }

    private CommonDTO.EntityDTO.Builder removeCouponCommodityIfPresent(
            final CommonDTO.EntityDTO.Builder entityBuilder) {
        final List<CommodityBought.Builder> mutableCommodityBoughtList =
                new ArrayList<>(entityBuilder.getCommoditiesBoughtBuilderList());
        mutableCommodityBoughtList.removeIf(commodityBought ->
                commodityBought.getBoughtBuilderList().stream()
                        .anyMatch(commodity -> commodity.getCommodityType()
                                == CommodityType.COUPON));
        entityBuilder.clearCommoditiesBought();
        entityBuilder.addAllCommoditiesBought(mutableCommodityBoughtList.stream()
                .map(CommodityBought.Builder::build).collect(Collectors.toList()));
        return entityBuilder;
    }

    /**
     * Get VMs discovered by the classic probe.
     *
     * @return VMs discovered by the classic probe.
     */
    private List<CommonDTO.EntityDTO> getDiscoveredVMs() {
        // these calls are null safe
        return discoveryResponseBuilder.getNonMarketEntityDTOList().stream()
            .flatMap(e -> e.getCloudServiceData().getBillingData().getVirtualMachinesList().stream())
            .collect(Collectors.toList());
    }

}
