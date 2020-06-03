package com.vmturbo.stitching.vdi;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;

/**
 * Abstract base class for the VDI stitching operations. This will reuse the
 * DataDriven stitching framework for all standard VDI stitching use cases defined
 * in
 * @link https://vmturbo.atlassian.net/wiki/spaces/Home/pages/758644807/Horizon+View+d5+-+stitching+and+other+mediation+changes
 * and @Link https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944996415/Horizon+XL+D1+-+Mediation+Probe+updates+and+Repository+component+changes
 */
public abstract class VDIStitchingOperation extends StringsToStringsDataDrivenStitchingOperation {

    public VDIStitchingOperation(EntityType entityType, Set<CommodityType> soldCommodityTypes,
                                 List<CommodityBoughtMetadata> boughtMetaDataList) {
        super(new StringsToStringsStitchingMatchingMetaData(
                        entityType, MergedEntityMetadata.newBuilder()
                        .mergeMatchingMetadata(MatchingMetadata.newBuilder()
                                .addMatchingData(MatchingData.newBuilder()
                                        .setMatchingProperty(EntityPropertyName.newBuilder()
                                                .setPropertyName(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                                                .build()))
                                .addExternalEntityMatchingProperty(MatchingData.newBuilder()
                                        .setMatchingProperty(EntityPropertyName.newBuilder()
                                                .setPropertyName(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                                                .build()))
                                .build())
                        .addAllCommoditiesSold(soldCommodityTypes)
                        .addAllCommoditiesBought(boughtMetaDataList)
                        .build()),
                Collections.singleton(ProbeCategory.HYPERVISOR));
    }

    public VDIStitchingOperation(EntityType entityType, Set<CommodityType> soldCommodityTypes) {
        this(entityType, soldCommodityTypes, Collections.emptyList());
    }
}
