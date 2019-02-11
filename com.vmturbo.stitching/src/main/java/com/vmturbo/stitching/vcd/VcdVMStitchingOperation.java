package com.vmturbo.stitching.vcd;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringToStringStitchingMatchingMetaDataImpl;

/**
 * A stitching operation appropriate for use by VCD targets.
 *
 * Matching:
 * - Matches proxy VMs discovered by VCD with VMs discovered by hypervisor probe.
 * - A match is determined when the supply chain property INTERNAL_NAME_TGT_ID in proxy VM is equal
 * with the field of existing hypervisor VM.
 */

public class VcdVMStitchingOperation extends StringToStringDataDrivenStitchingOperation {

    public VcdVMStitchingOperation() {
        super(new StringToStringStitchingMatchingMetaDataImpl(
            EntityType.VIRTUAL_MACHINE, MergedEntityMetadata.newBuilder()
                .mergeMatchingMetadata(MatchingMetadata.newBuilder()
                    .addMatchingData(MatchingData.newBuilder()
                        .setMatchingProperty(EntityPropertyName.newBuilder()
                            .setPropertyName(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                                .build()))
                    .setReturnType(ReturnType.STRING)
                    .addExternalEntityMatchingProperty(MatchingData.newBuilder()
                        .setMatchingProperty(EntityPropertyName.newBuilder()
                            .setPropertyName(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                                .build()))
                    .setExternalEntityReturnType(ReturnType.STRING).build())
                .addPatchedFields(EntityField.newBuilder()
                        .setFieldName(SupplyChainConstants.DISPLAY_NAME).build())
                .addPatchedProperties(EntityPropertyName.newBuilder()
                        .setPropertyName(SupplyChainConstants.LOCAL_NAME).build())
            .build()),
            ImmutableSet.of(ProbeCategory.HYPERVISOR));
    }
}
