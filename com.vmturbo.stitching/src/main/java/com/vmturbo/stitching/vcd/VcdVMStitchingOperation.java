package com.vmturbo.stitching.vcd;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.MatchingProperty;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.StringMatchingProperty;
import com.vmturbo.stitching.StringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringToStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.MergeEntities;

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
