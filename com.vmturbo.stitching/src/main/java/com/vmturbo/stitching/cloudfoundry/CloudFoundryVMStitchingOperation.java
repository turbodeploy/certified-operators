package com.vmturbo.stitching.cloudfoundry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.ListStringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToListStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.MatchingPropertyOrField;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StringMatchingProperty;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A stitching operation appropriate for use by Cloud foundry targets.
 *
 * Matching:
 * - The discovered proxy VM from Cloud foundry target will match the real VM discovered by VC.
 * There are two ways stitching:
 * 1. If the real VM has both field "ipAddress" and "customTargetId", we then stitch the VM with
 * related proxy VM directly.
 * 2. If the real VM has have only field "ipAddress", this is mean that the VM does not stitch with
 * Pivotal proxy VM yet, and we look at all of the VMs which have the same
 */

public class CloudFoundryVMStitchingOperation extends
        ListStringToListStringDataDrivenStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Custom target id matcher to extract the property
     * {@link SupplyChainConstants.CUSTOM_TARGET_ID} from given entity.
     */
    private final MatchingPropertyOrField<String> customTargetIdMatcher =
            new StringMatchingProperty(SupplyChainConstants.CUSTOM_TARGET_ID);

    /**
     * We use {@link ListStringToListStringStitchingMatchingMetaDataImpl} for generic IP addresses
     * matching.
     */
    public CloudFoundryVMStitchingOperation() {
        super(new ListStringToListStringStitchingMatchingMetaDataImpl(EntityType.VIRTUAL_MACHINE,
            MergedEntityMetadata.newBuilder()
                .mergeMatchingMetadata(MatchingMetadata.newBuilder()
                    .addMatchingData(MatchingData.newBuilder()
                        .setMatchingField(EntityField.newBuilder()
                            .addMessagePath(SupplyChainConstants.VIRTUAL_MACHINE_DATA)
                            .setFieldName(SupplyChainConstants.IP_ADDRESS)
                            .build())
                        .build())
                    .setReturnType(ReturnType.LIST_STRING)
                    .addExternalEntityMatchingProperty(MatchingData.newBuilder()
                        .setMatchingField(EntityField.newBuilder()
                            .addMessagePath(SupplyChainConstants.VIRTUAL_MACHINE_DATA)
                            .setFieldName(SupplyChainConstants.IP_ADDRESS)
                            .build())
                        .build())
                    .setExternalEntityReturnType(ReturnType.LIST_STRING)
                .build())
                .addCommoditiesSoldMetadata(CommoditySoldMetadata.newBuilder()
                    .setCommodityType(CommodityType.MEM_ALLOCATION))
                .addCommoditiesSoldMetadata(CommoditySoldMetadata.newBuilder()
                    .setCommodityType(CommodityType.CLUSTER))
            .build()),
            ImmutableSet.of(ProbeCategory.HYPERVISOR, ProbeCategory.CLOUD_MANAGEMENT));
    }

    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        return super.stitch(filterStitchingPoints(stitchingPoints), resultBuilder);
    }

    /**
     * Since we are using list string data driven stitching for IP address matching as the first
     * stitching step, if there is Pivotal target that already stitched with hypervisor target,
     * we need to filter out the stitching points witch
     * {@link SupplyChainConstants.CUSTOM_TARGET_ID}s are not matching.
     *
     * @param stitchingPoints The collection of {@link StitchingPoint}s that should be stitched.
     * @return The collection of {@link StitchingPoint}s that have been filtered by
     * {@link SupplyChainConstants.CUSTOM_TARGET_ID}
     */
    private Collection<StitchingPoint> filterStitchingPoints(
            @Nonnull final Collection<StitchingPoint> stitchingPoints) {
        final List<StitchingPoint> filteredStitchingPoints = Lists.newArrayList();
        stitchingPoints.stream().forEach(stitchingPoint -> {
            final Optional<String> internalCustomTargetId = customTargetIdMatcher.getMatchingValue(
                    stitchingPoint.getInternalEntity());
            // no custom target id from cloud foundry discovery, skip the following matching steps
            if (!internalCustomTargetId.isPresent()) {
                return;
            }
            final List<StitchingEntity> matchedExternalEntities = Lists.newArrayList();
            stitchingPoint.getExternalMatches().forEach(externalEntity -> {
                final Optional<String> externalCustomTargetId = customTargetIdMatcher
                        .getMatchingValue(externalEntity);
                // check if the custom target id of internal entity and external entity are same
                if (externalCustomTargetId.isPresent()
                        && internalCustomTargetId.get().equals(externalCustomTargetId.get())) {
                    logger.debug("Paired Cloud Foundry entity {} with VM {} by custom target" +
                        " id {}", stitchingPoint.getInternalEntity().getDisplayName(),
                        externalEntity.getDisplayName(), internalCustomTargetId.get());
                    matchedExternalEntities.add(externalEntity);
                }
            });
            // create new stitching point for the matched entities
            if (!matchedExternalEntities.isEmpty()) {
                filteredStitchingPoints.add(new StitchingPoint(stitchingPoint.getInternalEntity(),
                        matchedExternalEntities));
            }
        });
        return !filteredStitchingPoints.isEmpty() ? filteredStitchingPoints : stitchingPoints;
    }
}

