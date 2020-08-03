package com.vmturbo.stitching.cloudfoundry;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.MatchingProperty;
import com.vmturbo.stitching.MatchingPropertyOrField;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;
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

public class CloudFoundryVMStitchingOperation extends StringsToStringsDataDrivenStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Custom target id matcher to extract the property
     * {@link SupplyChainConstants.CUSTOM_TARGET_ID} from given entity.
     */
    private final MatchingPropertyOrField<String> customTargetIdMatcher =
            new MatchingProperty(SupplyChainConstants.CUSTOM_TARGET_ID);

    /**
     * We use {@link StringsToStringsStitchingMatchingMetaData} for generic IP addresses
     * matching.
     */
    public CloudFoundryVMStitchingOperation() {
        super(new StringsToStringsStitchingMatchingMetaData(EntityType.VIRTUAL_MACHINE,
            MergedEntityMetadata.newBuilder()
                .mergeMatchingMetadata(MatchingMetadata.newBuilder()
                    .addMatchingData(MatchingData.newBuilder()
                        .setMatchingField(EntityField.newBuilder()
                            .addMessagePath(SupplyChainConstants.VIRTUAL_MACHINE_DATA)
                            .setFieldName(SupplyChainConstants.IP_ADDRESS)
                            .build())
                        .build())
                    .addExternalEntityMatchingProperty(MatchingData.newBuilder()
                        .setMatchingField(EntityField.newBuilder()
                            .addMessagePath(SupplyChainConstants.VIRTUAL_MACHINE_DATA)
                            .setFieldName(SupplyChainConstants.IP_ADDRESS)
                            .build())
                        .build())
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
            final Collection<String> internalCustomTargetId = customTargetIdMatcher.getMatchingValue(
                    stitchingPoint.getInternalEntity());
            // no custom target id from cloud foundry discovery, skip the following matching steps
            if (internalCustomTargetId.isEmpty()) {
                return;
            }
            final List<StitchingEntity> matchedExternalEntities = Lists.newArrayList();
            stitchingPoint.getExternalMatches().forEach(externalEntity -> {
                final Collection<String> externalCustomTargetId = customTargetIdMatcher
                        .getMatchingValue(externalEntity);
                // check if the custom target id of internal entity and external entity are same
                if (!externalCustomTargetId.isEmpty()
                        && internalCustomTargetId.iterator().next().equals(externalCustomTargetId.iterator().next())) {
                    logger.debug("Paired Cloud Foundry entity {} with VM {} by custom target" +
                        " id {}", stitchingPoint.getInternalEntity().getDisplayName(),
                        externalEntity.getDisplayName(), internalCustomTargetId.iterator().next());
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

