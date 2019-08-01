package com.vmturbo.mediation.aws;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.aws.client.AwsAccount;
import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * The wrapper probe around original AWS probe, which stands between mediation and AWS probe. It
 * takes the topology discovered by AWS probe and converts it into the topology in new cloud model.
 */
public class AwsConversionProbe extends AwsProbe {

    private final Logger logger = LogManager.getLogger();

    /**
     * List of new cloud entity types to create supply chain nodes for, which don't exist in
     * original AWS probe supply chain definition.
     */
    @VisibleForTesting
    protected static Set<EntityType> NEW_SHARED_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_SERVER_TIER,
            EntityType.AVAILABILITY_ZONE,
            EntityType.REGION
    );

    /**
     * List of new non-shared cloud entity types to create supply chain nodes for, which don't
     * exist in original AWS probe supply chain definition.
     */
    @VisibleForTesting
    protected static Set<EntityType> NEW_NON_SHARED_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_VOLUME
    );

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AwsAccount awsAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        logger.debug("Started converting discovery response for AWS target {}",
                awsAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new CloudDiscoveryConverter(
                getRawDiscoveryResponse(awsAccount, discoveryContext),
                new AwsConversionContext()).convert();

        logger.debug("Done converting discovery response for AWS target {} within {} ms",
                awsAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AwsAccount awsAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        return super.discoverTarget(awsAccount, discoveryContext);
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        final Set<TemplateDTO> sc = Sets.newHashSet();

        // add stitching metadata for existing nodes (BUSINESS_ACCOUNT)
        for (TemplateDTO templateDTO : super.getSupplyChainDefinition()) {
            if (templateDTO.getTemplateClass() == EntityType.BUSINESS_ACCOUNT) {
                sc.add(templateDTO.toBuilder()
                        .setMergedEntityMetaData(createMergedEntityMetadataBuilder()
                                .mergedField("displayName", Collections.emptyList())
                                .mergedField("consistsOf", Collections.emptyList())
                                .mergedField("dataDiscovered", Lists.newArrayList("business_account_data"))
                                .build())
                        .build());
            } else {
                sc.add(templateDTO);
            }
        }

        // create supply chain nodes for new shared entities and add stitching metadata
        for (EntityType entityType : NEW_SHARED_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
                    .mergedBy(createMergedEntityMetadataBuilder().build())
                    .buildEntity());
        }

        // create supply chain nodes for new non-shared entities
        for (EntityType entityType : NEW_NON_SHARED_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
                    .buildEntity());
        }

        return sc;
    }

    /**
     * Create MergedEntityMetadataBuilder for supply chain node used for stitching. Cloud entities
     * are shared across different targets, and only one set should be kept in topology. This
     * metadata matches based on id of the EntityDTO.
     *
     * @return MergedEntityMetadataBuilder for use by stitching
     */
    private static MergedEntityMetadataBuilder createMergedEntityMetadataBuilder() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingField("id", Collections.emptyList())
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingField("id", Collections.emptyList())
                .externalMatchingType(ReturnType.STRING);
    }
}
