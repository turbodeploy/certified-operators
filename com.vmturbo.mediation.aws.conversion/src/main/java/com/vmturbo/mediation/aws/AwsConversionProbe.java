package com.vmturbo.mediation.aws;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.aws.client.AwsAccount;
import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
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
     * List of new cloud entity types to create supply chain node for, which don't exist in
     * original AWS probe discovery response.
     */
    private static Set<EntityType> NEW_SUPPLY_CHAIN_NODE_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_TIER,
            EntityType.AVAILABILITY_ZONE,
            EntityType.REGION
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

        // add stitching metadata for PM (AZ) and DC (Region)
        super.getSupplyChainDefinition().stream()
                .map(templateDTO -> templateDTO.getTemplateClass() == EntityType.BUSINESS_ACCOUNT ?
                        templateDTO.toBuilder()
                                .setMergedEntityMetaData(createMergedEntityMetadata())
                                .build() : templateDTO
                ).forEach(sc::add);

        // create supply chain node and add stitching metadata for new cloud entity types
        NEW_SUPPLY_CHAIN_NODE_ENTITY_TYPES.stream()
                .map(entityType ->
                        new SupplyChainNodeBuilder()
                            .entity(entityType)
                            .mergedBy(createMergedEntityMetadata())
                            .buildEntity())
                .forEach(sc::add);

        return sc;
    }

    /**
     * Create MergedEntityMetadata for supply chain node used for stitching. Cloud entities are
     * shared across different targets, and only one set should be kept in topology. This metadata
     * matches based on id of the EntityDTO.
     *
     * @return MergedEntityMetadata for use by stitching
     */
    private static MergedEntityMetadata createMergedEntityMetadata() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingField("id", Lists.newArrayList())
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingField("id", Lists.newArrayList())
                .externalMatchingType(ReturnType.STRING)
                .build();
    }
}
