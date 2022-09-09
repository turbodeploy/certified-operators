package com.vmturbo.mediation.azure.pricing.controller;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.probe.ProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Base class for pricing discovery controllers.
 *
 * @param <A> The type of account for which pricing is to be discovered
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public abstract class AbstractPricingDiscoveryController<A extends ProxyAwareAccount,
        E extends ProbeStageEnum>
        implements PricingDiscoveryController<A> {
    private final Logger logger = LogManager.getLogger();

    /**
     * The directory to use for storing downloaded pricing data.
     */
    public static final IProbePropertySpec<Path> DOWNLOAD_DIRECTORY = new PropertySpec<>(
            "download.directory", Paths::get, Paths.get("/tmp"));

    /**
     * Construct the discovery controller.
     */
    public AbstractPricingDiscoveryController() {
    }

    /**
     * Discover the target with the given account values.
     *
     * @param accountValues account values specifying the target to discover
     * @param propertyProvider properties configuring the discovery
     * @return a discovery response DTO
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(
            @Nonnull A accountValues,
            @Nonnull IPropertyProvider propertyProvider) {
        // TODO locking to prevent simultaneous discovery. Should be based on the
        // PricingFileFetcher key not the key below, because we don't want to try to
        // process two pipelines that will return the same DiscoveredPricing.Keys

        DiscoveredPricing.Key key = getKey(accountValues);

        DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder();
        ProbeStageTracker<E> stageTracker = new ProbeStageTracker<E>(getDiscoveryStages());

        // TODO Check the cache and use the cached value if still valid, else...

        PriceTable.Builder tableBuilder = null;

        try (PricingPipelineContext context = new PricingPipelineContext(key.toString(), stageTracker)) {
            PricingPipeline<A, DiscoveredPricing> pipeline = buildPipeline(context, propertyProvider);

            DiscoveredPricing pricing = pipeline.run(accountValues);

            // TODO enter pricing for each DiscoveredPricing.Key into the cache

            tableBuilder = pricing.getPricingMap().get(key);
            if (tableBuilder != null) {
                // TODO checksumming and returning NoChange instead of result if same

                tableBuilder.addAllPriceTableKeys(key.getPricingIdentifiers());
                responseBuilder.setPriceTable(tableBuilder);
            }
        } catch (Exception ex) {
            logger.error("Exception during discovery", ex);

            return stageTracker.createDiscoveryError(ex.getMessage());
        }

        /* if (tableBuilder == null) {
            // This needs to be disabled until the placeholder stage is replaced with
            // the real final stage that returns the expected pricing:

            return stageTracker.createDiscoveryError(
                "Discovery pipeline succeeded but did not include the requested pricing "
                + key.toString());
        }*/

        responseBuilder.addAllStagesDetail(stageTracker.getStageDetails());

        return responseBuilder.build();
    }

    /**
     * Implement in subclasses to build the pipeline for discovery.
     *
     * @param context the context that will be used for invoking this instance of the pipeline
     * @param propertyProvider provides the properties for this discovery
     * @return the pipeline to run for live discovery
     */
    @Nonnull
    public abstract PricingPipeline<A, DiscoveredPricing> buildPipeline(
            @Nonnull PricingPipelineContext context,
            @Nonnull IPropertyProvider propertyProvider);

    /**
     * Implement to return the value of E::getDiscoveryStages(), indicating which stages of the
     * enumeration apply go discovery (vs those which might apply only to validation).
     *
     * @return The list of probe discovery expected to run during discovery.
     */
    @Nonnull
    public abstract List<E> getDiscoveryStages();

    /**
     * A helper function to construct a PricingIdentifier.
     *
     * @param name the name of the type of identifier
     * @param value the value of the identifier
     * @return the constructed PricingIdentifier
     */
    @Nonnull
    public static PricingIdentifier pricingIdentifier(
            @Nonnull final PricingIdentifierName name,
            @Nonnull final String value) {
        return PricingIdentifier.newBuilder().setIdentifierName(name).setIdentifierValue(value).build();
    }
}