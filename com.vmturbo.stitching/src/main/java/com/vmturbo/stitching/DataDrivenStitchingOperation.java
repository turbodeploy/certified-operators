package com.vmturbo.stitching;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.StitchingScopeType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.DTOFieldAndPropertyHandler;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.MergeEntities;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy;

/**
 * A class that extends {@link StitchingOperation} to use stitching metadata from the probe supply
 * chain to drive the stitching behavior.  This is done so that many probes can stitch without
 * having to write any code.  Defining the meta data drives the stitching behavior.
 * {@link StitchingMatchingMetaData} is the interface that encapsulates the meta data needed to
 * drive the stitching.
 *
 * @param <InternalSignatureT> The type of the signature used for matching with the internal
 *                                 (probe) side.
 * @param <ExternalSignatureT> The type of the signature used for matching with the external
 *                                 (server) side.
 */
public class DataDrivenStitchingOperation<InternalSignatureT, ExternalSignatureT>
                extends AbstractExternalSignatureCachingStitchingOperation<InternalSignatureT,
        ExternalSignatureT> {

    private static final Logger logger = LogManager.getLogger();
    private static final String INTERNAL = "internal";
    private static final String EXTERNAL = "external";

    // The data structure encapsulating the stitching behavior passed in from the supply chain of
    // the probe.
    private final StitchingMatchingMetaData<InternalSignatureT, ExternalSignatureT>
            matchingInformation;

    // Set of ProbeCategory identifying the categories of probe who we want to stitch with
    private final Set<ProbeCategory> categoriesToStitchWith;

    // Probe category of the probe that is associated with this stitching operation
    private final ProbeCategory probeCategory;

    // A map of provider entity type to the entity type of the provider it is
    // replacing.  For example, in the case of a logical pool provider for a proxy Storage, it
    // replaces a provider of entity type disk array.
    private Map<EntityType, EntityType> replacementEntityMap;

    /**
     * Constructor taking {@link StitchingMatchingMetaData} of the appropriate type to define the
     * stitching behavior.
     *
     * @param matchingInfo {@link StitchingMatchingMetaData} defining the stitching
     *                 behavior.
     * @param categoriesToStitchWith {@link Set} of {@link ProbeCategory} giving the
     *                 probe categories that
     * @param category the category of the probe associated with this operation
     */
    public DataDrivenStitchingOperation(
                    @Nonnull StitchingMatchingMetaData<InternalSignatureT, ExternalSignatureT> matchingInfo,
                    @Nonnull final Set<ProbeCategory> categoriesToStitchWith,
                    @Nonnull final ProbeCategory category) {
        this.matchingInformation = Objects.requireNonNull(matchingInfo);
        this.categoriesToStitchWith = Objects.requireNonNull(categoriesToStitchWith);
        this.probeCategory = category;
        initReplacementEntityMap();
    }

    @Override
    public boolean isCachingEnabled() {
        // Caching should be disabled if a scope is defined and that scope is PARENT
        // scope or if this operation stitches with targets in its own category. In the latter
        // case, we want to disable caching since stitching one target may affect the set of
        // external entities available for other targets.
        return matchingInformation.getStitchingScope().map(scope -> scope.getScopeType()
                != StitchingScopeType.PARENT).orElse(!categoriesToStitchWith.isEmpty()
                && !categoriesToStitchWith.contains(probeCategory));
    }

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
            long targetId) {
        // TODO add support for more scope types like GLOBAL and CATEGORY.  Eventually, all scopes
        // could be defined in the metadata.
        if (matchingInformation.getStitchingScope().isPresent()
                && matchingInformation.getStitchingScope().get().getScopeType()
                == StitchingScopeType.PARENT) {
            logger.trace("Returning parent scope for stitching operation with target {}",
                    targetId);
            return getExternalEntityType().map(externalEntityType -> stitchingScopeFactory
                    .parentTargetEntityType(externalEntityType, targetId));
        }
        if (categoriesToStitchWith.isEmpty()) {
            return Optional.empty();
        }
        return getExternalEntityType().map(externalEntityType ->stitchingScopeFactory
                .multiProbeCategoryEntityTypeScope(categoriesToStitchWith, externalEntityType));
    }

    /**
     * Iterate over the commodity bought data and build a map that connects provider type to the
     * commodity that identifies the provider it can replace.
     */
    private void initReplacementEntityMap() {
        replacementEntityMap = Maps.newHashMap();
        for (CommodityBoughtMetadata commBoughtData :
                matchingInformation.getCommoditiesBoughtToPatch()) {
            if (commBoughtData.hasReplacesProvider()){
                replacementEntityMap.put(commBoughtData.getProviderType(),
                        commBoughtData.getReplacesProvider());
            }
        }
    }

    /**
     * Take a provider from the internal entity and look for a matching provider from the
     * external entity to be replaced.
     *
     * @param internalEntity The provider on the probe side.
     * @param extEntity   The consumer on the server side whose providers will be searched for a match.
     * @return {@link Optional<StitchingEntity>} representing the provider if one was found.
     */
    private Optional<StitchingEntity> getReplacedEntity(StitchingEntity internalEntity,
                                                        StitchingEntity extEntity) {
        EntityType replacedEntityType = replacementEntityMap.get(
                internalEntity.getEntityType());
        if (replacedEntityType != null) {
            return extEntity.getProviders().stream()
                    .filter(stitchingEntity ->
                            stitchingEntity.getEntityType() == replacedEntityType)
                    .findFirst();
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return matchingInformation.getInternalEntityType();
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(matchingInformation.getExternalEntityType());
    }

    @Override
    public Collection<InternalSignatureT> getInternalSignature(@Nonnull StitchingEntity entity) {
        return getSignatures(entity, matchingInformation::getInternalMatchingData, INTERNAL);
    }

    @Override
    protected Collection<ExternalSignatureT> getExternalSignature(@Nonnull StitchingEntity entity) {
        return getSignatures(entity, matchingInformation::getExternalMatchingData, EXTERNAL);
    }

    private static <S> Collection<S> getSignatures(@Nonnull StitchingEntity internalEntity,
                    @Nonnull Supplier<Collection<MatchingPropertyOrField<S>>> matchingMetadataProvider,
                    @Nonnull String signatureType) {
        final Stopwatch sw = Stopwatch.createStarted();
        final Collection<MatchingPropertyOrField<S>> matchingPropertyOrFields =
                        matchingMetadataProvider.get();
        final Collection<S> result = matchingPropertyOrFields.stream()
                        .map(matchingFieldOrProp -> matchingFieldOrProp
                                        .getMatchingValue(internalEntity))
                        .filter(values -> !values.isEmpty())
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        if (logger.isTraceEnabled()) {
            logger.trace("Extracted '{}' '{}' signatures for '{}' entity using '{}' in '{}' ms",
                            result, signatureType, internalEntity.getOid(),
                            matchingPropertyOrFields, sw.stop().elapsed(TimeUnit.MILLISECONDS));
        } else if (logger.isDebugEnabled()) {
            if (result.size() > 1) {
                logger.debug("'{}|{}' entity has '{}' '{}' signatures: '{}'",
                                internalEntity.getOid(),
                                internalEntity.getDisplayName(), result.size(),
                                signatureType, result);
            }
        }
        return result;
    }

    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        final StringBuilder errorMessageBuilder = new StringBuilder();
        stitchingPoints.forEach(stitchingPoint -> {
            // TODO confirm that this logic works in case where multiple proxy entities are discovered
            // by a probe and they are in a consumer/provider relationship with each other.
            // The proxy entity (internalEntity) and real entity (externalEntity) to merge
            final StitchingEntity internalEntity = stitchingPoint.getInternalEntity();
            final StitchingEntity externalEntity =
                    stitchingPoint.getExternalMatches().iterator().next();

            // log an error if more than one external entity matched a single internal entity
            if (stitchingPoint.getExternalMatches().size() > 1) {
                errorMessageBuilder.append(String.format(
                                "Internal Entity %s matched multiple External Entities: %s%n",
                                internalEntity.getDisplayName(),
                                stitchingPoint.getExternalMatches().stream()
                                                .map(StitchingEntity::getDisplayName)
                                                .collect(Collectors.joining(", "))));
            }
            stitch(internalEntity, externalEntity, resultBuilder);
        });
        if (errorMessageBuilder.length() > 0) {
            logger.error(errorMessageBuilder.toString());
        }
        return resultBuilder.build();
    }

    /**
     * Merge the internal matching entity with the corresponding external matching entity.  Identify
     * providers that are replaced and queue their replacement.  Push commodities bought from
     * the proxy to the real object.  Push sold commodities from the proxy to the real object
     * according to the list of commodities to push in the metadata and push fields and properties
     * as well according to the metadata.
     *
     * @param internalEntity The proxy entity
     * @param externalEntity The real entity
     * @param resultBuilder  The builder of the results of the stitching operation. Changes to
     *                       relationships made by the stitching operation should be noted
     *                       in these results.
     */
    protected void stitch(@Nonnull final StitchingEntity internalEntity,
            @Nonnull final StitchingEntity externalEntity,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // iterate over bought meta data finding providers that need to be replaced
        for (CommodityBoughtMetadata boughtData : matchingInformation.getCommoditiesBoughtToPatch()) {
            // see if there is a provider related to the internalEntity for this set of
            // commodities bought
            Optional<StitchingEntity> internalProvider = internalEntity.getProviders().stream()
                    .filter(entity -> entity.getEntityType() == boughtData.getProviderType())
                    .findFirst();
            // We found a provider for the internal entity.  See if it is replacing a provider in the
            // externalEntity.  It is also possible that the provider from the internalEntity is of
            // origin=replaceable, in which case we should replace it by the matching provider from
            // the external entity.  This can happen, for example, when UCS has a DataCenter
            // provider for the PM that should be ignored if there is a DataCenter provider
            // already from the hypervisor
            internalProvider.ifPresent(provider -> {
                Optional<StitchingEntity> externalEntityProvider =
                        getReplacedEntity(provider, externalEntity);

                // Remove the externalProvider from the externalEntity provider-relationship.
                externalEntityProvider.ifPresent(externalProvider -> {
                    resultBuilder.queueChangeRelationships(externalEntity, toUpdate ->
                            toUpdate.removeProvider(externalProvider));
                    // Remove external provider from the topology if it is replaceable.
                    if (externalProvider.getEntityBuilder().getOrigin()
                            == EntityOrigin.REPLACEABLE) {
                        resultBuilder.queueEntityRemoval(externalProvider);
                    }
                });
            });
        }
        // Now copy all the bought commodities that are listed in the matching metadata from the
        // internalEntity to the externalEntity, do not copy if commoditiesBoughtToPatch is empty
        if (!matchingInformation.getCommoditiesBoughtToPatch().isEmpty()) {
            resultBuilder.queueChangeRelationships(externalEntity, toUpdate ->
                    CopyCommodities.copyCommodities(
                            matchingInformation.getCommoditiesBoughtToPatch())
                            .from(internalEntity).to(toUpdate));
        }

        // Create a MergeEntitiesDetails instance with a MergeCommoditySoldStrategy that is aware
        // of the list of sold commodities to merge from internal to external entity.
        // This MergeEntitiesDetails also handles merging of properties and entity fields.
        MergeEntities.MergeEntitiesDetails mergeEntitiesDetails =
                MergeEntities.mergeEntity(internalEntity)
                        .onto(externalEntity, getMergeCommoditySoldStrategy());
        matchingInformation.getPropertiesToPatch().forEach(prop -> {
            mergeEntitiesDetails.addFieldMerger(EntityFieldMergers.getPropertyFieldMerger(prop));
        });
        matchingInformation.getAttributesToPatch().forEach(attribute -> {
            mergeEntitiesDetails.addFieldMerger(EntityFieldMergers.getAttributeFieldMerger(attribute));
        });

        // merge sold commodities from proxy to real object and switch consumers that are consuming
        // from the proxy to consume from the real object
        resultBuilder.queueEntityMerger(mergeEntitiesDetails);
    }

    @Override
    public String toString() {
        return String.format(
                        "%s [matchingInformation=%s, categoriesToStitchWith=%s, replacementEntityMap=%s]",
                        getClass().getSimpleName(), this.matchingInformation,
                        this.categoriesToStitchWith, this.replacementEntityMap);
    }

    /**
     * Get a collection of {@link CommoditySoldMetadata} defined in the stitching metadata.
     *
     * @return a collection of commodity sold metadata
     */
    @Nonnull
    protected Collection<CommoditySoldMetadata> getCommoditiesSoldToPatch() {
        return matchingInformation.getCommoditiesSoldToPatch();
    }

    /**
     * Get the {@link MergeCommoditySoldStrategy} used by this stitching operation.
     *
     * @return the merge commodity sold strategy for this stitching operation
     */
    @Nonnull
    protected MergeCommoditySoldStrategy getMergeCommoditySoldStrategy() {
        return new MetaDataAwareMergeCommoditySoldStrategy(getCommoditiesSoldToPatch());
    }

    /**
     * A {@link MergeCommoditySoldStrategy} that only merges commodities sold that are listed in the
     * meta data.  This allows us to have extra sold commodities in the DTO returned from the probe
     * that don't get pushed.  This is useful, for example, in the case of standalone entities where
     * we need to include certain commodities so that we can create an entity in the case that no
     * match is found for the proxy entity.
     */
    public static class MetaDataAwareMergeCommoditySoldStrategy implements MergeCommoditySoldStrategy {

        private final Map<CommodityType, CommoditySoldMergeSpec> commoditySoldMergeSpecByType;

        public MetaDataAwareMergeCommoditySoldStrategy(@Nonnull final Collection<CommoditySoldMetadata> soldMetaData) {
            commoditySoldMergeSpecByType = soldMetaData.stream()
                .collect(Collectors.toMap(CommoditySoldMetadata::getCommodityType,
                    CommoditySoldMergeSpec::new));
        }

        @Nonnull
        protected Optional<CommoditySoldMergeSpec> getCommoditySoldMergeSpec(final CommodityType commodityType) {
            return Optional.ofNullable(commoditySoldMergeSpecByType.get(commodityType));
        }

        @Nonnull
        @Override
        public Optional<Builder> onDistinctCommodity(@Nonnull final Builder commodity,
                                                     @Nonnull final Origin origin) {
            // don't keep the from commodity if it is not in the list of sold commodities we are
            // configured to keep
            if (origin == Origin.FROM_ENTITY &&
                    !commoditySoldMergeSpecByType.containsKey(commodity.getCommodityType())) {
                return Optional.empty();
            }
            return Optional.of(commodity);
        }

        @Nonnull
        @Override
        public Optional<Builder> onOverlappingCommodity(@Nonnull final Builder fromCommodity,
                                                        @Nonnull final Builder ontoCommodity) {
            // if the soldMetaData says to preserve this commodity type merge the fromCommodity
            // with the ontoCommodity
            final CommoditySoldMergeSpec commoditySoldMergeSpec =
                this.commoditySoldMergeSpecByType.get(fromCommodity.getCommodityType());
            // if the fromCommodity is a type which should be ignored when ontoEntity already
            // has this commodity, then we can just use the ontoCommodity, no need to merge
            if (commoditySoldMergeSpec != null && !commoditySoldMergeSpec.isIgnoreIfPresent()) {
                return Optional.of(DTOFieldAndPropertyHandler.mergeBuilders(fromCommodity,
                        ontoCommodity, commoditySoldMergeSpec.getPatchedFields()));
            }
            return Optional.of(ontoCommodity);
        }

        @Override
        public boolean ignoreIfPresent(@Nonnull final CommodityType fromCommodityType) {
            return Optional.ofNullable(commoditySoldMergeSpecByType.get(fromCommodityType))
                .map(CommoditySoldMergeSpec::isIgnoreIfPresent).orElse(false);
        }
    }

    /**
     * A wrapper class around {@link CommoditySoldMetadata} which contains information about how to
     * merge the sold commodity during stitching.
     */
    public static class CommoditySoldMergeSpec {

        private final boolean ignoreIfPresent;

        private final List<DTOFieldSpec> patchedFields;

        CommoditySoldMergeSpec(@Nonnull CommoditySoldMetadata commoditySoldMetadata) {
            this.ignoreIfPresent = commoditySoldMetadata.getIgnoreIfPresent();
            this.patchedFields = commoditySoldMetadata.getPatchedFieldsList().stream()
                .map(DTOFieldSpecImpl::new).collect(Collectors.toList());
        }

        boolean isIgnoreIfPresent() {
            return ignoreIfPresent;
        }

        @Nonnull
        public List<DTOFieldSpec> getPatchedFields() {
            return patchedFields;
        }
    }
}
