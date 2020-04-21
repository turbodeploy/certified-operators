package com.vmturbo.stitching;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
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
 * @param <INTERNAL_SIGNATURE_TYPE> The type of the signature used for matching with the internal
 *                                 (probe) side.
 * @param <EXTERNAL_SIGNATURE_TYPE> The type of the signature used for matching with the external
 *                                 (server) side.
 */
public class DataDrivenStitchingOperation<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> implements
        StitchingOperation<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> {

    private static final Logger logger = LogManager.getLogger();

    // The data structure encapsulating the stitching behavior passed in from the supply chain of
    // the probe.
    private final StitchingMatchingMetaData<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE>
            matchingInformation;

    // Set of ProbeCategory identifying the categories of probe who we want to stitch with
    private final Set<ProbeCategory> categoriesToStitchWith;

    // A map of provider entity type to the entity type of the provider it is
    // replacing.  For example, in the case of a logical pool provider for a proxy Storage, it
    // replaces a provider of entity type disk array.
    private Map<EntityType, EntityType> replacementEntityMap;

    /**
     * Constructor taking {@link StitchingMatchingMetaData} of the appropriate type to define the stitching
     * behavior.
     *
     * @param matchingInfo {@link StitchingMatchingMetaData} defining the storage behavior.
     * @param categoriesToStitchWith {@link Set} of {@link ProbeCategory} giving the probe categories that
     *                                  this stitching operation can stitch with.
     */
    public DataDrivenStitchingOperation(@Nonnull StitchingMatchingMetaData<INTERNAL_SIGNATURE_TYPE,
            EXTERNAL_SIGNATURE_TYPE> matchingInfo,
                                        @Nonnull final Set<ProbeCategory> categoriesToStitchWith) {
        this.matchingInformation = Objects.requireNonNull(matchingInfo);
        this.categoriesToStitchWith = Objects.requireNonNull(categoriesToStitchWith);
        initReplacementEntityMap();
    }

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        if (categoriesToStitchWith.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(stitchingScopeFactory
                .multiProbeCategoryEntityTypeScope(categoriesToStitchWith, getExternalEntityType().get()));
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

    /**
     * Return a concatenated list of the String values passed in.  This is here as a convenience
     * method to be called from combineSignatures by subclasses that are matching on String.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional string of concatenated values or Optional.empty.
     */
    protected static Optional<String> combineStringSignatures(List<String> matchingValues) {
        if (matchingValues.isEmpty()) {
            return Optional.empty();
        }
        String concatenatedValues = matchingValues.stream()
                .collect(Collectors.joining(""));

        return Optional.of(concatenatedValues);
    }

    /**
     * Method to combine multiple internal signature values into a single value.  Default behavior
     * is to return first element of list.  Subclasses should override with useful
     * behavior if they support combining multiple values.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional of combined values.
     */
    protected Optional<INTERNAL_SIGNATURE_TYPE> combineInternalSignatures(
            List<INTERNAL_SIGNATURE_TYPE> matchingValues) {
        if (matchingValues.isEmpty()) {
            return Optional.empty();
        }
        if (matchingValues.size() > 1) {
            logger.error("Multiple internal signatures returned for a stitching operation without "
                    + "a defined method to combine values.  No stitching can be done on this entity.");
            return Optional.empty();
        }
        return Optional.of(matchingValues.get(0));
    }

    /**
     * Method to combine multiple external signature values into a single value.  Default behavior
     * is to return first element of list.  Subclasses should override with useful
     * behavior if they support combining multiple values.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional of combined values.
     */
    protected Optional<EXTERNAL_SIGNATURE_TYPE> combineExternalSignatures(
            List<EXTERNAL_SIGNATURE_TYPE> matchingValues) {
        if (matchingValues.isEmpty()) {
            return Optional.empty();
        }
        if (matchingValues.size() > 1) {
            logger.error("Multiple external signatures returned for a stitching operation without "
            + "a defined method to combine values.  No stitching can be done on this entity.");
            return Optional.empty();
        }
        return Optional.of(matchingValues.get(0));
    }

    @Override
    public Optional<INTERNAL_SIGNATURE_TYPE> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
        return combineInternalSignatures(matchingInformation.getInternalMatchingData()
                .stream()
                .map(matchingFieldOrProp -> matchingFieldOrProp.getMatchingValue(internalEntity))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));
    }

    @Override
    public Optional<EXTERNAL_SIGNATURE_TYPE> getExternalSignature(@Nonnull StitchingEntity externalEntity) {
        return combineExternalSignatures(matchingInformation.getExternalMatchingData()
                .stream()
                .map(matchingFieldOrProp -> matchingFieldOrProp.getMatchingValue(externalEntity))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> {
            // TODO confirm that this logic works in case where multiple proxy entities are discovered
            // by a probe and they are in a consumer/provider relationship with each other.
            // The proxy entity (internalEntity) and real entity (externalEntity) to merge
            final StitchingEntity internalEntity = stitchingPoint.getInternalEntity();
            final StitchingEntity externalEntity =
                    stitchingPoint.getExternalMatches().iterator().next();

            // log an error if more than one external entity matched a single internal entity
            if (stitchingPoint.getExternalMatches().size() > 1) {
                logger.error("Internal Entity {} matched multiple External Entities: {}",
                        internalEntity.getDisplayName(), stitchingPoint.getExternalMatches()
                                .stream()
                                .map(StitchingEntity::getDisplayName)
                                .collect(Collectors.joining(", ")));
            }
            stitch(internalEntity, externalEntity, resultBuilder);
        });

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

                // remove the provider from the externalEntity
                externalEntityProvider.ifPresent(externalProvider -> {
                    resultBuilder.queueChangeRelationships(externalEntity, toUpdate ->
                            toUpdate.removeProvider(externalProvider));
                    // if the external provider is replaceable, merge it onto the internal
                    // provider and then delete it.  Merging adds the oid and target of the replaced
                    // entity to the entity we are keeping.
                    if (externalProvider.getEntityBuilder().getOrigin()
                            .equals(EntityOrigin.REPLACEABLE)) {
                        resultBuilder.queueEntityMerger(MergeEntities
                                .mergeEntity(externalProvider).onto(provider));
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
                        .onto(externalEntity,
                                new MetaDataAwareMergeCommoditySoldStrategy(
                                        matchingInformation.getCommoditiesSoldToPatch()));
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
    private static class CommoditySoldMergeSpec {

        private boolean ignoreIfPresent;

        private List<DTOFieldSpec> patchedFields;

        CommoditySoldMergeSpec(@Nonnull CommoditySoldMetadata commoditySoldMetadata) {
            this.ignoreIfPresent = commoditySoldMetadata.getIgnoreIfPresent();
            this.patchedFields = commoditySoldMetadata.getPatchedFieldsList().stream()
                .map(entityField -> new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return entityField.getFieldName();
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return entityField.getMessagePathList();
                    }
                }).collect(Collectors.toList());
        }

        boolean isIgnoreIfPresent() {
            return ignoreIfPresent;
        }

        List<DTOFieldSpec> getPatchedFields() {
            return patchedFields;
        }
    }
}




