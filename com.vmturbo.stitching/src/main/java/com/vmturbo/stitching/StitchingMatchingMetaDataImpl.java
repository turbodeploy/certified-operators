package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;

/**
 * Base implementation of the class that encapsulates the stitching meta data that we get from the
 * supply chain.  This has most of the logic within it to extract the information we need for
 * stitching from the MergedEntityMetadata and implement the StitchingMatchingMetaData interface.
 * Subclasses only need specific logic for dealing with the internal and external signature types.
 *
 * @param <INTERNAL_SIGNATURE_TYPE> The type of the matching signature on the probe (internal) side.
 * @param <EXTERNAL_SIGNATURE_TYPE> The type of the matching signature of the external entity.
 */
public abstract class StitchingMatchingMetaDataImpl<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE>
        implements StitchingMatchingMetaData<INTERNAL_SIGNATURE_TYPE, EXTERNAL_SIGNATURE_TYPE> {
    private static final Logger logger = LogManager.getLogger();

    protected final MergedEntityMetadata mergedEntityMetadata;
    private final EntityType entityType;
    private final Collection<String> patchedPropertiesList;
    private final Collection<DTOFieldSpec> patchedAttributesList;

    public StitchingMatchingMetaDataImpl(@Nonnull final EntityType entityType,
                                         @Nonnull final MergedEntityMetadata mergedEntityMetadata) {
        this.entityType = entityType;
        this.mergedEntityMetadata = mergedEntityMetadata;
        patchedPropertiesList = mergedEntityMetadata.getPatchedPropertiesList().stream()
                .map(EntityPropertyName::getPropertyName)
                .collect(Collectors.toList());
        patchedAttributesList = mergedEntityMetadata.getPatchedFieldsList().stream()
                .map(DTOFieldSpecImpl::new)
                .collect(Collectors.toList());
    }

    @Override
    public EntityType getInternalEntityType() {
        return entityType;
    }

    @Override
    public EntityType getExternalEntityType() {
        return entityType;
    }

    @Override
    public Collection<String> getPropertiesToPatch() {
        return patchedPropertiesList;
    }

    @Override
    public Collection<DTOFieldSpec> getAttributesToPatch() {
        return patchedAttributesList;
    }

    @Override
    public Collection<CommoditySoldMetadata> getCommoditiesSoldToPatch() {
        final List<CommodityType> soldList = mergedEntityMetadata.getCommoditiesSoldList();
        if (!soldList.isEmpty()) {
            // if probe is still using the deprecated field, we should convert it to new field
            // note: this is currently only used for loading old diags, it's not needed for
            // PT or customer, since new probes will register new ProbeInfo with new metadata
            return soldList.stream()
                .map(commodityType -> CommoditySoldMetadata.newBuilder()
                    .setCommodityType(commodityType)
                    .build())
                .collect(Collectors.toList());
        }
        return mergedEntityMetadata.getCommoditiesSoldMetadataList();
    }

    @Override
    public Collection<CommodityBoughtMetadata> getCommoditiesBoughtToPatch() {
        return mergedEntityMetadata.getCommoditiesBoughtList();
    }

    @Override
    public boolean getKeepStandalone() {
        return mergedEntityMetadata.getKeepStandalone();
    }

    @Override
    public Optional<MergedEntityMetadata.StitchingScope> getStitchingScope() {
        if (mergedEntityMetadata.hasStitchingScope()) {
            return Optional.of(mergedEntityMetadata.getStitchingScope());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Convenience method for subclasses to call to parse matching fields or properties that return
     * type collection of {@link String}s.
     *
     * @param matchingData collection of {@link MatchingData} defining fields or
     *                 properties that are {@link String} values to use for matching.
     * @return collection of {@link MatchingPropertyOrField} of type that are providing
     *                 values used to find appropriate entity for stitching process.
     */
    @Nonnull
    protected static Collection<MatchingPropertyOrField<String>> handleStringsMatchingData(
                    @Nonnull Collection<MatchingData> matchingData) {
        final Collection<MatchingPropertyOrField<String>> result =
                        new ArrayList<>(matchingData.size());
        for (MatchingData matching : matchingData) {
            if (matching.hasMatchingProperty()) {
                final boolean hasDelimiter = matching.hasDelimiter();
                final String delimiter = hasDelimiter ? matching.getDelimiter() : null;
                result.add(new MatchingProperty(matching.getMatchingProperty().getPropertyName(),
                                delimiter));
            } else if (matching.hasMatchingEntityOid()) {
                result.add(new MatchingEntityOid());
            } else {
                // TODO We don't handle delimiter separated Strings in MatchingField here.  It's not
                // clear if such fields exist.
                result.add(new MatchingField<>(matching.getMatchingField().getMessagePathList(),
                                matching.getMatchingField().getFieldName()));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                        "%s [mergedEntityMetadata=%s, entityType=%s, patchedPropertiesList=%s, patchedAttributesList=%s]",
                        getClass().getSimpleName(), this.mergedEntityMetadata, this.entityType,
                        this.patchedPropertiesList, this.patchedAttributesList);
    }
}
