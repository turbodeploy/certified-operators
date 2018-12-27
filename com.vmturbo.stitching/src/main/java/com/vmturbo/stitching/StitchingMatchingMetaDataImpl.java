package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
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

    private final EntityType entityType;
    protected final MergedEntityMetadata mergedEntityMetadata;
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
                .map(entityField -> new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return entityField.getFieldName();
                    }

                    @Override
                    public List<String> getMessagePath() {
                        // path may be empty since the field may be in EntityDTO directly, no nested layers
                        return entityField.getMessagePathList().isEmpty() ? Collections.emptyList()
                                : entityField.getMessagePathList().subList(0, entityField.getMessagePathCount());
                    }
                })
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
    public Collection<CommodityType> getCommoditiesSoldToPatch() {
        return mergedEntityMetadata.getCommoditiesSoldList();
    }

    @Override
    public Collection<CommodityBoughtMetadata> getCommoditiesBoughtToPatch() {
        return mergedEntityMetadata.getCommoditiesBoughtList();
    }

    @Override
    public boolean getKeepStandalone() {
        return mergedEntityMetadata.getKeepStandalone();
    }


    /**
     * Convenience method for subclasses to call to parse matching fields or properties that return
     * type String.
     *
     * @param matchingDataList {@link List} of {@link MatchingData} defining fields or properties
     *                                     that are {@link String} values to use for matching.
     * @return {@link MatchingPropertyOrField} of type {@link String} representing the
     * matchingDataList.
     */
    protected static List<MatchingPropertyOrField<String>> handleStringMatchingData(
            List<MatchingData> matchingDataList) {
        List<MatchingPropertyOrField<String>> retVal = Lists.newArrayList();
        for (MatchingData matchingData : matchingDataList) {
            if (matchingData.hasMatchingProperty()) {
                retVal.add(new StringMatchingProperty(
                        matchingData.getMatchingProperty().getPropertyName()));
            } else {
                retVal.add(new MatchingField<>(
                        matchingData.getMatchingField().getMessagePathList(),
                        matchingData.getMatchingField().getFieldName()));
            }
        }
        return retVal;
    }


    /**
     * Convenience method for subclasses to call to parse matching fields or properties that return
     * type {@link List} of {@link String}.
     *
     * @param matchingDataList {@link List} of {@link MatchingData} defining fields or properties
     *                                     that are {@link String} values to use for matching.
     * @return {@link MatchingPropertyOrField} of type type {@link List} of {@link String}
     * representing the matchingDataList.
     */
    protected static List<MatchingPropertyOrField<List<String>>> handleListStringMatchingData(
            List<MatchingData> matchingDataList) {
        List<MatchingPropertyOrField<List<String>>> retVal = Lists.newArrayList();
        for (MatchingData matchingData : matchingDataList) {
            if (matchingData.hasMatchingProperty()) {
                if (!matchingData.hasDelimiter()) {
                    logger.error("List<String> matching property must specify delimiter."
                    + " No delimiter specified for matching property {}",
                            matchingData.getMatchingProperty().getPropertyName());
                    continue;
                }
                retVal.add(new ListStringMatchingProperty(
                        matchingData.getMatchingProperty().getPropertyName(),
                        matchingData.getDelimiter()));
            } else {
                // TODO We don't handle delimiter separated Strings in MatchingField here.  It's not
                // clear if such fields exist.
                retVal.add(new MatchingField<>(
                        matchingData.getMatchingField().getMessagePathList(),
                        matchingData.getMatchingField().getFieldName()));
            }
        }
        return retVal;
    }
}
