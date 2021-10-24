package com.vmturbo.extractor.patchers;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are not available in TopologyEntityDTO, like: severity.
 */
public class PrimitiveFieldsNotOnTEDPatcher extends AbstractPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fetch(PartialEntityInfo recordInfo, DataProvider dataProvider) {
        // find metadata for primitive fields not on TopologyEntityDTO (no topo function defined)
        // TODO static
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.getEntityType(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() == null)
                        .collect(Collectors.toList());

        final long entityOid = recordInfo.getOid();

        metadataList.forEach(metadata -> {
            switch (metadata) {
                case PRIMITIVE_SEVERITY:
                    // TODO fix types when severity is implemented in DataProvider (it is not)
                    recordInfo.putAttr(metadata,
                                    getEnumDbValue(dataProvider.getSeverity(recordInfo.getOid()),
                                                    PatchCase.SEARCH,
                                                    Severity.class,
                                                    e -> e));
                    break;
                case PRIMITIVE_IS_EPHEMERAL_VOLUME:
                    patchIsEphemeralVolume(entityOid, recordInfo, metadata, dataProvider);
                    break;
                case PRIMITIVE_IS_ENCRYPTED_VOLUME:
                    patchIsEncryptedVolume(entityOid, recordInfo, metadata, dataProvider);
                    break;
                default:
                    logger.error("Unsupported primitive metadata: {}", metadata);
            }
        });
    }

    /**
     * Appends map with whether ephemeralVolume exists.
     * @param entityOid oid of entity in fouce
     * @param recordInfo storage for columns
     * @param metadata descriptor for the field
     * @param dataProvider fetchedData
     */
    public void patchIsEphemeralVolume(long entityOid, PartialEntityInfo recordInfo, SearchMetadataMapping metadata,
                    DataProvider dataProvider) {
        Set<Long> virtualVolumeOids = dataProvider.getRelatedEntitiesOfType(entityOid, EntityType.VIRTUAL_VOLUME);
        if (virtualVolumeOids.isEmpty()) {
            return;
        }

        boolean isEphemeral = true;
        for (long virtualVolumeOid : virtualVolumeOids) {
            Boolean vvIsEphemeral = dataProvider.virtualVolumeIsEphemeral(virtualVolumeOid);
            if (vvIsEphemeral == null) {
                return;
            }
            isEphemeral = isEphemeral && vvIsEphemeral;
        }
        recordInfo.putAttr(metadata, isEphemeral ? 1 : 0);
    }

    /**
     * Appends map with whether encryptedVolume exists.
     * @param entityOid oid of entity in fouce
     * @param recordInfo storage for colums
     * @param metadata descriptor for the field
     * @param dataProvider fetchedData
     */
    public void patchIsEncryptedVolume(long entityOid, PartialEntityInfo recordInfo, SearchMetadataMapping metadata,
                    DataProvider dataProvider) {
        Set<Long> virtualVolumeOids = dataProvider.getRelatedEntitiesOfType(entityOid, EntityType.VIRTUAL_VOLUME);
        if (virtualVolumeOids.isEmpty()) {
            return;
        }

        boolean isEncrypted = true;
        for (long virtualVolumeOid : virtualVolumeOids) {
            Boolean vvIsEncrypted = dataProvider.virtualVolumeIsEncrypted(virtualVolumeOid);
            if (vvIsEncrypted == null) {
                return;
            }
            isEncrypted = isEncrypted && vvIsEncrypted;
        }
        recordInfo.putAttr(metadata, isEncrypted ? 1 : 0);
    }
}
