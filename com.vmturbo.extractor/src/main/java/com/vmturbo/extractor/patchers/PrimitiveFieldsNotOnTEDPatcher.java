package com.vmturbo.extractor.patchers;

import static com.vmturbo.extractor.models.ModelDefinitions.SEVERITY_ENUM;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add basic fields which are not available in TopologyEntityDTO, like: severity.
 */
public class PrimitiveFieldsNotOnTEDPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        // find metadata for primitive fields not on TopologyEntityDTO (no topo function defined)
        final List<SearchMetadataMapping> metadataList =
                SearchMetadataUtils.getMetadata(recordInfo.getEntityType(), FieldType.PRIMITIVE).stream()
                        .filter(m -> m.getTopoFieldFunction() == null)
                        .collect(Collectors.toList());

        final long entityOid = recordInfo.getOid();
        final Map<String, Object> attrs = recordInfo.getAttrs();

        metadataList.forEach(metadata -> {
            final String jsonKey = metadata.getJsonKeyName();
            switch (metadata) {
                case PRIMITIVE_SEVERITY:
                    recordInfo.getRecord().set(SEVERITY_ENUM,
                            dataProvider.getSeverity(recordInfo.getOid()));
                    break;
                case PRIMITIVE_IS_EPHEMERAL_VOLUME:
                    patchIsEphemeralVolume(entityOid, attrs, jsonKey, dataProvider);
                    break;
                case PRIMITIVE_IS_ENCRYPTED_VOLUME:
                    patchIsEncryptedVolume(entityOid, attrs, jsonKey, dataProvider);
                    break;
                default:
                    logger.error("Unsupported primitive metadata: {}", metadata);
            }
        });
    }

    /**
     * Appends map with whether ephemeralVolume exists.
     * @param entityOid oid of entity in fouce
     * @param attrs map for jsonB colums
     * @param jsonKey the attrs.key to store isEphemeral value
     * @param dataProvider fetchedData
     */
    public void patchIsEphemeralVolume(long entityOid, Map<String, Object> attrs, String jsonKey, DataProvider dataProvider) {
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
        attrs.put(jsonKey, isEphemeral);
    }

    /**
     * Appends map with whether encryptedVolume exists.
     * @param entityOid oid of entity in fouce
     * @param attrs map for jsonB colums
     * @param jsonKey the attrs.key to store isEphemeral value
     * @param dataProvider fetchedData
     */
    public void patchIsEncryptedVolume(long entityOid, Map<String, Object> attrs, String jsonKey, DataProvider dataProvider) {
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
        attrs.put(jsonKey, isEncrypted);
    }
}
