package com.vmturbo.extractor.export;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Extract those type specific info from entity which are defined in search metadata, same as
 * what we are doing for reporting.
 */
public class AttrsExtractor {

    /**
     * Metadata list for primitive fields on TopologyEntityDTO (there is topo function defined),
     * grouped by entity type.
     */
    private static final Map<Integer, List<SearchMetadataMapping>> ATTRS_METADATA_BY_ENTITY_TYPE =
            Arrays.stream(EntityType.values())
                    .map(entityType -> {
                        final List<SearchMetadataMapping> attrsMetadata =
                                SearchMetadataUtils.getMetadata(entityType.getNumber(), FieldType.PRIMITIVE)
                                        .stream()
                                        .filter(m -> m.getTopoFieldFunction() != null)
                                        .filter(m -> m.getJsonKeyName() != null)
                                        .collect(Collectors.toList());
                        return new Pair<>(entityType.getNumber(), attrsMetadata);
                    }).filter(pair -> !pair.second.isEmpty())
                    .collect(Collectors.toMap(pair -> pair.first, pair -> pair.second));

    /**
     * Extract attributes from given entity.
     *
     * @param e TopologyEntityDTO
     * @return mapping from entity type to another mapping of attrs by name
     */
    @Nullable
    public Map<String, Object> extractAttrs(@Nonnull TopologyEntityDTO e) {
        final List<SearchMetadataMapping> attrsMetadata = ATTRS_METADATA_BY_ENTITY_TYPE.get(e.getEntityType());
        if (attrsMetadata == null || attrsMetadata.isEmpty()) {
            // no attrs for this entity type
            return null;
        }

        final Map<String, Object> attrs = new HashMap<>();
        attrsMetadata.forEach(metadata ->
                metadata.getTopoFieldFunction().apply(e).ifPresent(value ->
                        attrs.put(metadata.getJsonKeyName(), value)));
        return attrs.isEmpty() ? null : attrs;
    }
}
