package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.ENTITY_ATTRS_SECTION_NAME;
import static com.vmturbo.extractor.docgen.DocGenUtils.ENTITY_ATTRS_SECTION_PATH;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Documentation section generator for Entity Attributes. This section documents values that may
 * appear in the JSONB `entity.attrs` table column and also the attrs field in
 * {@link com.vmturbo.extractor.schema.json.export.Entity}.
 */
public class EntityAttrsSection extends BaseAttrsSection<EntityType> {

    /**
     * Create a new instance.
     *
     * @param docTree JSON structure with doc snippets to inject into the doc
     */
    public EntityAttrsSection(JsonNode docTree) {
        super(ENTITY_ATTRS_SECTION_NAME, ENTITY_ATTRS_SECTION_PATH, docTree);
    }

    @Override
    public Map<SearchMetadataMapping, List<EntityType>> getJsonKeyToSupportedTypes() {
        Map<SearchMetadataMapping, List<EntityType>> jsonKeyToSupportedEntityTypes = new HashMap<>();
        PrimitiveFieldsOnTEDPatcher.getJsonbColumnMetadataByEntityType().forEach((key, value) -> {
            final EntityType entity_type = EntityType.forNumber(key);
            value.forEach(mapping -> {
                jsonKeyToSupportedEntityTypes.computeIfAbsent(mapping,
                        k -> new ArrayList<>()).add(entity_type);
            });
        });
        return jsonKeyToSupportedEntityTypes;
    }

    @Override
    String typeToString(EntityType type) {
        return ExportUtils.getEntityTypeJsonKey(type.getNumber());
    }

    @Override
    EntityType[] getAllTypes() {
        return EntityType.values();
    }
}

