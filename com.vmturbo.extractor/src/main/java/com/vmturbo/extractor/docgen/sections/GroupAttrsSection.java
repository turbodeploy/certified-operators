package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.GROUP_ATTRS_SECTION_NAME;
import static com.vmturbo.extractor.docgen.DocGenUtils.GROUP_ATTRS_SECTION_PATH;
import static com.vmturbo.extractor.docgen.DocGenUtils.SUPPORTED_GROUP_TYPES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Documentation section generator for Group Attributes. This section documents values that may
 * appear in the JSONB `entity.attrs` table column and also the attrs field in
 * {@link com.vmturbo.extractor.schema.json.export.Group}.
 */
public class GroupAttrsSection extends BaseAttrsSection<GroupType> {

    /**
     * Create a new instance.
     *
     * @param docTree JSON structure with doc snippets to inject into the doc
     */
    public GroupAttrsSection(JsonNode docTree) {
        super(GROUP_ATTRS_SECTION_NAME, GROUP_ATTRS_SECTION_PATH, docTree);
    }

    @Override
    public Map<SearchMetadataMapping, List<GroupType>> getJsonKeyToSupportedTypes() {
        Map<SearchMetadataMapping, List<GroupType>> jsonKeyToSupportedGroupTypes = new HashMap<>();
        GroupPrimitiveFieldsOnGroupingPatcher.getJsonbColumnMetadataByGroupType().forEach((key, value) -> {
            value.forEach(mapping -> {
                jsonKeyToSupportedGroupTypes.computeIfAbsent(mapping,
                        k -> new ArrayList<>()).add(key);
            });
        });
        return jsonKeyToSupportedGroupTypes;
    }

    @Override
    String typeToString(GroupType type) {
        return ExportUtils.getGroupTypeJsonKey(type);
    }

    @Override
    GroupType[] getAllTypes() {
        return GroupType.values();
    }

    @Override
    String getSupportedTypeField() {
        return SUPPORTED_GROUP_TYPES;
    }
}

