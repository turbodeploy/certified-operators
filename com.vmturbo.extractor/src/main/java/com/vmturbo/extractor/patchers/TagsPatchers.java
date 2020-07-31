package com.vmturbo.extractor.patchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;

/**
 * Two patchers to handle tags structures in entity and group protobufs.
 *
 * <p>Eventually these should probably be included in existing {@link PrimitiveFieldsOnTEDPatcher}
 * and {@link GroupPrimitiveFieldsOnGroupingPatcher}, but XLR is getting there before search, and
 * this meets XLR's needs without expanding {@link FieldValueApiDTO.Type}.</p>
 */
public class TagsPatchers {
    private TagsPatchers() {
    }

    /**
     * JSON key for tags values in attrs.
     */
    public static final String TAGS_JSON_KEY_NAME = "tags";

    /**
     * Patcher for tags for entities.
     */
    public static class EntityTagsPatcher implements EntityRecordPatcher<TopologyEntityDTO> {


        @Override
        public void patch(final PartialRecordInfo recordInfo, final TopologyEntityDTO entity) {
            // this should probably be done with a FieldValueApiDTO, but latter's Type enum doesn't
            // have anything that can represent the Map<String, List<String>> we need here, so we'll
            // just grind it out for now
            Optional<Object> tagsValue = entity.hasTags()
                    ? Optional.of(tagsToMap(entity.getTags()))
                    : Optional.empty();
            tagsValue.ifPresent(tags ->
                    recordInfo.putAttrs(TAGS_JSON_KEY_NAME, tags));
        }
    }

    /**
     * Patcher for tags for groups.
     */
    public static class GroupTagsPatcher implements EntityRecordPatcher<Grouping> {


        @Override
        public void patch(final PartialRecordInfo recordInfo, final Grouping group) {
            // this should probably be done with a FieldValueApiDTO, but latter's Type enum doesn't
            // have anything that can represent the Map<String, List<String>> we need here, so we'll
            // just grind it out for now
            Optional<Object> tagsValue = group.getDefinition().hasTags()
                    ? Optional.of(tagsToMap(group.getDefinition().getTags()))
                    : Optional.empty();
            tagsValue.ifPresent(tags ->
                    recordInfo.putAttrs(TAGS_JSON_KEY_NAME, tags));
        }
    }

    private static Map<String, List<String>> tagsToMap(Tags tags) {
        Map<String, List<String>> map = new HashMap<>();
        tags.getTagsMap().forEach((key, values) -> {
            map.computeIfAbsent(key, t -> new ArrayList<>())
                    .addAll(values.getValuesList());
        });
        return map;
    }
}
