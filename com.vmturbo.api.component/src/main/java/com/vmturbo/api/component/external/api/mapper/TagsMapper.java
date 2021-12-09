package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.TagFilter;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;

/**
 * Utility class to convert tags from XL to API format.
 */
public class TagsMapper {
    /**
     * Convert one tag key and its values in XL format into a {@link TagApiDTO} object.
     *
     * @param key the tag key.
     * @param values the tag values wrapped in a {@link TagValuesDTO} object.
     * @return a {@link TagApiDTO} translation.
     */
    @Nonnull
    public static TagApiDTO convertTagToApi(@Nonnull String key, @Nonnull TagValuesDTO values) {
        final TagApiDTO result = new TagApiDTO();
        result.setKey(key);
        result.setValues(values.getValuesList());
        return result;
    }

    /**
     * Convert one tag map in XL format into a list of {@link TagApiDTO} objects,
     * as expected by the APO.  The XL format is a map
     * from strings to {@link TagValuesDTO} objects.
     *
     * @param tags the tags map.
     * @return a list of {@link TagApiDTO} objects.
     */
    @Nonnull
    public static List<TagApiDTO> convertTagsToApi(@Nonnull Map<String, TagValuesDTO> tags) {
        return
            tags.entrySet().stream()
                .map(e -> convertTagToApi(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Convert {@link TagApiDTO} object to {@link TagFilter} used by Cost component service to
     * retrieve cloud billed stats.
     *
     * @param tag Tag DTO.
     * @return Tag filter instance.
     */
    public static TagFilter convertTagToTagFilter(@Nonnull final TagApiDTO tag) {
        return TagFilter.newBuilder()
                .setTagKey(tag.getKey())
                .addAllTagValue(tag.getValues())
                .build();
    }
}
