package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;

/**
 * Utility class with static methods to facilitate the creation of searches and filters.
 */
public class SearchMapper {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a map filter for the specified property name and
     * specified expression field coming from the UI.
     *
     * The form of the value of the expression field is expected to be
     * "k=v1|k=v2|...", where k is the key and v1, v2, ... are the possible values.
     * If the expression does not conform to the expected format,
     * then a filter with empty key and values fields is generated.
     *
     * TODO: the expression value coming from the UI is currently unsanitized.
     * It is assumed that the tag keys and values do not contain the characters
     * '=' and '|'.  This is reported as a JIRA issue OM-39039.
     *
     * The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].
     *
     * @param propName property name to use for the search.
     * @param expField expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    public static PropertyFilter mapPropertyFilterForMultimapsExact(
            @Nonnull String propName, @Nonnull String expField, boolean positiveMatch
    ) {
        final String[] keyValuePairs = expField.split("\\|");
        String key = null;
        final List<String> values = new ArrayList<>();
        for (String kvp : keyValuePairs) {
            final String[] kv = kvp.split("=");
            if (key == null) {
                // For exact matches, remove any double-escape characters.
                key = kv[0].replaceAll("\\\\", "");
            } else if (!key.equals(kv[0])) {
                logger.error("Map filter {} contains more than one keys.", expField);
                return emptyMapPropertyFilter(propName);
            }
            if (kv.length == 2 && !kv[1].isEmpty()) {
                // For exact matches, remove any double-escape characters.
                values.add(kv[1].replaceAll("\\\\", ""));
            }
        }


        final PropertyFilter result = PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setMapFilter(MapFilter.newBuilder()
                .setKey(key == null ? "" : key)
                .addAllValues(values)
                .setPositiveMatch(positiveMatch)
                .build())
            .build();
        logger.debug("Property filter constructed: {}", result);
        return result;
    }

    /**
     * Create a map filter for the specified property name
     * and specified regex coming from the UI.
     *
     * This filter should match values to the regex expression.
     *
     * TODO: the expression value coming from the UI is currently unsanitized.
     * It is assumed that the tag keys and values do not contain the characters '=' and '|'.
     * This is reported as a JIRA issue OM-39039.
     *
     * The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].
     *
     * @param key property name to use for the search.
     * @param regex expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    public static PropertyFilter mapPropertyFilterForMultimapsRegex(
            @Nonnull String propName, @Nonnull String key, @Nonnull String regex, boolean positiveMatch) {
        final PropertyFilter result = PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setMapFilter(MapFilter.newBuilder()
                // The key is not a regex, so remove backslashes.
                .setKey(key.replaceAll("\\\\", ""))
                .setRegex(SearchProtoUtil.makeFullRegex(regex))
                .setPositiveMatch(positiveMatch)
                .build())
            .build();
        logger.debug("Property filter constructed: {}", result);

        return result;
    }

    @Nonnull
    private static PropertyFilter emptyMapPropertyFilter(String propName) {
        return
            PropertyFilter.newBuilder()
                .setPropertyName(propName)
                .setMapFilter(
                    MapFilter.newBuilder().setKey("").build())
                .build();
    }

    /**
     * Create a discoveredBy for the se, based on the given target id and probe type map.
     *
     * @param targetId id of the target
     * @param targetIdToProbeType the map from target id to probe type
     * @return TargetApiDTO which represents the discoveredBy field of se
     */
    public static TargetApiDTO createDiscoveredBy(@Nonnull String targetId,
                                                  @Nonnull Map<Long, String> targetIdToProbeType) {
        final TargetApiDTO discoveredBy = new TargetApiDTO();
        discoveredBy.setUuid(targetId);
        discoveredBy.setType(targetIdToProbeType.get(Long.valueOf(targetId)));
        return discoveredBy;
    }

    private static final ImmutableList<UIEntityType> EXCLUDE_FROM_SEARCH_ALL =
                    ImmutableList.of(UIEntityType.INTERNET, UIEntityType.HYPERVISOR_SERVER, UIEntityType.UNKNOWN);

    public static final List<String> SEARCH_ALL_TYPES = Stream.of(UIEntityType.values())
                    .filter(e -> !EXCLUDE_FROM_SEARCH_ALL.contains(e))
                    .map(UIEntityType::apiStr)
                    .collect(Collectors.toList());
}
