package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.tag.Tag.Tags;

/**
 * An index to support tag searches.
 *
 * <p/>We use an index instead of entity-specific tags to reduce the memory footprints of tags
 * on a large topologies.
 */
public interface TagIndex {

    /**
     * Check if a particular entity matches a tag filter.
     *
     * @param entityId The id of the entity.
     * @param mapFilter The tag filter (contains filters on keys and values).
     * @return Whether or not the entity matches the filter.
     */
    boolean isMatchingEntity(long entityId, MapFilter mapFilter);

    /**
     * Get all entities that match a particular filter.
     * This can be used to optimize a search on the full topology - it will be faster
     * than going over each entity and calling {@link DefaultTagIndex#isMatchingEntity(long, MapFilter)}.
     *
     * @param mapFilter The tag filter.
     * @param predicate Additional predicate for potentially matching entities.
     * @return The set of all OIDs that match the filters.
     */
    @Nonnull
    LongSet getMatchingEntities(@Nonnull MapFilter mapFilter, @Nonnull LongPredicate predicate);

    /**
     * See {@link TagIndex#getMatchingEntities(MapFilter, LongPredicate)}.
     *
     * @param mapFilter The tag filter.
     * @return The set of all OIDs that match the filter.
     */
    @Nonnull
    default LongSet getMatchingEntities(@Nonnull MapFilter mapFilter) {
        return getMatchingEntities(mapFilter, e -> true);
    }

    /**
     * The default (and, at the time of this writing, the only) tag index implementation.
     * This class is thread-safe once all tags have been added and "finish" has been called.
     */
    class DefaultTagIndex implements TagIndex {

        /**
         * (key) -> (value) -> (entities that have this key-value tag).
         */
        private final Map<String, Map<String, LongSet>> tags = new HashMap<>();

        /**
         * The list of "key=value" strings. Initialized lazily, if we have a search request that
         * requires this joined string.
         *
         * <p/>Because the lazy initialization happens after the Tag index is fully constructed,
         * and is being used to respond to remote calls, we need an atomic reference for thread safety.
         */
        private final AtomicReference<List<String>> joinedKvPairs = new AtomicReference<>();

        /**
         * Finish - meaning no more tags will be added.
         */
        public void finish() {
            tags.values().forEach(vals -> vals.values().forEach(entitySet -> ((LongOpenHashSet)entitySet).trim()));
        }

        /**
         * Create a tag index for a single entity. Utility method.
         *
         * @param entityId The id of the entity.
         * @param tags The tags for the entity.
         * @return A {@link TagIndex}.
         */
        @Nonnull
        public static DefaultTagIndex singleEntity(long entityId, @Nonnull final Tags tags) {
            DefaultTagIndex tagIndex = new DefaultTagIndex();
            tagIndex.addTags(entityId, tags);
            tagIndex.finish();
            return tagIndex;
        }

        /**
         * Record the tags on a specific entity.
         *
         * @param entityId The oid of the entity.
         * @param tags     The tags on the entity.
         */
        public void addTags(final long entityId, @Nonnull final Tags tags) {
            tags.getTagsMap().forEach((key, vals) -> {
                final Map<String, LongSet> valuesMap = this.tags.computeIfAbsent(key, k -> new HashMap<>());
                vals.getValuesList().forEach(val -> {
                    valuesMap.computeIfAbsent(val, v -> new LongOpenHashSet()).add(entityId);
                });
            });
        }

        /**
         * Get the tags on a set of entities.
         *
         * @param entities The OIDs of the entities.
         * @return The tags (expressed as key -> set(value)).
         */
        @Nonnull
        public Map<String, Set<String>> getTagsForEntities(@Nonnull final LongSet entities) {
            return getTags(entitiesWithTag -> {
                // Iterate over the smaller set, look up in the bigger set.
                final LongSet smallerSet;
                final LongSet largerSet;
                if (entitiesWithTag.size() > entities.size()) {
                    smallerSet = entities;
                    largerSet = entitiesWithTag;
                } else {
                    smallerSet = entitiesWithTag;
                    largerSet = entities;
                }
                LongIterator longIt = smallerSet.iterator();
                while (longIt.hasNext()) {
                    long nextId = longIt.nextLong();
                    if (largerSet.contains(nextId)) {
                        return true;
                    }
                }
                return false;
            });
        }

        /**
         * Get unmodifiable map of entities by tag.
         *
         * @param tagKey tag
         * @return unmodifiable map
         */
        @Nonnull
        public Map<String, LongSet> getEntitiesByValueMap(@Nonnull String tagKey) {
            return Collections.unmodifiableMap(tags.getOrDefault(tagKey, Collections.emptyMap()));
        }

        /**
         * Get the tags on a set of entities, arranged by entity ID.
         *
         * @param entities The OIDs of the entities.
         * @return The tags (expressed as key -> set(value)).
         */
        @Nonnull
        public Long2ObjectMap<Map<String, Set<String>>> getTagsByEntity(@Nonnull final LongSet entities) {
            Long2ObjectMap<Map<String, Set<String>>> ret = new Long2ObjectOpenHashMap<>();
            tags.forEach((key, vals) -> {
                vals.forEach((val, entitiesWithTag) -> {
                    final LongSet smallerSet;
                    final LongSet largerSet;
                    if (entitiesWithTag.size() > entities.size()) {
                        smallerSet = entities;
                        largerSet = entitiesWithTag;
                    } else {
                        smallerSet = entitiesWithTag;
                        largerSet = entities;
                    }
                    smallerSet.forEach((LongConsumer)e -> {
                        if (largerSet.contains(e)) {
                            ret.computeIfAbsent(e, k -> new HashMap<>())
                                .computeIfAbsent(key, k -> new HashSet<>()).add(val);
                        }
                    });
                });
            });
            return ret;
        }

        /**
         * Get the tags on a single entity.
         *
         * @param entityId The OID of the entity.
         * @return The tags (expressed as key -> set(value)).
         */
        @Nonnull
        public Map<String, Set<String>> getTagsForEntity(final long entityId) {
            return getTags(entitiesWithTag -> entitiesWithTag.contains(entityId));
        }

        @Nonnull
        private Map<String, Set<String>> getTags(@Nonnull final Predicate<LongSet> test) {
            final Map<String, Set<String>> ret = new HashMap<>();
            tags.forEach((key, vals) -> {
                vals.forEach((val, entitiesWithTag) -> {
                    if (test.test(entitiesWithTag)) {
                        ret.computeIfAbsent(key, k -> new HashSet<>()).add(val);
                    }
                });
            });
            return ret;
        }

        @Override
        @Nonnull
        public LongSet getMatchingEntities(@Nonnull MapFilter mapFilter, @Nonnull LongPredicate entityPredicate) {
            LongSet retSet = new LongOpenHashSet();
            if (StringUtils.isEmpty(mapFilter.getKey())) {
                // Loop over all key-value pairs, create the joined "key=value" string, and
                // check for regex matching.
                if (!StringUtils.isEmpty(mapFilter.getRegex())) {
                    final Pattern pattern = Pattern.compile(mapFilter.getRegex());
                    // We don't cache the joined pairs here, because we just need to join each
                    // key-value pair once for the whole topology.
                    tags.forEach((key, valsToEntities) -> {
                        valsToEntities.forEach((val, entities) -> {
                            String keyVal = key + "=" + val;
                            if (pattern.matcher(keyVal).matches()) {
                                entities.forEach((LongConsumer)eId -> {
                                    if (entityPredicate.test(eId)) {
                                        retSet.add(eId);
                                    }
                                });
                            }
                        });
                    });
                }
            } else {
                // Look for a specific key.
                final Map<String, LongSet> valuesMap = tags.getOrDefault(mapFilter.getKey(), Collections.emptyMap());
                if (!mapFilter.getValuesList().isEmpty()) {
                    // Look for exact values matches.
                    mapFilter.getValuesList().forEach(value -> {
                        LongSet matchingEntities = valuesMap.get(value);
                        if (matchingEntities != null) {
                            retSet.addAll(matchingEntities);
                        }
                    });
                } else if (!StringUtils.isEmpty(mapFilter.getRegex())) {
                    // Value must match regex.
                    final Pattern pattern = Pattern.compile(mapFilter.getRegex());
                    valuesMap.forEach((val, entities) -> {
                        if (pattern.matcher(val).matches()) {
                            retSet.addAll(entities);
                        }
                    });
                } else {
                    // no values or regex provided, look for all values for the given key
                    valuesMap.values().forEach(retSet::addAll);
                }
            }
            return retSet;
        }

        @Override
        public boolean isMatchingEntity(final long entityId, @Nonnull final MapFilter mapFilter) {
            if (StringUtils.isEmpty(mapFilter.getKey())) {
                return isMatchingJoinedRegex(entityId, mapFilter.getRegex());
            } else {
                // key is present in the filter
                // key must match and value must satisfy a specific predicate
                Map<String, LongSet> valueMap = tags.get(mapFilter.getKey());
                if (valueMap == null) {
                    return false;
                } else if (!mapFilter.getValuesList().isEmpty()) {
                    return mapFilter.getValuesList().stream().anyMatch(val -> {
                        LongSet entitiesWithTag = valueMap.get(val);
                        return entitiesWithTag != null && entitiesWithTag.contains(entityId);
                    });
                } else if (!StringUtils.isEmpty(mapFilter.getRegex())) {
                    final Pattern pattern = Pattern.compile(mapFilter.getRegex());
                    for (Map.Entry<String, LongSet> entry : valueMap.entrySet()) {
                        if (pattern.matcher(entry.getKey()).matches() && entry.getValue().contains(entityId)) {
                            return true;
                        }
                    }
                } else {
                    return true;
                }
            }
            return false;
        }

        private boolean isMatchingJoinedRegex(final long entityId, @Nonnull final String regex) {
            // This is the most expensive case - if there is no explicit key set in the request,
            // we take the regex in the request and match it against all
            // possible "key=value" string combinations.
            //
            // We cache the combined pairs, because they will be the same for all entities.
            List<String> joinedPairs = joinedKvPairs.get();
            if (joinedPairs == null) {
                final List<String> newPairs = new ArrayList<>();
                tags.forEach((key, valsToEntities) -> {
                    valsToEntities.keySet().forEach(val -> {
                        newPairs.add(key + "=" + val);
                    });
                });
                joinedKvPairs.set(newPairs);
                joinedPairs = newPairs;
            }

            final Pattern pattern = Pattern.compile(regex);
            for (String joinedPair : joinedPairs) {
                if (pattern.matcher(joinedPair).matches()) {
                    final String[] keyAndVal = joinedPair.split("=");
                    final LongSet entitiesWithTag =
                            tags.getOrDefault(keyAndVal[0], Collections.emptyMap()).get(keyAndVal[1]);
                    if (entitiesWithTag != null && entitiesWithTag.contains(entityId)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
