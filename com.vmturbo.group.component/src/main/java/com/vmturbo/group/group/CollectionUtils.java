package com.vmturbo.group.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Utility class to operate with collections.
 */
public class CollectionUtils {

    private CollectionUtils() {}

    /**
     * Method returns a sorted list from the initial collection taking care of objects
     * inter-dependency in the collection. So, the sequence of elements will be suitable for
     * creating objects from scratch. The resulting list will be started with elements that
     * have no dependencies on other elements.
     *
     * <p>For example, if you have a collection of groups with static subgroups, you cannot create
     * groups which have non-existing subgroups - you have to create subgroups first. In this
     * regard parent group depends on child groups.
     *
     * @param collection initial collection to sort.
     * @param dependencyExtractor returns a collection of elements that requested element
     *         depends on
     * @param <T> type of elements in a collection
     * @return a new editable list of elements in the sorted manner. Elements with no dependencies
     *         come first
     */
    @Nonnull
    public static <T> List<T> sortWithDependencies(@Nonnull Collection<T> collection,
            @Nonnull Function<T, Collection<T>> dependencyExtractor) {
        return sortWithDependencies(collection, Function.identity(), dependencyExtractor);
    }

    /**
     * Method returns a sorted list from the initial collection taking care of objects
     * inter-dependency in the collection. So, the sequence of elements will be suitable for
     * creating objects from scratch. The resulting list will be started with elements that
     * have no dependencies on other elements.
     *
     * <p>For example, if you have a collection of groups with static subgroups, you cannot create
     * groups which have non-existing subgroups - you have to create subgroups first. In this
     * regard parent group depends on child groups.
     *
     * @param collection initial collection to sort.
     * @param valueExtractor value extractor. It will convert the collection element to the
     *         same type as {@code dependencyExtractor} operates with
     * @param dependencyExtractor returns a collection of elements that requested element
     *         depends on
     * @param <T> type of elements in a collection
     * @param <V> type of values used to calculate dependencies.
     * @return a new editable list of elements in the sorted manner. Elements with no dependencies
     *         come first
     */
    @Nonnull
    public static <T, V> List<T> sortWithDependencies(@Nonnull Collection<T> collection,
            @Nonnull Function<T, V> valueExtractor,
            @Nonnull Function<T, Collection<V>> dependencyExtractor) {
        final Set<V> addedObjects = new HashSet<>(collection.size());
        final List<T> result = new ArrayList<>(collection.size());
        final Collection<T> initialCollection = new LinkedList<>(collection);
        int valuesChanged = 1;
        while (valuesChanged > 0 && !initialCollection.isEmpty()) {
            valuesChanged = 0;
            final Iterator<T> iter = initialCollection.iterator();
            while (iter.hasNext()) {
                final T nextObject = iter.next();
                final Collection<V> dependsOn = dependencyExtractor.apply(nextObject);
                if (addedObjects.containsAll(dependsOn)) {
                    final V value = valueExtractor.apply(nextObject);
                    addedObjects.add(value);
                    result.add(nextObject);
                    iter.remove();
                    valuesChanged++;
                }
            }
        }
        if (!initialCollection.isEmpty()) {
            throw new IllegalArgumentException(
                    "Source collection has cyclic or broken dependencies: "
                            + initialCollection.stream()
                            .map(value -> value + "->" + dependencyExtractor.apply(value))
                            .collect(Collectors.joining(", ")));
        }
        return result;
    }
}
