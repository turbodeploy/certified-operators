package com.vmturbo.components.common.utils;

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

/**
 * A utility class for working with {@link Optional}s.
 */
public class OptionalUtils {

    private OptionalUtils() {}

    /**
     * Reduces a set of optionals to a single value.
     * @param reduce The reduce method.
     * @param optionals The {@link Optional} instances to reduce.
     * @param <T> The type wrapped by the {@link Optional} instances.
     * @return A reduced value of {@code T}.
     */
    public static <T> Optional<T> reduce(BinaryOperator<T> reduce,
                                         Optional<T>... optionals) {
        return Stream.of(optionals)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(reduce);
    }
}
