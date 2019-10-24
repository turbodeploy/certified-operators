package com.vmturbo.trax;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import com.vmturbo.trax.Trax.TraxCalculation;
import com.vmturbo.trax.Trax.TraxInfixOperation;
import com.vmturbo.trax.Trax.TraxIntermediateTrackingCalculation;
import com.vmturbo.trax.Trax.TraxTrackable;

/**
 * Utilities for creating implementions of the {@link Collector} interface for streams of
 * {@link TraxNumber}s.
 */
public class TraxCollectors {

    /**
     * Hide constructor for utility class.
     */
    private TraxCollectors() {

    }

    /**
     * Collect a stream of {@link TraxNumber}s by summing them.
     *
     * @return A {@link Collector} for summing a stream of {@link TraxNumber}s.
     */
    public static Collector<TraxNumber, ?, TraxNumber> sum() {
        return sum(null);
    }

    /**
     * Collect a stream of {@link TraxNumber}s by summing them.
     *
     * @param resultName The name of the resulting {@link TraxNumber} that results from the sum.
     * @return A {@link Collector} for summing a stream of {@link TraxNumber}s.
     */
    public static Collector<TraxNumber, ?, TraxNumber> sum(@Nullable final String resultName) {
        return new TraxNumberSummer(resultName);
    }

    /**
     * A base type for collecting {@link TraxNumber}s in stream operations.
     *
     * @param <COLLECTOR_TYPE> The type of the intermediate object performing collection
     *                         (ie a sum or a product, etc.)
     */
    private abstract static class TraxNumberCollector<COLLECTOR_TYPE extends OngoingCollection>
        implements Collector<TraxNumber, COLLECTOR_TYPE, TraxNumber> {
        final String resultName;

        /**
         * Create a new {@link TraxNumberCollector}.
         *
         * @param resultName The name of the resulting {@link TraxNumber}.
         */
        TraxNumberCollector(@Nullable final String resultName) {
            this.resultName = resultName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public BiConsumer<COLLECTOR_TYPE, TraxNumber> accumulator() {
            return OngoingCollection::add;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Function<COLLECTOR_TYPE, TraxNumber> finisher() {
            return OngoingCollection::finishCollection;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public BinaryOperator<COLLECTOR_TYPE> combiner() {
            return OngoingCollection::combine;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Set<Characteristics> characteristics() {
            return Sets.immutableEnumSet(Characteristics.UNORDERED);
        }
    }

    /**
     * A {@link com.vmturbo.trax.TraxCollectors.TraxNumberSummer} can be used to sum up
     * a stream of {@link TraxNumber}s.
     */
    private static class TraxNumberSummer extends TraxNumberCollector<OngoingSummation> {
        TraxNumberSummer(@Nullable final String resultName) {
            super(resultName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Supplier<OngoingSummation> supplier() {
            return () -> new OngoingSummation(resultName);
        }
    }

    /**
     * An {@link com.vmturbo.trax.TraxCollectors.OngoingCollection} stores intermediate results
     * when collecting over a stream of {@link TraxNumber}s.
     */
    private static class OngoingCollection {
        protected final List<TraxTrackable> components = new ArrayList<>();
        protected double result = 0;
        protected final DoubleBinaryOperator operator;
        protected final TraxInfixOperation connectorOperation;

        private final String resultName;
        private final boolean trackingEnabled;

        /**
         * Create a new {@link com.vmturbo.trax.TraxCollectors.OngoingCollection}.
         *
         * @param resultName The name of the resulting {@link TraxNumber} created by the call
         *                   to {@link #finishCollection()}.
         * @param operation The operation connecting the elements being collected.
         * @param operator A {@link DoubleBinaryOperator} for use to collect intermediate results
         *                 (ie add two numbers, multiply two numbers, etc.)
         */
        OngoingCollection(@Nullable final String resultName,
                                 @Nonnull final TraxInfixOperation operation,
                                 @Nonnull final DoubleBinaryOperator operator) {
            this.resultName = resultName;
            this.connectorOperation = Objects.requireNonNull(operation);
            this.operator = Objects.requireNonNull(operator);
            this.trackingEnabled = TraxConfiguration.on(Thread.currentThread());
        }

        /**
         * Finish the {@link com.vmturbo.trax.TraxCollectors.OngoingCollection}
         * when all {@link TraxNumber}s in the stream are collected.
         *
         * @return The resulting {@link TraxNumber} by applying the operator to all numbers
         *         in the collection.
         */
        TraxNumber finishCollection() {
            if (trackingEnabled) {
                final TraxCalculation calculation = new TraxCalculation(components);
                final TraxIntermediateTrackingCalculation numberCalc =
                    new TraxIntermediateTrackingCalculation(result, calculation);
                return (resultName == null) ? numberCalc.compute() : numberCalc.compute(resultName);
            } else {
                return new TraxNumber(result);
            }
        }

        /**
         * Add a new {@link TraxNumber} to the ongoing collection.
         *
         * @param number The number to add to the ongoing collection.
         */
        void add(@Nonnull final TraxNumber number) {
            result = operator.applyAsDouble(result, number.getValue());
            if (trackingEnabled) {
                if (!components.isEmpty()) {
                    components.add(connectorOperation);
                }

                components.add(number);
            }
        }

        /**
         * Combine two {@link com.vmturbo.trax.TraxCollectors.OngoingCollection}s.
         *
         * @param first The first collection to combine.
         * @param second The second collection to combine.
         * @param <T> The specific types of the {@link com.vmturbo.trax.TraxCollectors.OngoingCollection}s.
         * @return A new {@link com.vmturbo.trax.TraxCollectors.OngoingCollection} representing the combination
         *         of the first and second collections.
         */
        static <T extends OngoingCollection> T combine(@Nonnull final T first,
                                                       @Nonnull final T second) {
            first.result = first.operator.applyAsDouble(first.result, second.result);
            if (!first.components.isEmpty()) {
                first.components.add(first.connectorOperation);
            }

            first.components.addAll(second.components);
            return first;
        }
    }

    /**
     * Create a new {@link com.vmturbo.trax.TraxCollectors.OngoingSummation} to sum the {@link TraxNumber}s
     * in a stream.
     */
    private static class OngoingSummation extends OngoingCollection {
        OngoingSummation(@Nullable final String resultName) {
            super(resultName, TraxInfixOperation.PLUS, (a, b) -> a + b);
        }
    }

    /**
     * Create a new {@link com.vmturbo.trax.TraxCollectors.OngoingProduct} to multiply the {@link TraxNumber}s
     * in a stream. Note that this does not necessarily
     */
    private static class OngoingProduct extends OngoingCollection {
        OngoingProduct(@Nullable final String resultName) {
            super(resultName, TraxInfixOperation.TIMES, (a, b) -> a * b);
        }
    }
}
