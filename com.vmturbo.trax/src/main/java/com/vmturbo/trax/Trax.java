package com.vmturbo.trax;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.components.api.StackTrace;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxUseLimit;
import com.vmturbo.trax.TraxNumber.TraxConstantNumber;
import com.vmturbo.trax.TraxNumber.TraxTrackingNumber;

/**
 * TRAX: Calculation Tracking library.
 * Use {@link Trax} factory and helper methods to trace the elements of a calculation as
 * the calculation is composed.
 * <p/>
 * Consider the following calculation of a {@link TraxNumber}:
 * {@code TraxNumber addition = trax(5.0, "foo").plus(10.0, "bar").compute("addition")}
 * <p/>
 * then (when configured at DEBUG level verbosity) the result of {@code addition#calculationStack()}
 * will be:
 * <p/>
 * assertEquals(
 *     "15[addition] = 5[foo] + 10[bar]\n" +
 *     "├── 5[foo]\n" +
 *     "└── 10[bar]\n",
 *         addition.calculationStack());
 * <p/>
 * Usually, it's not necessary to do this sort of deep inspection of a particular calculation
 * result (although when it is necessary, it is usually incredibly valuable to be able to do so).
 * In cases where a calculation is not configured for debugging or tracing, we want
 * to introduce the minimum possible performance overhead in performing the actual calculation.
 * As such, a bare {@link TraxNumber} is a thin wrapper around a primitive double value. We only
 * retain additional information when {@link Trax} tracking has been configured to debug
 * a given calculation.
 * <p/>
 * Perform arithmetic on {@link TraxNumber}s with the arithmetic operators such as
 * {@link TraxNumber#plus(double)}, {@link TraxNumber#minus(double)},
 * {@link TraxNumber#subtractFrom(double)}, etc.
 * <p/>
 * Do not directly instantiate {@link TraxNumber}s through their constructor (which
 * is package-private). Instead, call {@link Trax#trax(double)} and its variants.
 * These {@link Trax} factory methods will look up the appropriate {@link TraxContext}
 * for the current thread and decide whether to instantiate a plain {@link TraxNumber}
 * (minimal performance impact, but does not trace the calculation hierarchy that
 * goes into composing itself), or whether to allocate a {@link TraxTrackingNumber}
 * which requires more memory than a simple wrapper around a double, but permits
 * deep introspection into every sub-calculation that goes into calculating that
 * number.
 * <p/>
 * Note that {@link TraxNumber} and {@link TraxTrackingNumber} are {@link Immutable}.
 * Calling an arithmetic operation on a {@link TraxNumber} creates a new
 * {@link TraxNumber} rather than modifying the existing one. Since they are
 * {@link Immutable}, {@link TraxNumber}s are also
 * {@link javax.annotation.concurrent.ThreadSafe}.
 */
public class Trax {
    /**
     * The Java class containing the {@link Trax} code.
     * Used to determine the top of the call stack for calls made into {@link Trax}.
     */
    private static final int JAVA_SOURCE_FILE_SUFFIX_LENGTH = ".java".length();
    private static final String TRAX_SOURCE_NAME_PREFIX = "Trax";

    /**
     * Hide the constructor for the utilitiy class.
     */
    private Trax() {

    }

    /**
     * If {@link Trax} is configured to track the calculation, the returned
     * {@link TraxContext} will be inserted into the {@code trackingTable}
     * and maintained until the corresponding {@link TraxContext} is closed
     * via {@link TraxContext#close()}. The recommended way of initiating
     * tracking is by enclosing this method call in a try-with-resources
     * block.
     * <p/>
     * If {@link Trax} is NOT configured to track the calculation, the
     * returned {@link TraxContext} will NOT be inserted into the
     * {@code trackingTable}. Requests to get the context will return
     * a {@link TraxContext} configured with tracking turned off.
     * <p/>
     * To change the {@link Verbosity} for a topic, see
     * {@link TraxConfiguration#configureTopics(String, Verbosity)}. The {@link Verbosity}
     * of the tracking will be the maximum verbosity of all trackingTopics provided.
     * For example, if passing trackingTopics=["foo", "bar", "12345", "baz"] and the
     * topics are mapped to the following verbosities:
     * <p/>
     * "foo" -> OFF
     * "bar" -> TRACE
     * "12345" -> OFF
     * "baz" -> DEBUG
     * <p/>
     * then tracking will be performed at TRACE level because it is the most verbose.
     * <p/>
     * Tracking is performed on a per-thread basis. If a single calculation is composed
     * of sub-calculations made across multiple threads, each thread involved in
     * the calculation should be configured its own context via a call to track.
     * By default, the returned context is assigned to the current thread of the
     * caller. To override the owner thread of the context, see
     * {@Link #track(Thread, ContextOptions, String...)}. Parameters of tracking are fixed for
     * the thread at the time of creating the {@link TraxContext} and tracking
     * stops at the time {@link TraxContext#close()} is called.
     * <p/>
     * Nesting Trax tracking contexts is usually not recommended because of the confusion it can cause,
     * but it can be done. When it is done, the most verbose context in scope takes precedence for
     * all calculations done in that thread until it falls out of scope. For example, if doing:
     * <p/>
     * {@code try (track(Verbosity.DEBUG)) { try (track(Verbosity.OFF)) { [... some code...] } } }
     * <p/>
     * Even though the "... some code..." section is directly inside a Verbosity.OFF block, because
     * it is nested inside a Verbosity.DEBUG block, all calculations made in that code will be
     * tracked at DEBUG level. This is so that anyone trying to follow the outer calculation does
     * not lose parts of the outer calculation created inside the inner context.
     * <p/>
     * Configurations for topics with exhausted limits are not used and will be ignored.
     * For applicable topics with a {@link TraxUseLimit}, the limit will have its remaining
     * use count decremented.
     *
     * @param trackingTopics The topics associated with the tracking being initiated.
     *                       Examples of topics: entity OIDs, action IDs,
     *                       calculation category topics (ie. "cloud.savings"), etc.
     *
     * @return A {@link TraxContext} that can be used to track calculations
     *         made using {@link TraxNumber}. If configuration is not enabled,
     *         the returned {@link TraxContext} will not actually do
     *         any tracking.
     */
    @Nonnull
    public static TraxContext track(@Nonnull final String... trackingTopics) {
        return track(Thread.currentThread(), TraxTopicConfiguration.getDefaultInstance(),
            trackingTopics);
    }

    /**
     * If {@link Trax} is configured to track the calculation, the returned
     * {@link TraxContext} will be inserted into the {@code trackingTable}
     * and maintained until the corresponding {@link TraxContext} is closed
     * via {@link TraxContext#close()}. The recommended way of initiating
     * tracking is by enclosing this method call in a try-with-resources
     * block.
     * <p/>
     * If {@link Trax} is NOT configured to track the calculation, the
     * returned {@link TraxContext} will NOT be inserted into the
     * {@code trackingTable}. Requests to get the context will return
     * a {@link TraxContext} configured with tracking turned off.
     * <p/>
     * To change the {@link Verbosity} for a topic, see
     * {@link TraxConfiguration#configureTopics(String, Verbosity)}. The {@link Verbosity}
     * of the tracking will be the maximum verbosity of all trackingTopics provided.
     * For example, if passing trackingTopics=["foo", "bar", "12345", "baz"] and the
     * topics are mapped to the following verbosities:
     * <p/>
     * "foo" -> OFF
     * "bar" -> TRACE
     * "12345" -> OFF
     * "baz" -> DEBUG
     * <p/>
     * then tracking will be performed at TRACE level because it is the most verbose.
     * If the input {@link TopicSettings} is explicitly assigned a verbosity,
     * this will override the topic verbosity, even if it is less verbose than
     * the topic verbosity.
     * <p/>
     * Tracking is performed on a per-thread basis. If a single calculation is composed
     * of sub-calculations made across multiple threads, each thread involved in
     * the calculation should be configured its own context via a call to track.
     * By default, the returned context is assigned to the current thread of the
     * caller. To override the owner thread of the context, see
     * {@Link #track(Thread, ContextOptions, String...)}. Parameters of tracking are fixed for
     * the thread at the time of creating the {@link TraxContext} and tracking
     * stops at the time {@link TraxContext#close()} is called.
     * <p/>
     * Configurations for topics with exhausted limits are not used and will be ignored.
     * For applicable topics with a {@link TraxUseLimit}, the limit will have its remaining
     * use count decremented.
     *
     * @param settings Options to explicitly configure the behavior of the created
     *                {@link TraxContext}.
     * @param trackingTopics The topics associated with the tracking being initiated.
     *                       Examples of topics: entity OIDs, action IDs,
     *                       calculation category topics (ie. "cloud.savings"), etc.
     *
     * @return A {@link TraxContext} that can be used to track calculations
     *         made using {@link TraxNumber}. If configuration is not enabled,
     *         the returned {@link TraxContext} will not actually do
     *         any tracking.
     */
    @Nonnull
    public static TraxContext track(@Nonnull final TraxTopicConfiguration settings,
                                    @Nonnull final String... trackingTopics) {
        return track(Thread.currentThread(), settings, trackingTopics);
    }

    /**
     * If {@link Trax} is configured to track the calculation, the returned
     * {@link TraxContext} will be inserted into the {@code trackingTable}
     * and maintained until the corresponding {@link TraxContext} is closed
     * via {@link TraxContext#close()}. The recommended way of initiating
     * tracking is by enclosing this method call in a try-with-resources
     * block.
     * <p/>
     * If {@link Trax} is NOT configured to track the calculation, the
     * returned {@link TraxContext} will NOT be inserted into the
     * {@code trackingTable}. Requests to get the context will return
     * a {@link TraxContext} configured with tracking turned off.
     * <p/>
     * To change the {@link Verbosity} for a topic, see
     * {@link TraxConfiguration#configureTopics(String, Verbosity)}. The {@link Verbosity}
     * of the tracking will be the maximum verbosity of all trackingTopics provided.
     * For example, if passing trackingTopics=["foo", "bar", "12345", "baz"] and the
     * topics are mapped to the following verbosities:
     * <p/>
     * "foo" -> OFF
     * "bar" -> TRACE
     * "12345" -> OFF
     * "baz" -> DEBUG
     * <p/>
     * then tracking will be performed at TRACE level because it is the most verbose.
     * If the input {@link TopicSettings} is explicitly assigned a verbosity,
     * this will override the topic verbosity, even if it is less verbose than
     * the topic verbosity.
     * <p/>
     * Tracking is performed on a per-thread basis. If a single calculation is composed
     * of sub-calculations made across multiple threads, each thread involved in
     * the calculation should be configured its own context via a call to track.
     * The owner thread is assigned to be the thread of the passed in parameter.
     * Parameters of tracking are fixed for the thread at the time of creating the
     * {@link TraxContext} and tracking stops at the time {@link TraxContext#close()}
     * is called.
     * <p/>
     * Configurations for topics with exhausted limits are not used and will be ignored.
     * For applicable topics with a {@link TraxUseLimit}, the limit will have its remaining
     * use count decremented.
     *
     * @param ownerThread The thread for the returned {@link TraxContext}. Use to manually
     *                    configure the thread for the tracking context if you want to
     *                    set up calculation tracking for a thread other than the current thread.
     * @param explicitSettings Options to explicitly configure the behavior of the created
     *                         {@link TraxContext}. If a field is not set, the value from
     *                         the most verbose associated topic will be used.
     * @param trackingTopics The topics associated with the tracking being initiated.
     *                       Examples of topics: entity OIDs, action IDs,
     *                       calculation category topics (ie. "cloud.savings"), etc.
     *
     * @return A {@link TraxContext} that can be used to track calculations
     *         made using {@link TraxNumber}. If configuration is not enabled,
     *         the returned {@link TraxContext} will not actually do
     *         any tracking.
     */
    @Nonnull
    public static TraxContext track(@Nonnull final Thread ownerThread,
                                    @Nonnull final TraxTopicConfiguration explicitSettings,
                                    @Nonnull final String... trackingTopics) {
        return TraxConfiguration.track(ownerThread, explicitSettings, trackingTopics);
    }

    /**
     * Create a {@link TraxNumber} for use as a static final constant.
     * This number can be used as a number with tracking enabled and its name will be retained
     * even though it is created outside of an explicit {@link TraxContext} enabling debug mode.
     * <p/>
     * Do not call outside of use for creating static final constant {@link TraxNumber}s.
     *
     * @param value The value for the number.
     * @param name The name for the {@link TraxNumber}.
     * @return A{@link TraxNumber} for use as a static final constant. The number can be used
     *         inside tracking and its name will be displayed.
     */
    public static TraxNumber traxConstant(final double value,
                                          @Nonnull final String name) {

        return new TraxConstantNumber(value, name, TraxConfiguration.CONTEXT_FOR_CONSTANTS);
    }

    /**
     * Create a new {@link TraxNumber} of the given value. No name or details will be provided.
     * Calculations associated with the number will only be tracked if calculation tracking
     * is on for the given thread creating the {@link TraxNumber} (ie {@link Thread#currentThread()}).
     * <p/>
     * Details will be set to the {@link TraxNumber} will be set to its call stack if the
     * verbosity for the current thread's {@link TraxContext} is set to {@link Verbosity#TRACE}.
     * <p/>
     * See comments for {@link Trax#track(String...)} for information on how to turn on calculation
     * tracking.
     *
     * @param value The value for the number.
     * @return A {@link TraxNumber} for the input value.
     */
    public static TraxNumber trax(final double value) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, null, context)
            : new TraxNumber(value);
    }

    /**
     * Create a new {@link TraxNumber} of the given value and with the given name.
     * Calculations associated with the number will only be tracked if calculation tracking
     * is on for the given thread creating the {@link TraxNumber} (ie {@link Thread#currentThread()}).
     * <p/>
     * Details will be set to the {@link TraxNumber} will be set to its call stack if the
     * verbosity for the current thread's {@link TraxContext} is set to {@link Verbosity#TRACE}.
     * <p/>
     * See comments for {@link Trax#track(String...)} for information on how to turn on calculation
     * tracking.
     *
     * @param value The value for the number.
     * @param name The name for the {@link TraxNumber}.
     * @return A {@link TraxNumber} for the input value.
     */
    public static TraxNumber trax(final double value,
                                  @Nonnull final String name) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, name, null, context)
            : new TraxNumber(value);
    }

    /**
     * Create a new {@link TraxNumber} of the given value and with the given name and details.
     * Calculations associated with the number will only be tracked if calculation tracking
     * is on for the given thread creating the {@link TraxNumber} (ie {@link Thread#currentThread()}).
     * <p/>
     * See comments for {@link Trax#track(String...)} for information on how to turn on calculation
     * tracking.
     *
     * @param value The value for the number.
     * @param name The name for the {@link TraxNumber}.
     * @param description Description details for the {@link TraxNumber}.
     * @return A {@link TraxNumber} for the input value.
     */
    public static TraxNumber trax(final double value,
                                  @Nonnull final String name,
                                  @Nonnull final String description) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, name, description, null, context)
            : new TraxNumber(value);
    }

    /**
     * Package-private factory method for constructing {@link TraxNumber} with an equivalent.
     * When printing the calculation stack for a {@link TraxNumber} the "equivalent" is
     * printed on the right side of an equals sign. The equivalent is usually a
     * {@link com.vmturbo.trax.Trax.TraxCalculation} that contains details explaining how the
     * number was computed.
     * <p/>
     * Like the public versions of the {@code #trax} factory methods, for performance reasons
     * these factory methods only retain deteails and do work if the {@link TraxContext}
     * has tracking enabled for the {@link Thread} running the calculation.
     *
     * @param value The value for the {@link TraxNumber}.
     * @param equivalent An equivalent to the number.
     * @return A {@link TraxNumber} with the number and an equivalent.
     */
    static TraxNumber trax(final double value,
                           @Nullable final TraxTrackable equivalent) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, equivalent, context)
            : new TraxNumber(value);
    }

    /**
     * Package-private factory method for constructing {@link TraxNumber} with an equivalent.
     * When printing the calculation stack for a {@link TraxNumber} the "equivalent" is
     * printed on the right side of an equals sign. The equivalent is usually a
     * {@link com.vmturbo.trax.Trax.TraxCalculation} that contains details explaining how the
     * number was computed.
     * <p/>
     * Like the public versions of the {@code #trax} factory methods, for performance reasons
     * these factory methods only retain deteails and do work if the {@link TraxContext}
     * has tracking enabled for the {@link Thread} running the calculation.
     *
     * @param value The value for the {@link TraxNumber}.
     * @param name The name for the {@link TraxNumber}.
     * @param equivalent An equivalent to the number.
     * @return A {@link TraxNumber} with the number and an equivalent.
     */
    static TraxNumber trax(final double value,
                           @Nonnull final String name,
                           @Nullable final TraxTrackable equivalent) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, name, equivalent, context)
            : new TraxNumber(value);
    }

    /**
     * Package-private factory method for constructing {@link TraxNumber} with an equivalent.
     * When printing the calculation stack for a {@link TraxNumber} the "equivalent" is
     * printed on the right side of an equals sign. The equivalent is usually a
     * {@link com.vmturbo.trax.Trax.TraxCalculation} that contains details explaining how the
     * number was computed.
     * <p/>
     * Like the public versions of the {@code #trax} factory methods, for performance reasons
     * these factory methods only retain deteails and do work if the {@link TraxContext}
     * has tracking enabled for the {@link Thread} running the calculation.
     *
     * @param value The value for the {@link TraxNumber}.
     * @param name The name for the {@link TraxNumber}.
     * @param details Description details for the {@link TraxNumber}.
     * @param equivalent An equivalent to the number.
     * @return A {@link TraxNumber} with the number and an equivalent.
     */
    static TraxNumber trax(final double value,
                           @Nonnull final String name,
                           @Nonnull final String details,
                           @Nullable final TraxTrackable equivalent) {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());
        return context.on()
            ? new TraxTrackingNumber(value, name, details, equivalent, context)
            : new TraxNumber(value);
    }

    /**
     * Compute the minimum between two {@link TraxNumber}s.
     *
     * @param a The first argument.
     * @param b The other argument.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    public static TraxIntermediateCalculation min(@Nonnull final TraxNumber a,
                                                  @Nonnull final TraxNumber b) {
        // Two element case specially implemented as an optimization to avoid having
        // to reallocate a new array
        return minCalc(Math.min(a.getValue(), b.getValue()), true, a, b);
    }

    /**
     * Compute the minimum between two or more {@link TraxNumber}s.
     *
     * @param firstNumber The first numbers.
     * @param otherNumbers Additional numbers to examine for the minimum.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    public static TraxIntermediateCalculation min(@Nonnull final TraxNumber firstNumber,
                                                  @Nonnull final TraxNumber... otherNumbers) {
        // Require the first element because it makes no sense to take the min of zero elements
        double result = firstNumber.getValue();
        for (TraxNumber num : otherNumbers) {
            result = Math.min(result, num.getValue());
        }

        return minCalc(result, true, prepend(firstNumber, otherNumbers));
    }

    /**
     * Compute the minimum of a {@link Collection} of {@link TraxNumber}s or return a default
     * value if the collection is empty.
     *
     * @param numbers The collection of numbers to compute the minimum of.
     * @param defaultMinOnEmpty The default value to return as the minimum on an empty collection.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     *
     *         If the {@link Collection} of numbers is empty, the value of the returned
     *         {@link TraxIntermediateCalculation} will be the value of the defaultMinOnEmpty
     *         parameter.
     */
    public static TraxIntermediateCalculation minOrDefault(@Nonnull final Collection<TraxNumber> numbers,
                                                           @Nonnull final TraxNumber defaultMinOnEmpty) {
        if (numbers.isEmpty()) {
            return minCalc(defaultMinOnEmpty.getValue(), false, defaultMinOnEmpty);
        } else {
            final Iterator<TraxNumber> iter = numbers.iterator();
            double result = iter.next().getValue();
            while (iter.hasNext()) {
                result = Math.min(result, iter.next().getValue());
            }
            return minCalc(result, true, numbers.toArray(new TraxNumber[numbers.size()]));
        }
    }

    /**
     * Compute the maximum between two {@link TraxNumber}s.
     *
     * @param a The first argument.
     * @param b The other argument.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    public static TraxIntermediateCalculation max(@Nonnull final TraxNumber a,
                                                  @Nonnull final TraxNumber b) {
        // Two element case specially implemented as an optimization to avoid having
        // to reallocate a new array
        return maxCalc(Math.max(a.getValue(), b.getValue()), true, a, b);
    }

    /**
     * Compute the maximum between two or more {@link TraxNumber}s.
     *
     * @param firstNumber The first numbers.
     * @param otherNumbers Additional numbers to examine for the maximum.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    public static TraxIntermediateCalculation max(@Nonnull final TraxNumber firstNumber,
                                                  @Nonnull final TraxNumber... otherNumbers) {
        // Require the first element because it makes no sense to take the max of zero elements
        double result = firstNumber.getValue();
        for (TraxNumber num : otherNumbers) {
            result = Math.max(result, num.getValue());
        }

        return maxCalc(result, true, prepend(firstNumber, otherNumbers));
    }

    /**
     * Compute the maximum of a {@link Collection} of {@link TraxNumber}s or return a default
     * value if the collection is empty.
     *
     * @param numbers The collection of numbers to compute the maximum of.
     * @param defaultMaxOnEmpty The default value to return as the maximum on an empty collection.
     * @return A {@link TraxIntermediateCalculation}. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     *
     *         If the {@link Collection} of numbers is empty, the value of the returned
     *         {@link TraxIntermediateCalculation} will be the value of the defaultMaxOnEmpty
     *         parameter.
     */
    public static TraxIntermediateCalculation maxOrDefault(@Nonnull final Collection<TraxNumber> numbers,
                                                           @Nonnull final TraxNumber defaultMaxOnEmpty) {
        if (numbers.isEmpty()) {
            return maxCalc(defaultMaxOnEmpty.getValue(), false, defaultMaxOnEmpty);
        } else {
            final Iterator<TraxNumber> iter = numbers.iterator();
            double result = iter.next().getValue();
            while (iter.hasNext()) {
                result = Math.max(result, iter.next().getValue());
            }
            return maxCalc(result, true, numbers.toArray(new TraxNumber[numbers.size()]));
        }
    }

    /**
     * Private helper method for constructing a {@link TraxCalculation} for a minimum
     * computation with tracking set appropriately for the current {@link TraxContext}.
     *
     * @param result The computed minimum for the calculation.
     * @param showInteriorElements Whether or not to show the interior elements of the minimum calculation.
     *                             This argument should usually be true except when the minimum is a default
     *                             value rather than taking the actual minimum of a non-empty list of values.
     * @param numbers The interior numbers whose minimum is the result.
     * @return A {@link TraxIntermediateCalculation} for the minimum. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    private static TraxIntermediateCalculation minCalc(final double result,
                                                       final boolean showInteriorElements,
                                                       @Nonnull final TraxNumber... numbers) {
        if (TraxConfiguration.on(Thread.currentThread())) {
            final TraxCalculation minCalc = new TraxMinCalculation(showInteriorElements, numbers);
            return new TraxIntermediateTrackingCalculation(result, minCalc);
        } else {
            return new TraxIntermediateCalculation(result);
        }
    }

    /**
     * Private helper method for constructing a {@link TraxCalculation} for a maximum
     * computation with tracking set appropriately for the current {@link TraxContext}.
     *
     * @param result The computed maximum for the calculation.
     * @param showInteriorElements Whether or not to show the interior elements of the maximum calculation.
     *                             This argument should usually be true except when the maximum is a default
     *                             value rather than taking the actual maximum of a non-empty list of values.
     * @param numbers The interior numbers whose maximum is the result.
     * @return A {@link TraxIntermediateCalculation} for the maximum. Call
     *         {@link TraxIntermediateCalculation#compute(String)} to compute the resulting number
     *         and to name it.
     */
    private static TraxIntermediateCalculation maxCalc(final double result,
                                                       final boolean showInteriorElements,
                                                       @Nonnull final TraxNumber... numbers) {
        if (TraxConfiguration.on(Thread.currentThread())) {
            final TraxCalculation maxCalc = new TraxMaxCalculation(showInteriorElements, numbers);
            return new TraxIntermediateTrackingCalculation(result, maxCalc);
        } else {
            return new TraxIntermediateCalculation(result);
        }
    }

    /**
     * Helper method for prepending a single element onto the front of an array of elements.
     *
     * @param initialElement The element to be prepended onto the front of the array.
     * @param others The other elements to follow the initialElement in the returned array.
     *               The array of others may be empty, but it may not be null.
     * @return A new array containing the initialElement followed by the other elements in order.
     *         The size of the returned array will be others.length+1.
     */
    private static TraxNumber[] prepend(@Nonnull final TraxNumber initialElement,
                                        @Nonnull final TraxNumber... others) {
        final TraxNumber[] result = new TraxNumber[others.length + 1];
        result[0] = initialElement;
        int i = 1;
        for (TraxNumber element : others) {
            result[i] = element;
            i++;
        }

        return result;
    }

    /**
     * A helper method containing information about the verbosity and max decimal places for
     * a specific topic in {@link Trax}.
     */
    @Immutable
    public static class TopicLevel {
        /**
         * The topic.
         */
        public final String topic;

        /**
         * The verbosity level.
         */
        public final Verbosity level;

        /**
         * The max decimal places.
         */
        public final int maxDecimalPlaces;

        /**
         * Construct a new {@link com.vmturbo.trax.Trax.TopicLevel}.
         *
         * @param topic The topic described by this {@link com.vmturbo.trax.Trax.TopicLevel}.
         * @param level The tracking verbosity for this {@link com.vmturbo.trax.Trax.TopicLevel}.
         * @param maxDecimalPlaces The max decimal places to log in a calculation stack for this
         *                         {@link com.vmturbo.trax.Trax.TopicLevel}.
         */
        public TopicLevel(@Nonnull final String topic,
                          @Nonnull final Verbosity level,
                          final int maxDecimalPlaces) {
            this.topic = Objects.requireNonNull(topic);
            this.level = Objects.requireNonNull(level);
            this.maxDecimalPlaces = maxDecimalPlaces;
        }
    }

    /**
     * Get the file name and the line number of the first stack trace element of
     * the top of the stack not in the {@link TraxNumber} class itself.
     *
     * @return The file and line number of the caller in the call stack.
     */
    @Nullable
    public static String callerFromStackTrace() {
        return StackTrace.getFilteredCaller(element -> {
            return (element.getFileName() != null
                    && !element.getFileName().startsWith(TRAX_SOURCE_NAME_PREFIX))
                || element.getClassName().endsWith("Test");
        });
    }

    /**
     * An interface for {@link Trax} objects that can be tracked. They nest in a way that
     * top level numbers have references to the sub-calculations used to compute that number.
     * <p/>
     * All {@link TraxTrackable} objects should be {@code javax.annotation.concurrent.Immutable}.
     */
    public interface TraxTrackable {
        /**
         * One of the method to be used in rendering the calculation stack for a
         * {@link TraxTrackable} element.
         * <p/>
         * Append strings for the top-line element for this particular trackable element.
         * For a {@link TraxNumber} for example, this will be rendered similar to:
         * value[name] = equivalent#appendSelfString
         *
         * @param builder The builder on which to append the top-line of the calculation stack
         *                for this number.
         * @param context The {@link TraxContext} associated with the calculation.
         */
        void appendSelfString(@Nonnull StringBuilder builder,
                              @Nonnull TraxContext context);

        /**
         * One of the methods to be used in rendering the calculation stack for
         * a {@link TraxTrackable} element.
         * <p/>
         * Append strings for the sub-elements for this particular trackable element.
         * For a {@link TraxCalculation} for example, involve rendering all the sub-elements
         * of the calculation underneath the top-line selfString.
         * <p/>
         * See https://stackoverflow.com/a/8948691 for more details on the rendering
         * algorithm.
         *
         * @param builder The builder on which to append the top-line of the calculation stack
         *                for this number.
         * @param prefix A prefix for the current element.
         * @param childrenPrefix A prefix for the children of the current element.
         * @param context The {@link TraxContext} associated with the calculation.
         */
        void appendChildrenStrings(@Nonnull StringBuilder builder,
                                   @Nonnull String prefix,
                                   @Nonnull String childrenPrefix,
                                   @Nonnull TraxContext context);

        /**
         * Get the next prefix for use when appending children strings.
         * <p/>
         * See https://stackoverflow.com/a/8948691 for more details on the rendering
         * algorithm.
         *
         * @param currentPrefix The current prefix.
         * @param hasMoreChildren Whether there are additional children to render
         *                        following the current one.
         * @return A prefix (see comments for
         *         {@link #appendChildrenStrings(StringBuilder, String, String, TraxContext)})
         *         to use when appending childrenStrings.
         */
        static String nextPrefix(@Nonnull final String currentPrefix,
                                 final boolean hasMoreChildren) {
            return hasMoreChildren
                ? currentPrefix + "|__ "
                : currentPrefix + "\\__ ";
        }

        /**
         * Get the next prefix for use when appending children strings.
         * <p/>
         * See https://stackoverflow.com/a/8948691 for more details on the rendering
         * algorithm.
         *
         * @param currentPrefix The current prefix.
         * @param hasMoreChildren Whether there are additional children to render
         *                        following the current one.
         * @return A prefix (see comments for
         *         {@link #appendChildrenStrings(StringBuilder, String, String, TraxContext)})
         *         to use when appending childrenStrings.
         */
        static String nextChildrenPrefix(@Nonnull final String currentPrefix,
                                         final boolean hasMoreChildren) {
            return hasMoreChildren
                ? currentPrefix + "|   "
                : currentPrefix + "    ";
        }
    }

    /**
     * A {@link TraxCalculation} is used to trace the elements of a calculation that
     * was performed to compute some resulting {@link TraxNumber}. It is usually set
     * to be the equivalent of the top-level result {@link TraxNumber}.
     * <p/>
     * A {@link TraxCalculation} is usually created by first creating a
     * {@link TraxIntermediateCalculation} whose {@link TraxIntermediateCalculation#compute}
     * method will create a {@link TraxNumber} with the appropriate equivalent
     * {@link TraxCalculation}.
     */
    @Immutable
    public static class TraxCalculation implements TraxTrackable {

        private final TraxTrackable[] elements;

        /**
         * Create a new {@link TraxCalculation} containing the given elements.
         *
         * @param elements The sub-elements of the calculation.
         */
        public TraxCalculation(@Nonnull final TraxTrackable... elements) {
            this.elements = Objects.requireNonNull(elements);
        }

        /**
         * Create a new {@link TraxCalculation} containing the given elements.
         *
         * @param elements The sub-elements of the calculation.
         */
        public TraxCalculation(@Nonnull final Collection<TraxTrackable> elements) {
            final TraxTrackable[] array = new TraxTrackable[elements.size()];
            this.elements = elements.toArray(array);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void appendSelfString(@Nonnull StringBuilder builder,
                                     @Nonnull TraxContext context) {
            int i = 0;
            for (TraxTrackable element : elements) {
                if (i > 0) {
                    builder.append(" ");
                }
                element.appendSelfString(builder, context);

                i++;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void appendChildrenStrings(@Nonnull final StringBuilder builder,
                                          @Nonnull final String prefix,
                                          @Nonnull final String childrenPrefix,
                                          @Nonnull final TraxContext context) {
            // https://stackoverflow.com/a/8948691
            for (Iterator<TraxTrackable> it = Stream.of(elements).iterator(); it.hasNext();) {
                final TraxTrackable element = it.next();
                element.appendChildrenStrings(builder,
                    TraxTrackable.nextPrefix(childrenPrefix, it.hasNext()),
                    TraxTrackable.nextChildrenPrefix(childrenPrefix, it.hasNext()),
                    context);
            }
        }

        /**
         * Get the elements of the calculation.
         * <p/>
         * Package-private. Not strictly enforced, but do NOT modify the array elements
         * because this class is expected to be immutable.
         *
         * @return The sub-elements composing the calculation.
         */
        final TraxTrackable[] getElements() {
            return elements;
        }
    }

    /**
     * A {@link TraxIntermediateCalculation} is used as an intermediate step when building
     * a resulting {@link TraxNumber} and its {@link TraxCalculation} equivalent.
     * <p/>
     * The {@link TraxIntermediateCalculation#compute(String)} method and its variants
     * can be used for this purpose.
     * <p/>
     * {@link TraxIntermediateCalculation}s should not be directly constructed, instead use
     * methods such as {@link TraxNumber#plus(double)}, {@link TraxNumber#minus(double)} and
     * the like.
     * <p/>
     * A {@link TraxIntermediateCalculation} does not actually track calculation details
     * for performance reasons. Its subclass {@link TraxIntermediateTrackingCalculation}
     * should be used when calculation tracking is enabled for the current {@link TraxContext}
     * to track calculation details.
     */
    @Immutable
    public static class TraxIntermediateCalculation {
        final double value;

        /**
         * Create a new {@link TraxIntermediateCalculation}.
         *
         * @param value The result value of the calculation.
         */
        TraxIntermediateCalculation(final double value) {
            this.value = value;
        }

        /**
         * Get the result value for the calculation.
         *
         * @return the result value for the calculation.
         */
        public double getValue() {
            return value;
        }

        /**
         * Compute a {@link TraxNumber} for the calculation's result value.
         *
         * @return A {@link TraxNumber} for the calculation's result value.
         */
        @Nonnull
        public TraxNumber compute() {
            return new TraxNumber(value);
        }

        /**
         * Compute a {@link TraxNumber} for the calculation's result value.
         *
         * @param name The name of the resulting calculation.
         * @return A {@link TraxNumber} for the calculation's result value.
         */
        @Nonnull
        public TraxNumber compute(@Nonnull final String name) {
            return new TraxNumber(value);
        }

        /**
         * Compute a {@link TraxNumber} for the calculation's result value.
         *
         * @param name The name of the resulting calculation.
         * @param details The details of the resulting calculation.
         * @return A {@link TraxNumber} for the calculation's result value.
         */
        @Nonnull
        public TraxNumber compute(@Nonnull final String name,
                                  @Nonnull final String details) {
            return new TraxNumber(value);
        }
    }

    /**
     * A {@link TraxIntermediateTrackingCalculation} is a subclass of
     * {@link TraxIntermediateCalculation} that preserves calculation tracking details so
     * that they can be recorded in a {@link TraxNumber}s calculation stack.
     */
    @Immutable
    public static class TraxIntermediateTrackingCalculation extends TraxIntermediateCalculation {
        private final TraxCalculation calculation;

        /**
         * Create a new {@link TraxIntermediateTrackingCalculation} for the given
         * value and containing the given {@link TraxCalculation} equivalent.
         *
         * @param value The value for the calculation result.
         * @param calculation The {@link TraxCalculation} equivalent for the value.
         */
        TraxIntermediateTrackingCalculation(final double value,
                                            @Nonnull final TraxCalculation calculation) {
            super(value);
            this.calculation = Objects.requireNonNull(calculation);
        }

        /**
         * Get the interior calculation equivalent to the result value.
         *
         * @return the interior calculation equivalent to the result value.
         */
        @Nonnull
        public TraxCalculation getCalculation() {
            return calculation;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public TraxNumber compute() {
            return trax(value, calculation);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public TraxNumber compute(@Nonnull final String name) {
            return trax(value, name, calculation);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public TraxNumber compute(@Nonnull final String name,
                                  @Nonnull final String details) {
            return trax(value, name, details, calculation);
        }
    }

    /**
     * A {@link TraxInfixOperation} is a calculation element that goes in between
     * two {@link TraxNumber}s. For example, in the expression:
     * <p/>
     * 1 + 4
     *   ^
     *   |
     *   *---- The '+' symbol is represented by the 'PLUS' infix operation.
     */
    @Immutable
    public enum TraxInfixOperation implements TraxTrackable {
        /**
         * Addition.
         */
        PLUS("+"),

        /**
         * Subtraction.
         */
        MINUS("-"),

        /**
         * Multiplication.
         */
        TIMES("x"),

        /**
         * Division.
         */
        DIVIDED_BY("/");

        private final String operationText;

        /**
         * Create a new {@link TraxInfixOperation}.
         *
         * @param operationText The text for the infix operation. Usually a mathematical symbol.
         */
        TraxInfixOperation(@Nonnull final String operationText) {
            this.operationText = Objects.requireNonNull(operationText);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void appendSelfString(@Nonnull final StringBuilder builder,
                                     @Nonnull final TraxContext context) {
            builder.append(operationText);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void appendChildrenStrings(@Nonnull final StringBuilder builder,
                                          @Nonnull final String prefix,
                                          @Nonnull final String childrenPrefix,
                                          @Nonnull final TraxContext context) {
            // Operations have no children. Nothing to do
        }
    }

    /**
     * A special class of calculation like min or max that surrounds
     * a bunch of interior elements. For example:
     * <p/>
     * min(1, 2, 3) or
     * max(1, 2, 3)
     */
    @Immutable
    private static class TraxSurroundingCalculation extends TraxCalculation {
        final String operatorName;
        final boolean showInteriorElements;

        /**
         * Create a new {@link TraxSurroundingCalculation}.
         *
         * @param operatorName The name of the surrounding operation (ie "min", "max", etc.)
         * @param showInteriorElements Whether or not to show interior elements. If true, we will
         *                             render, for example min(1,2,3). If false we will render
         *                             the same data as min(). Even if false, interior elements will
         *                             still be rendered as children, they just won't be in the interior
         *                             of the expression. This is useful when rendering, for example,
         *                             the min of an empty list which we set to be a default value.
         * @param elements The interior elements of the surrounding calculation.
         */
        TraxSurroundingCalculation(@Nonnull final String operatorName,
                                   final boolean showInteriorElements,
                                   @Nonnull final TraxTrackable... elements) {
            super(elements);
            this.showInteriorElements = showInteriorElements;
            this.operatorName = Objects.requireNonNull(operatorName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void appendSelfString(@Nonnull StringBuilder builder,
                                     @Nonnull TraxContext context) {
            builder.append(operatorName).append("(");

            if (showInteriorElements) {
                int i = 0;
                for (TraxTrackable element : getElements()) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    element.appendSelfString(builder, context);

                    i++;
                }
            }

            builder.append(")");
        }
    }

    /**
     * A {@link TraxSurroundingCalculation} to capture the calculation of a minimum
     * of a collection of numbers.
     */
    @Immutable
    private static class TraxMinCalculation extends TraxSurroundingCalculation {
        public static final String OPERATOR_NAME = "min";

        /**
         * Create a new {@link TraxMinCalculation}.
         *
         * @param showInteriorElements Whether to show interior elements.
         * @param elements The interior elements.
         */
        TraxMinCalculation(final boolean showInteriorElements,
                           @Nonnull final TraxTrackable... elements) {
            super(OPERATOR_NAME, showInteriorElements, elements);
        }
    }

    /**
     * A {@link TraxSurroundingCalculation} to capture the calculation of a maximum
     * of a collection of numbers.
     */
    @Immutable
    private static class TraxMaxCalculation extends TraxSurroundingCalculation {
        public static final String OPERATOR_NAME = "max";

        /**
         * Create a new {@link TraxMaxCalculation}.
         *
         * @param showInteriorElements Whether to show interior elements.
         * @param elements The interior elements.
         */
        TraxMaxCalculation(final boolean showInteriorElements,
                           @Nonnull final TraxTrackable... elements) {
            super(OPERATOR_NAME, showInteriorElements, elements);
        }
    }
}
