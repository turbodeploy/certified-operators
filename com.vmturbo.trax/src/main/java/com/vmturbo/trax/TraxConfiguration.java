package com.vmturbo.trax;

import java.text.DecimalFormat;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingTimeLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingUseLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.Builder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingNoLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingTimeLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingUseLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationRequest;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.Trax.TopicLevel;
import com.vmturbo.trax.TraxConfiguration.TraxContext.EmptyTraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxContext.TrackingTraxContext;

/**
 * TRAX: Calculation Tracking library.
 * Use a TraxNumber to trace the elements of a calculation as the calculation is composed.
 * <p/>
 * Contains configurations for calculation tracking. Configuration is performed on a per-topic
 * basis where topics are strings. Some example topics might be a target type (ie "Azure") or
 * the Object ID (OID) of a VM whose costs and savings values are being computed.
 * <p/>
 * See the Trax protobuf documentation on available configuration options.
 * <p/>
 * See the documentation for {@link Trax#track(String...)} for additional details on
 * configuration scoping and precedence.
 */
@ThreadSafe
public class TraxConfiguration {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A statically-maintained list of all calculation trails currently being tracked.
     * <p/>
     * Use a copy-on-write ArrayList because lookups and traversal operations are extremely
     * common but mutations are much, much less uncommon. Also, the list will almost always
     * be small so copying the whole list on mutation should be fairly cheap.
     * <p/>
     * Note that if this table ever gets big (ie over the size warning) we will probably
     * need to reconsider this data structure.
     * <p/>
     * Note that a {@link CopyOnWriteArrayList} is thread-safe.
     */
    private static final List<TraxContext> trackingTable = new CopyOnWriteArrayList<>();

    /**
     * A statically-maintained list of all calculation topics to track.
     * If a topic is not in this list, we consider tracking for it to be OFF.
     */
    private static final Map<String, TopicSettings> topicsToTrack = new HashMap<>();

    /**
     * If the tracking table size ever grows over this limit, issue a warning message.
     */
    private static final int TRACKING_TABLE_SIZE_WARNING_LIMIT = 128;

    /**
     * A {@link TraxContext} for use specifically when creating static final constant Trax numbers.
     * Because these numbers are created during static initialization when the class containing
     * the constants is loaded and no TraxContext can actually be configured or looked up,
     * we have all constants created in DEBUG mode so that they can actually be used inside
     * tracking code and contain meaningful information.
     */
    static final TraxContext CONTEXT_FOR_CONSTANTS = new TrackingTraxContext(
        TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .build());

    /**
     * The default topic. All {@link Trax#track(String...)} calls implicitly fall under
     * the default topic in addition to their explicit topics. This allows enabling
     * tracking of all {@link TraxNumber}s globally no matter their explicit topic.
     */
    public static final String DEFAULT_TOPIC_NAME = "DEFAULT";

    /**
     * Hide constructor for utility class.
     */
    private TraxConfiguration() {

    }

    /**
     * See documentation for {@link Trax#track(Thread, TraxTopicConfiguration, String...)}.
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
    static TraxContext track(@Nonnull final Thread ownerThread,
                             @Nonnull final TraxTopicConfiguration explicitSettings,
                             @Nonnull final String... trackingTopics) {
        Objects.requireNonNull(ownerThread);

        final TopicSorter topicSorter = new TopicSorter();
        synchronized (topicsToTrack) {
            // Scan for explicit topics.
            for (final String topic : trackingTopics) {
                final TopicSettings options = topicsToTrack.get(topic);
                if (options != null) {
                    topicSorter.scanTopic(topic, options);
                }
            }

            // Scan the default topic that everything is associated with.
            final TopicSettings defaults = topicsToTrack.get(DEFAULT_TOPIC_NAME);
            if (defaults != null) {
                topicSorter.scanTopic(DEFAULT_TOPIC_NAME, defaults);
            }
        }

        // Remove all exhausted topics
        synchronized (topicsToTrack) {
            topicSorter.exhaustedTopicNames.forEach(topic -> {
                final TopicSettings options = topicsToTrack.remove(topic);
                if (options != null) {
                    logger.error("Removing exhausted topic {} (limit={})", topic,
                        options.getLimit().toProto().toString().replace("\n", ""));
                }
            });
        }

        if (explicitSettings.hasVerbosity()) {
            // Verbosity was configured explicitly. Add a topic noting this explicit configuration.
            topicSorter.addExplicitVerbosity(explicitSettings.getVerbosity());
        }
        if (explicitSettings.hasMaxDecimalPlaces()) {
            // Max decimal places was configured explicitly. Add a topic noting this explicit configuration.
            topicSorter.addExplicitMaxDecimalPlaces(explicitSettings.getMaxDecimalPlaces());
        }

        // If no max decimals were found, use the default.
        if (topicSorter.maxDecimalPlaces < 0) {
            topicSorter.maxDecimalPlaces = TraxTopicConfiguration.getDefaultInstance().getMaxDecimalPlaces();
        }

        return createContext(ownerThread, topicSorter.topics, TraxTopicConfiguration.newBuilder()
            .setVerbosity(topicSorter.level)
            .setMaxDecimalPlaces(topicSorter.maxDecimalPlaces)
            .build());
    }

    /**
     * Configure a topic for calculation tracking. Use this method to configure
     * a topic without a limit to the number of uses or amount of time we track the topic.
     *
     * @param topicName The name of the topic whose configurations should be set.
     *                  Must not be null or empty.
     * @param verbosity The verbosity level of calculation tracking.
     * @return A {@link TopicSettings} object containing the configurations.
     */
    public static TopicSettings configureTopics(@Nonnull final String topicName,
                                                @Nonnull final Verbosity verbosity) {
        Objects.requireNonNull(topicName);
        Preconditions.checkArgument(!topicName.isEmpty());

        final TopicSettings settings = new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(verbosity)
            .build(), topicName);

        return configureTopics(settings);
    }

    /**
     * Configure one or more topics for calculation tracking. Use this method to configure topics
     * with a limit to the number of uses or amount of time we track a topic.
     *
     * @param settings The settings to use for the topic.
     * @return A {@link TopicSettings} object containing the configurations.
     */
    public static TopicSettings configureTopics(@Nonnull final TopicSettings settings) {
        logger.debug("Configuring the following Trax topics: {} with settings {} and limit {}",
            settings.getTopicNames(), settings.getTopicConfiguration(), settings.getLimit());
        synchronized (topicsToTrack) {
            settings.getTopicNames().forEach(name -> {
                topicsToTrack.put(name, settings);
                if (DEFAULT_TOPIC_NAME.equals(name)) {

                }
            });
        }

        return settings;
    }

    /**
     * Configure a topic according to a {@link TraxConfigurationRequest}.
     *
     * @param config A protobuf configuration request containing the details for the topic's
     *               configuratino.
     * @return A {@link TopicSettings} object containing the configurations.
     */
    public static TopicSettings configureTopics(@Nonnull TraxConfigurationRequest config) {
        final TopicSettings settings = new TopicSettings(config.getTopicConfiguration(),
            TraxLimit.fromProto(config.getTrackingLimit()), config.getTopicNamesList());

        return configureTopics(settings);
    }

    /**
     * Clear configuration for a specific topic.
     *
     * @param topicName The name of the topic whose configuration should be cleared.
     * @return The {@link TopicSettings} for the cleared topic. If no configuration was
     *         cleared, returns null.
     */
    @Nullable
    public static TopicSettings clearConfiguration(@Nonnull final String topicName) {
        logger.info("Clearing configuration for topic {}", topicName);
        synchronized (topicsToTrack) {
            return topicsToTrack.remove(topicName);
        }
    }

    /**
     * Clear configuration for all topics with configuration.
     *
     * @return The number of configurations cleared.
     */
    public static int clearAllConfiguration() {
        logger.info("Clearing all {} topic configurations", topicsToTrack.size());
        synchronized (topicsToTrack) {
            int configCount = topicsToTrack.size();
            topicsToTrack.clear();
            return configCount;
        }
    }

    /**
     * Get the list of all configured topics to track.
     *
     * @return An immutable copy of all topics currently being tracked.
     */
    public static Map<String, TopicSettings> getTrackingTopics() {
        clearExhaustedTopics();

        synchronized (topicsToTrack) {
            return ImmutableMap.copyOf(topicsToTrack);
        }
    }

    /**
     * Clear all topics whose limits have been exhausted.
     */
    public static void clearExhaustedTopics() {
        synchronized (topicsToTrack) {
            topicsToTrack.entrySet().removeIf(entry -> entry.getValue().getLimit().isExhausted());
        }
    }

    /**
     * Package-private.
     * <p/>
     * Remove the {@link TraxContext} from the table of contexts being tracked.
     *
     * @param traxContext The context to remove.
     * @return Whether the context was removed.
     */
    static boolean remove(@Nonnull final TraxContext traxContext) {
        return trackingTable.remove(traxContext);
    }

    /**
     * Package-private.
     * Add a {@link TraxContext} to the table of tracked contexts.
     * <p/>
     * Note that the context cannot already be in the table.
     * If the table is too large, a warning may be issued but the context will still be added.
     * <p/>
     * Note that contexts should be short-lived. They should be added on creation and removed
     * via the {@link TraxContext#close()} method. Contexts are best used in a try-with-resources
     * style statement so that no one can forget to remove the context when it is no longer
     * needed.
     *
     * @param traxContext The context to add.
     */
    static void add(@Nonnull final TraxContext traxContext) {
        Preconditions.checkArgument(!trackingTable.contains(traxContext));
        trackingTable.add(traxContext);

        if (trackingTable.size() >= TRACKING_TABLE_SIZE_WARNING_LIMIT) {
            logger.warn("Size limit exceeded for context tracking table. " +
                "This may indicate a context leak. {} >= {}",
                trackingTable.size(), TRACKING_TABLE_SIZE_WARNING_LIMIT);
        }
    }

    /**
     * Find the most verbose context for the given thread.
     *
     * @param thread The thread whose most verbose context should be found.
     * @return the most verbose context for the given thread.
     *         Returns an {@link EmptyTraxContext} if no context has been explicitly set for the thread.
     */
    @Nonnull
    public static TraxContext mostVerboseContextFor(@Nonnull final Thread thread) {
        TraxContext mostVerboseContext = EmptyTraxContext.instance();
        for (final TraxContext traxContext : trackingTable) {
            if (traxContext.ownedBy(thread) && traxContext.moreVerboseThan(mostVerboseContext)) {
                mostVerboseContext = traxContext;
            }
        }

        return mostVerboseContext;
    }

    /**
     * Get the verbosity for the most verbose context for the thread.
     *
     * @param thread The thread whose verbosity should be looked up.
     * @return the verbosity for the most verbose context for the thread.
     */
    @Nonnull
    public static Verbosity verbosity(@Nonnull final Thread thread) {
        return mostVerboseContextFor(thread).getVerbosity();
    }

    /**
     * Check whether calculation tracking is off for the thread.
     * This should return the opposite value of {@link #on(Thread)} for the given thread.
     *
     * @param thread The thread whose context should be looked up.
     * @return whether calculation tracking is off for the thread.
     */
    public static boolean off(@Nonnull final Thread thread) {
        return mostVerboseContextFor(thread).off();
    }

    /**
     * Check whether calculation tracking is on for the thread.
     * This should return the opposite value of {@link #off(Thread)} for the given thread.
     *
     * @param thread The thread whose context should be looked up.
     * @return whether calculation tracking is on for the thread.
     */
    public static boolean on(@Nonnull final Thread thread) {
        return mostVerboseContextFor(thread).on();
    }

    /**
     * Check whether calculation tracking is at trace level for the thread.
     *
     * @param thread The thread whose verbosity level should be looked up.
     * @return whether calculation tracking is on at trace level for the thread.
     */
    public static boolean trace(@Nonnull final Thread thread) {
        return mostVerboseContextFor(thread).trace();
    }

    /**
     * Create a {@link TraxContext} for use in tracking calculations for a particular thread.
     *
     * @param ownerThread The {@link Thread} that owns the context. A thread may own multiple contexts.
     *                    The msot verbose of these contexts will be used to determine the appropriate
     *                    level at which to track calculations.
     * @param trackingTopics The list of topics associated with the context.
     * @param topicConfig The configuration for the topic.
     * @return A {@link TraxContext} for the associated topics.
     */
    private static TraxContext createContext(@Nonnull final Thread ownerThread,
                                             @Nonnull final List<TopicLevel> trackingTopics,
                                             @Nonnull final TraxTopicConfiguration topicConfig) {
        if (trackingEnabled(topicConfig)) {
            return new TrackingTraxContext(ownerThread, trackingTopics, topicConfig);
        } else {
            return EmptyTraxContext.instance();
        }
    }

    /**
     * Check if tracking is enabled (ie not OFF) for the given {@link Verbosity}.
     *
     * @param verbosity The verbosity to check if tracking is enabled.
     * @return Whether calculation tracking is enabled for this particular verbosity.
     */
    public static boolean trackingEnabled(@Nonnull final Verbosity verbosity) {
        return verbosity.getNumber() > Verbosity.OFF.getNumber();
    }

    /**
     * Check if tracking is enabled (ie not OFF) for the given verbosity of the given
     * {@link TraxTopicConfiguration}.
     *
     * @param topicConfig The {@link TraxTopicConfiguration} whose verbosity should be checked.
     * @return Whether calculation tracking is enabled for this particular topic config.
     */
    public static boolean trackingEnabled(@Nonnull final TraxTopicConfiguration topicConfig) {
        return trackingEnabled(topicConfig.getVerbosity());
    }

    /**
     * An interface for limiting Trax tracking for a topic, either by time or number of uses.
     * See the protobuf comments for {@link TrackingLimit} for additional details.
     */
    @ThreadSafe
    public interface TraxLimit {
        /**
         * A constant representing an unlimited number of uses.
         */
        int UNLIMITED_USES = Integer.MIN_VALUE;

        /**
         * Configure a {@link TrackingLimitRemainder} builder to represent this object.
         * @param builder A builder for the tracking limit remainder.
         */
        void configure(@Nonnull TrackingLimitRemainder.Builder builder);

        /**
         * Request permission for calculation tracking from the limit. Returns true if permission
         * is granted, false otherwise.
         * <p/>
         * As a side effect, may exhaust a use of the limit.
         *
         * @return The number of remaining uses of the limit after decrement. If the limit is not
         *         limited by uses (ie noLimit or timeLimit), return UNLIMITED_USES.
         */
        boolean tryTracking();

        /**
         * Return whether a limit is exhausted. An exhausted limit indicates the associated
         * topic configuration should be removed, resulting in us not tracking calculations
         * for that topic anymore.
         *
         * @return Whether or not the limit has been exhausted.
         */
        boolean isExhausted();

        /**
         * Get a protobuf representation of this {@link com.vmturbo.trax.TraxConfiguration.TraxLimit}.
         *
         * @return a protobuf representation of this {@link com.vmturbo.trax.TraxConfiguration.TraxLimit}.
         */
        default TrackingLimitRemainder toProto() {
            final TrackingLimitRemainder.Builder builder = TrackingLimitRemainder.newBuilder();
            configure(builder);
            return builder.build();
        }

        /**
         * Create a new {@link com.vmturbo.trax.TraxConfiguration.TraxLimit} from a proto representation.
         *
         * @param limit The limit from which to construct a {@link com.vmturbo.trax.TraxConfiguration.TraxLimit}.
         * @return A limit constructed from {@link com.vmturbo.trax.TraxConfiguration.TraxLimit}.
         */
        static TraxLimit fromProto(@Nonnull final TrackingLimit limit) {
            if (limit.hasTimeLimit()) {
                return new TraxTimeLimit(limit.getTimeLimit());
            } else if (limit.hasUseLimit()) {
                return new TraxUseLimit(limit.getUseLimit());
            } else if (limit.hasThrottlingLimit()) {
                return new TraxThrottlingLimit(limit.getThrottlingLimit());
            } else {
                return new TraxNoLimit();
            }
        }
    }

    /**
     * A {@link TraxLimit} that can never be exhausted. A topic configuration with no
     * limit can only be cleared or replaced, it will never be removed because it has
     * been used up.
     */
    @ThreadSafe
    @Immutable
    public static class TraxNoLimit implements TraxLimit {
        private static final TraxNoLimit INSTANCE = new TraxNoLimit();

        /**
         * Create a new {@link TraxNoLimit}. We retain a singleton since there
         * are no distinct properties to an object of this type.
         */
        private TraxNoLimit() {

        }

        /**
         * Get the singleton instance of {@link TraxNoLimit}.
         *
         * @return the singleton instance of {@link TraxNoLimit}.
         */
        public static TraxNoLimit getInstance() {
            return INSTANCE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void configure(@Nonnull TrackingLimitRemainder.Builder builder) {
            builder.setNoLimit(TrackingNoLimitRemainder.newBuilder());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryTracking() {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isExhausted() {
            // NoLimit is never exhausted.
            return false;
        }
    }

    /**
     * A {@link TraxTimeLimit} is configured to trigger topic tracking for a certain time duration.
     * Once that duration expires, the limit is considered to be exhausted and the associated
     * topic configuration will no longer be used.
     */
    @ThreadSafe
    public static class TraxTimeLimit implements TraxLimit {

        private final long endTime;
        private final int originalTimeLimitMinutes;
        private final Clock clock;

        /**
         * Create a new {@link TraxTimeLimit}.
         *
         * @param timeLimitMinutes The amount of time in minutes that the configuration
         *                         associated with this limit will be in effect. This time will
         *                         start counting down immediately upon creation of the limit.
         * @param clock The clock to use to track the passage of time.
         */
        public TraxTimeLimit(final int timeLimitMinutes,
                             @Nonnull final Clock clock) {
            Preconditions.checkArgument(timeLimitMinutes >= 0);

            this.originalTimeLimitMinutes = timeLimitMinutes;
            this.clock = Objects.requireNonNull(clock);
            this.endTime = clock.millis() + TimeUnit.MINUTES.toMillis(originalTimeLimitMinutes);
        }

        /**
         * Create a new {@link TraxTimeLimit}. The system clock will be used to track
         * time for {@link TraxTimeLimit}s created with this constructor.
         *
         * @param limit The protobuf definition of the limit to use.
         */
        public TraxTimeLimit(@Nonnull final TrackingTimeLimit limit) {
            this(limit.getTimeLimitMinutes(), Clock.systemUTC());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void configure(@Nonnull Builder builder) {
            final TrackingTimeLimitRemainder remainder = TrackingTimeLimitRemainder.newBuilder()
                .setExpirationTimestamp(endTime)
                .setRemainingSeconds(remainingSeconds())
                .setOriginalTimeLimitMinutes(originalTimeLimitMinutes)
                .setHumanReadableExpirationTime(Instant.ofEpochMilli(endTime).toString())
                    .build();

            builder.setTimeLimit(remainder);
        }

        /**
         * Get the number of seconds remaining until this {@link TraxTimeLimit} is
         * exhausted.
         *
         * @return the number of seconds remaining until this {@link TraxTimeLimit} is
         * exhausted.
         */
        public double remainingSeconds() {
            return (endTime - clock.millis()) / 1000.0;
        }

        /**
         * Get the amount of time in minutes that this limit was originally configured for.
         *
         * @return the amount of time in minutes that this limit was originally configured for.
         */
        public int getOriginalTimeLimitMinutes() {
            return originalTimeLimitMinutes;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryTracking() {
            return !isExhausted();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isExhausted() {
            return remainingSeconds() <= 0;
        }
    }

    /**
     * A {@link TraxUseLimit} is configured to trigger topic tracking for a certain number of uses.
     * Once all uses are consumed, the limit is considered to be exhausted and the associated
     * topic configuration will no longer be used.
     * <p/>
     * Each call to one of the {@link Trax#track(String...)} variants for a topic associated with
     * this limit consumes exactly one use of this limit. This is true even if the configuration
     * for the associated topic is not the most verbose associated topic for that tracking call.
     */
    @ThreadSafe
    public static class TraxUseLimit implements TraxLimit {

        private final int originalUseLimit;
        private final AtomicInteger remainingUses;

        /**
         * Create a new {@link TraxUseLimit}.
         *
         * @param useCount The number of times the associated topic can be used until the limit
         *                 is exhausted.
         */
        public TraxUseLimit(final int useCount) {
            Preconditions.checkArgument(useCount >= 0);

            this.originalUseLimit = useCount;
            this.remainingUses = new AtomicInteger(useCount);
        }

        /**
         * Create a new {@link com.vmturbo.trax.TraxConfiguration.TraxUseLimit}.
         *
         * @param limit The limit definition.
         */
        public TraxUseLimit(@Nonnull final TrackingUseLimit limit) {
            this(limit.getMaxCalculationsToTrack());
        }

        /**
         * Get the number of uses originally allocated for the limit.
         *
         * @return the number of uses originally allocated for the limit.
         */
        public int getOriginalUseLimit() {
            return originalUseLimit;
        }

        /**
         * Get the number of remaining uses for the limit. This is the original number of uses
         * minus the number of times already used.
         *
         * @return the number of remaining uses for the limit.
         */
        public int getRemainingUseLimit() {
            return remainingUses.get();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void configure(@Nonnull Builder builder) {
            final TrackingUseLimitRemainder remainder = TrackingUseLimitRemainder.newBuilder()
                .setOriginalUseLimit(originalUseLimit)
                .setRemainingUseLimit(remainingUses.get())
                .build();

            builder.setUseLimit(remainder);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean tryTracking() {
            return remainingUses.getAndDecrement() > 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isExhausted() {
            return remainingUses.get() <= 0;
        }
    }

    /**
     * Options to explicitly configure the {@link TraxContext} used
     * in a particular set of calculations.
     */
    @ThreadSafe
    public static class TopicSettings {
        private final Set<String> topicNames;
        private final TraxTopicConfiguration topicConfiguration;
        private final TraxLimit limit;

        /**
         * Create a new {@link TopicSettings} with no limit.
         *
         * @param topicConfiguration The configuration for the topic.
         * @param topicNames the names of the topics that the configuration apply to.
         */
        public TopicSettings(@Nonnull final TraxTopicConfiguration topicConfiguration,
                             @Nonnull final String... topicNames) {
            this(topicConfiguration, TraxNoLimit.getInstance(), () -> Stream.of(topicNames).iterator());
        }

        /**
         * Create a new {@link TopicSettings}.
         *
         * @param topicConfiguration The configuration for the topic.
         * @param limit The limit for how this context can be used before it is exhausted.
         * @param topicNames the names of the topics that the configuration apply to.
         *                   Must contain at least one element. Must not have any empty strings.
         */
        public TopicSettings(@Nonnull final TraxTopicConfiguration topicConfiguration,
                             @Nonnull final TraxLimit limit,
                             @Nonnull final Iterable<String> topicNames) {
            Preconditions.checkArgument(topicConfiguration.getMaxDecimalPlaces() >= 0);

            this.topicNames = ImmutableSet.copyOf(topicNames);
            Preconditions.checkArgument(this.topicNames.size() > 0, "At least one topic name is required");
            Preconditions.checkArgument(!this.topicNames.stream().anyMatch(String::isEmpty),
                "Empty topic names are not permitted");

            this.topicConfiguration = Objects.requireNonNull(topicConfiguration);
            this.limit = Objects.requireNonNull(limit);
        }

        /**
         * Get the verbosity.
         *
         * @return the verbosity.
         */
        @Nonnull
        public Verbosity getVerbosity() {
            return topicConfiguration.getVerbosity();
        }

        /**
         * Get the limit.
         *
         * @return the limit.
         */
        @Nonnull
        public TraxLimit getLimit() {
            return limit;
        }

        /**
         * Get the names of the topics associated with these settings.
         *
         * @return the names of the topics associated with these settings.
         */
        public Set<String> getTopicNames() {
            return topicNames;
        }

        /**
         * Whether verbosity is configured at a high-enough level to enable tracking.
         *
         * @return Whether verbosity is configured at a high-enough level to enable tracking.
         */
        public boolean on() {
            return trackingEnabled(topicConfiguration);
        }

        /**
         * Get the max decimal places.
         *
         * @return the max decimal places.
         */
        public int getMaxDecimalPlaces() {
            return topicConfiguration.getMaxDecimalPlaces();
        }

        /**
         * Get the topic configuration.
         *
         * @return the topic configuration.
         */
        public TraxTopicConfiguration getTopicConfiguration() {
            return topicConfiguration;
        }
    }

    /**
     * A {@link TraxContext} preserves configuration details for an ongoing
     * calculation thread. A context maps to a unique thread, although a thread may have
     * multiple contexts.
     */
    @ThreadSafe
    @Immutable
    public interface TraxContext extends AutoCloseable {
        /**
         * Get the thread that owns the context.
         *
         * @return The thread that owns the context.
         */
        @Nullable
        Thread getOwnerThread();

        /**
         * Get the verbosity for the context.
         *
         * @return The verbosity for the context.
         */
        @Nonnull
        Verbosity getVerbosity();

        /**
         * Get the maximum number of decimal places to use when logging calculation details
         * captured in this context.
         *
         * @return the maximum number of decimal places to use when logging calculation details.
         */
        int getMaxDecimalPlaces();

        /**
         * Format a double value according to the context's max decimal places.
         *
         * @param value The value to format.
         * @return The formatted value.
         */
        @Nonnull
        String formatDecimal(double value);

        /**
         * Close the context. This will cause the context to be removed from the global trackingTable
         * of all in-use contexts.
         */
        @Override
        void close();

        /**
         * Check whether this {@link TraxContext} is owned by the given {@link Thread}.
         *
         * @param thread The thread to check for ownership.
         * @return whether this {@link TraxContext} is owned by the given {@link Thread}.
         */
        default boolean ownedBy(@Nonnull final Thread thread) {
            return getOwnerThread() == thread;
        }

        /**
         * Check whether calculation tracking is off for this context.
         *
         * @return whether calculation tracking is off for this context.
         */
        default boolean off() {
            return getVerbosity() == Verbosity.OFF;
        }

        /**
         * Check whether calculation tracking is enabled for this context.
         *
         * @return whether calculation tracking is enabled for this context.
         */
        default boolean on() {
            return getVerbosity() != Verbosity.OFF;
        }

        /**
         * Check whether calculation tracking verbosity is set to debug level
         * for this context.
         *
         * @return whether calculation tracking verbosity is set to debug level
         *         for this context.
         */
        default boolean debug() {
            return getVerbosity() == Verbosity.DEBUG;
        }

        /**
         * Check whether calculation tracking verbosity is set to trace level
         * for this context.
         *
         * @return whether calculation tracking verbosity is set to trace level
         *         for this context.
         */
        default boolean trace() {
            return getVerbosity() == Verbosity.TRACE;
        }

        /**
         * Check whether this context's configured verbosity is more verbose than another's.
         * <p/>
         * In the case of equal verbosity, the context with higher max decimal places is more verbose.
         *
         * @param other The context to compare.
         * @return whether this context's configured verbosity is more verbose
         *         than another's.
         */
        default boolean moreVerboseThan(@Nonnull final TraxContext other) {
            return false;
        }

        /**
         * A {@link EmptyTraxContext} is a singleton {@link TraxContext} where
         * calculation tracking is OFF. It has no owner thread.
         */
        @Immutable
        class EmptyTraxContext implements TraxContext {

            private static final EmptyTraxContext INSTANCE = new EmptyTraxContext();

            /**
             * Construct a new {@link EmptyTraxContext}.
             */
            private EmptyTraxContext() {}

            /**
             * Get the singleton instance of the {@link EmptyTraxContext}.
             *
             * @return the singleton instance of the {@link EmptyTraxContext}.
             */
            @Nonnull
            public static EmptyTraxContext instance() {
                return INSTANCE;
            }

            /**
             * {@inheritDoc}
             */
            @Nullable
            @Override
            public Thread getOwnerThread() {
                return null;
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public Verbosity getVerbosity() {
                return Verbosity.OFF;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getMaxDecimalPlaces() {
                return -1;
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public String formatDecimal(double value) {
                return Double.toString(value);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close() {
                // Nothing to do
            }
        }

        /**
         * A {@link TrackingTraxContext} permits calculation tracking within the context.
         */
        @Immutable
        class TrackingTraxContext implements TraxContext {
            private final Thread ownerThread;
            private final List<TopicLevel> topics;
            private final TraxTopicConfiguration topicConfiguration;
            private DecimalFormat decimalFormat;

            /**
             * Create a new {@link TrackingTraxContext}.
             *
             * @param ownerThread The owner thread for the context.
             * @param trackingTopics The topics being tracked.
             * @param topicConfiguration The configuration for the context.
             */
            public TrackingTraxContext(@Nonnull final Thread ownerThread,
                                       @Nonnull final List<TopicLevel> trackingTopics,
                                       @Nonnull final TraxTopicConfiguration topicConfiguration) {
                this.ownerThread = Objects.requireNonNull(ownerThread);
                this.topics = Objects.requireNonNull(trackingTopics);
                this.topicConfiguration = Objects.requireNonNull(topicConfiguration);
                this.decimalFormat = new DecimalFormat("#." + StringUtils.repeat('#',
                    topicConfiguration.getMaxDecimalPlaces()));

                TraxConfiguration.add(this);
            }

            /**
             * An empty tracking context. Not for public use. Only available to create the
             * context for constants, which are created outside of any real context.
             * <p/>
             * Contexts created in this fashion are NOT added to the list of tracked contexts
             * in the {@link TraxConfiguration} static tracking of contexts by thread.
             *
             * @param topicConfiguration The configuration for the context.
             */
            private TrackingTraxContext(@Nonnull final TraxTopicConfiguration topicConfiguration) {
                this.ownerThread = null;
                topics = Collections.emptyList();
                this.topicConfiguration = Objects.requireNonNull(topicConfiguration);
                this.decimalFormat = new DecimalFormat("#." + StringUtils.repeat('#',
                    topicConfiguration.getMaxDecimalPlaces()));
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public Verbosity getVerbosity() {
                return topicConfiguration.getVerbosity();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getMaxDecimalPlaces() {
                return topicConfiguration.getMaxDecimalPlaces();
            }

            /**
             * Get the topics associated with this context.
             *
             * @return The topics associated with this context.
             */
            public List<TopicLevel> getTopics() {
                return topics;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close() {
                TraxConfiguration.remove(this);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Thread getOwnerThread() {
                return ownerThread;
            }

            /**
             * {@inheritDoc}
             */
            @Nonnull
            @Override
            public String formatDecimal(double value) {
                return decimalFormat.format(value);
            }

            /**
             * {@inheritDoc}
             * <p/>
             * TODO: Account for decimal places in Verbosity.
             */
            @Override
            public boolean moreVerboseThan(@Nonnull final TraxContext other) {
                int verbosityComparison = Integer.compare(getVerbosity().getNumber(),
                    other.getVerbosity().getNumber());
                if (verbosityComparison > 0) {
                    // If more verbose, return true.
                    return true;
                } else {
                    // If the same verbosity, fall back to who has more decimal places.
                    return verbosityComparison == 0 && getMaxDecimalPlaces() > other.getMaxDecimalPlaces();
                }
            }
        }
    }

    /**
     * Small helper class to keep track of verbosity and decimal places when sorting topic
     * configurations.
     */
    private static final class TopicSorter {

        private Verbosity level;
        private int maxDecimalPlaces;

        /**
         * The topics scanned and sorted.
         */
        private final List<TopicLevel> topics = new ArrayList<>();

        /**
         * The scanned topics that we observed to be exhausted.
         */
        private final List<String> exhaustedTopicNames = new ArrayList<>();

        private TopicSorter() {
            level = Verbosity.OFF;
            maxDecimalPlaces = -1;
        }

        private void scanTopic(@Nonnull final String topic,
                       @Nullable final TopicSettings settings) {
            if (settings != null) {
                if (settings.getLimit().isExhausted()) {
                    // If the limit for this topic has been exhausted, add it to a list to remove
                    // and do not use it.
                    exhaustedTopicNames.add(topic);
                } else {
                    // Request the limit's permission to track the calculation.
                    if (settings.getLimit().tryTracking()) {
                        topics.add(new TopicLevel(topic, settings.getVerbosity(), settings.getMaxDecimalPlaces()));
                        if (settings.getVerbosity().getNumber() > level.getNumber()) {
                            // Discovered a more verbose topic. Take both its verbosity and max decimals.
                            level = settings.getVerbosity();
                            maxDecimalPlaces = settings.getMaxDecimalPlaces();
                        } else if (settings.getVerbosity().getNumber() == level.getNumber()) {
                            // An equally verbose topic. Take its max decimals if it's higher than the current
                            // for a topic of this verbosity.
                            if (settings.getMaxDecimalPlaces() > maxDecimalPlaces) {
                                maxDecimalPlaces = settings.getMaxDecimalPlaces();
                            }
                        }
                    }
                }
            }
        }

        private void addExplicitVerbosity(@Nonnull final Verbosity explicitVerbosity) {
            topics.add(new TopicLevel("EXPLICIT_VERBOSITY", explicitVerbosity, -1));
            level = explicitVerbosity;
        }

        private void addExplicitMaxDecimalPlaces(final int explicitMaxDecimalPlaces) {
            Preconditions.checkArgument(explicitMaxDecimalPlaces >= 0);

            topics.add(new TopicLevel("EXPLICIT_MAX_DECIMAL_PLACES", Verbosity.OFF,
                explicitMaxDecimalPlaces));
            maxDecimalPlaces = explicitMaxDecimalPlaces;
        }
    }
}
