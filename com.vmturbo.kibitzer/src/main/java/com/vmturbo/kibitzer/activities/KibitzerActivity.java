package com.vmturbo.kibitzer.activities;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.context.ApplicationContext;

import com.vmturbo.kibitzer.Kibitzer;
import com.vmturbo.kibitzer.KibitzerComponent;
import com.vmturbo.kibitzer.KibitzerDb;
import com.vmturbo.kibitzer.KibitzerDb.DbMode;

/**
 * Base class for kibitzer actions.
 *
 * @param <T> type of the per-repetition execution result
 * @param <U> type of the overall activity result
 */
public abstract class KibitzerActivity<T, U> {
    private static final Logger logger = LogManager.getLogger();

    // names of all the "common" config properties, which will be available in all activities
    protected static final String DESCRIPTION_CONFIG_KEY = "description";
    protected static final String DB_MODE_CONFIG_KEY = "db_mode";
    protected static final String RUNS_CONFIG_KEY = "runs";
    protected static final String SCHEDULE_CONFIG_KEY = "schedule";
    protected static final String DELAY_CONFIG_KEY = "delay";

    protected final ActivityConfig config = new ActivityConfig();
    protected KibitzerComponent component;
    protected String tag;

    protected DSLContext dsl = null;
    protected KibitzerDb db;
    protected ApplicationContext context = null;
    protected Integer idNo = null;

    /**
     * Create an instance of this activity implementation.
     *
     * <p>Each instance applies to a specific component, and carries a tag that must be unique
     * among all activity implementations associated with any given component. Multiple instances of
     * the same activity can run concurrently; they will have distinguishing "id" values generated
     * by {@link Kibitzer}.
     *
     * @param component the component this instance will be used with
     * @param tag       tag for the activity implementation class
     * @throws KibitzerActivityException if there's a problem creating the instance
     */
    public KibitzerActivity(KibitzerComponent component, String tag)
            throws KibitzerActivityException {
        this.component = component;
        this.tag = tag;
        defineCommonProperties();
    }

    private void defineCommonProperties() throws KibitzerActivityException {
        config.add(config.stringProperty(DESCRIPTION_CONFIG_KEY)
                .required()
                .withDescription("documentation for this activity"));
        config.add(config.enumProperty(DB_MODE_CONFIG_KEY, DbMode.class)
                .withDefault(DbMode.NONE)
                .withDescription("DbMode value for this activity's primary database access"));
        config.add(config.intProperty(RUNS_CONFIG_KEY)
                .withDefault(0)
                .withValidator(n -> n >= 0, "must not be negative")
                .withDescription("number of times to execute activity; 0 for unbounded"));
        config.add(config.durationProperty(SCHEDULE_CONFIG_KEY)
                .withDefault(Duration.ZERO)
                .withValidator(d -> !d.isNegative(), "must not be negative")
                .withDescription(
                        "time between repetitions (not guaranteed), e.g. 10s or 5m",
                        "or 1d2h3m4.5s; 0s means no delay between repetitions"));
        config.add(config.durationProperty(DELAY_CONFIG_KEY)
                .withDefault(Duration.ZERO)
                .withValidator(d -> !d.isNegative(), "must not be negative")
                .withDescription(
                        "delay prior to first repetition, so multiple activities can be staggered"));
    }

    /**
     * Initialize this activity instance, in whatever manner is required by this implementation.
     *
     * @param dsl     {@link DSLContext} providing access to the activity's main database
     * @param db      an instance of {@link KibitzerDb} that can be used to create endpoints for
     *                access to other databases, if needed
     * @param context Spring {@link ApplicationContext} under which {@link Kibitzer} is operating
     * @throws KibitzerActivityException if there's a problem during initialization
     */
    public void init(DSLContext dsl, KibitzerDb db, ApplicationContext context)
            throws KibitzerActivityException {
        this.dsl = dsl;
        this.db = db;
        this.context = context;
    }

    /**
     * Run a repetition of this activity.
     *
     * @param i repetition number, starting with 1
     * @return the reporting result for this repetition
     * @throws KibitzerActivityException if there's a problem with the run
     */
    public Optional<T> run(int i) throws KibitzerActivityException {
        return Optional.empty();
    }

    /**
     * Assemble final reporting results for this activity as a whole (all iterations).
     *
     * @param runResults individual per-iteration results, any of which may omitted (a not-present
     *                   {@link Optional})
     * @return overall reporting data for the activity
     * @throws KibitzerActivityException if there's a problem producing the report data
     */
    public Optional<U> finish(List<T> runResults) throws KibitzerActivityException {
        return Optional.empty();
    }

    /**
     * Create a new instance of this {@link KibitzerActivity} type, with all properties defined and
     * unconfigured, except that property defaults will be in place.
     *
     * @return the new instance
     * @throws KibitzerActivityException if there's a problem creating the instance
     */
    public abstract KibitzerActivity<T, U> newInstance() throws KibitzerActivityException;

    /**
     * Configure this activity from the colon-separated segments that were attached to this
     * activity's tag on the command line when launching the {@link Kibitzer}.
     *
     * @param segments config segments (name=value pairs in the colon-separated config spec)
     * @return the configured activity, or empty if there was a problem with configuration (errors
     *         will have been logged)
     */
    public Optional<KibitzerActivity<?, ?>> configure(String... segments) {
        if (!Arrays.stream(segments).allMatch(this::configure)) {
            logger.error("Failed to configure activity {}", this);
            return Optional.empty();
        }
        List<String> badSegments = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        Arrays.stream(segments)
                .map(s -> s.split("=", 2))
                .forEach(parts -> {
                    if (parts.length == 2 && parts[0].length() > 0) {
                        props.put(parts[0], parts[1]);
                    } else {
                        badSegments.add(String.join("=", parts));
                    }
                });
        boolean ok = true;
        if (badSegments.isEmpty()) {
            for (Entry<String, String> e : props.entrySet()) {
                ActivityConfigProperty<?> prop = config.getProperty(e.getKey());
                if (prop != null) {
                    if (!prop.configure(e.getValue())) {
                        logger.error("Failed to configure property '{}' for action {}",
                                prop.getName(), this);
                        ok = false;
                    }
                } else {
                    logger.error("Unknown config prop '{}' for action {}", e.getKey(), this);
                    ok = false;
                }
            }
        }
        return ok ? Optional.of(this) : Optional.empty();
    }

    private boolean configure(String segment) {
        String[] parts = segment.split("=", 2);
        if (parts.length != 2 || parts[0].length() == 0) {
            logger.error("Invalid property segment '{}' must be `<name>=<value`", segment);
            return false;
        }
        ActivityConfigProperty<?> prop = config.getProperty(parts[0]);
        if (prop == null) {
            logger.error("Unknown config property '{}' for activity {}", parts[0], this);
            return false;
        }
        return prop.configure(parts[1]);
    }

    public KibitzerComponent getComponent() {
        return component;
    }

    public void setComponent(KibitzerComponent component) {
        this.component = component;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getDescription() {
        return config.getString(DESCRIPTION_CONFIG_KEY);
    }

    public DbMode getDbMode() {
        return config.getEnum(DB_MODE_CONFIG_KEY, DbMode.class);
    }

    public int getRuns() {
        return config.getInt(RUNS_CONFIG_KEY);
    }

    public Duration getSchedule() {
        return config.get(SCHEDULE_CONFIG_KEY, Duration.class);
    }

    public Duration getDelay() {
        return config.getDuration(DELAY_CONFIG_KEY);
    }

    public Integer getIdNo() {
        return idNo;
    }

    public void setIdNo(Integer idNo) {
        this.idNo = idNo;
    }

    public String getId() {
        return idNo != null ? tag + "-" + idNo : tag;
    }

    @Override
    public String toString() {
        return String.format("%s: %s[%s]@%d %s",
                getComponent(), getClass().getSimpleName(), getId(), System.identityHashCode(this),
                config);
    }

    /**
     * Report the overall activity result, in whatever way(s) are appropriate.
     *
     * @param result activity result
     */
    public void report(U result) {}

    public Collection<ActivityConfigProperty<?>> getConfigProperties() {
        return config.getProperties();
    }

    /**
     * Class used for reporting failures when processing or otherwise processing a {@link
     * KibitzerActivity} instance.
     */
    public static class KibitzerActivityException extends Exception {
        /**
         * Create a new instance.
         *
         * @param message exception message
         */
        public KibitzerActivityException(String message) {
            super(message);
        }

        /**
         * Create a new instance.
         *
         * @param message exception message
         * @param cause   chained exception
         */
        public KibitzerActivityException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
