package com.vmturbo.kibitzer.activities;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Functions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Schema;
import org.jooq.Table;

import com.vmturbo.history.db.EntityType;
import com.vmturbo.kibitzer.activities.KibitzerActivity.KibitzerActivityException;

/**
 * Class to manage config properties for {@link KibitzerActivity}s.
 */
public class ActivityConfig {
    private static final Logger logger = LogManager.getLogger();

    // all the properties for this activity
    private final Map<String, ActivityConfigProperty<?>> properties = new LinkedHashMap<>();

    /**
     * Get the {@link ActivityConfigProperty} for this activity, given its name.
     *
     * @param name the property name
     * @return property with that name, or null if there is none
     */
    public ActivityConfigProperty<?> getProperty(String name) {
        return properties.get(name);
    }

    /**
     * Add an {@link ActivityConfigProperty} to this {@link ActivityConfig}. All properties are
     * added during {@link KibitzerActivity} construction, with the base class adding properties
     * that are common across all activities, and the activity implementation class supplying
     * activity-specific properties.
     *
     * @param property property to be added
     */
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public void add(ActivityConfigProperty<?> property) {
        properties.put(property.getName(), property);
    }

    /**
     * Get the {@link ActivityConfigProperty} with the given name and type associated with this
     * {@link ActivityConfig}.
     *
     * @param name property name
     * @param cls  class of property values
     * @param <T>  type of property values
     * @return the property instance, or null if not found
     */
    public <T> T get(String name, @SuppressWarnings("unused") Class<T> cls) {
        //noinspection unchecked
        ActivityConfigProperty<T> prop = (ActivityConfigProperty<T>)properties.get(name);
        return prop.get();
    }

    /**
     * Set the default value for the named config property.
     *
     * @param name  name of property
     * @param value value to use as as a default
     * @param <T>   type of property  values
     */
    public <T> void setDefault(String name, T value) {
        //noinspection unchecked
        ActivityConfigProperty<T> prop = (ActivityConfigProperty<T>)properties.get(name);
        prop.setDefault(value);
    }

    /**
     * Create a new string-valued {@link ActivityConfigProperty}.
     *
     * @param name property name
     * @return the new property instance
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    public ActivityConfigProperty<String> stringProperty(String name)
            throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, String.class)
                .withParser(Functions.identity());
    }

    /**
     * Get the value configured for a string-valued property.
     *
     * @param name property nam
     * @return string value configured for property, or default if none configured
     */
    public String getString(String name) {
        return get(name, String.class);
    }

    /**
     * Create a new integer-valued {@link ActivityConfigProperty}.
     *
     * @param name property name
     * @return the new property
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    public ActivityConfigProperty<Integer> intProperty(String name)
            throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, Integer.class)
                .withParser(Integer::parseInt);
    }

    /**
     * Get the value of an integer-valued property.
     *
     * @param name property name
     * @return configured value of the property, or the property default if not configured
     */
    public int getInt(String name) {
        return get(name, Integer.class);
    }

    /**
     * Create a new enum-valued {@link ActivityConfigProperty}.
     *
     * @param name      property name
     * @param enumClass enum type that is the property value type
     * @param <E>       enum type
     * @return newly constructed property
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    public <E extends Enum<E>> ActivityConfigProperty<E> enumProperty(
            String name, Class<E> enumClass) throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, enumClass)
                .withParser(memberName -> Enum.valueOf(enumClass, memberName));
    }

    /**
     * Get the value of an enum-valued property.
     *
     * @param name      property name
     * @param enumClass class of enum which is the value type of the property
     * @param <E>       enum type
     * @return configured value of property, or its default if none is set
     */
    public <E extends Enum<E>> E getEnum(String name, Class<E> enumClass) {
        return get(name, enumClass);
    }

    /**
     * Create a new {@link Table}-valued property.
     *
     * @param name   property name
     * @param schema {@link Schema} containing allowed {@link Table} values
     * @return new property
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    @SuppressWarnings("rawtypes")
    public ActivityConfigProperty<Table> tableProperty(String name, Schema schema)
            throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, Table.class)
                .withParser(schema::getTable)
                .withValidator(Objects::nonNull, "must not be null");
    }

    /**
     * Get the value of a table-valued property.
     *
     * @param name property name
     * @return configured value, or property default if none is configured
     */
    @SuppressWarnings("rawtypes")
    public Table getTable(String name) {
        return get(name, Table.class);
    }

    /**
     * Predicate to test whether a table is a history entity-stats table.
     *
     * @param table table to check
     * @return true if the table is an entity stats table
     */
    public static boolean isEntityStatsTable(Table<?> table) {
        return EntityType.fromTable(table).map(EntityType::persistsEntity).orElse(false);
    }

    /**
     * Create a new boolean-valued property.
     *
     * @param name property name
     * @return the new property
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    public ActivityConfigProperty<Boolean> booleanProperty(String name)
            throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, Boolean.class)
                .withParser(Boolean::parseBoolean);
    }

    /**
     * Get the value of a boolean-valued property.
     *
     * @param name property name
     * @return configured boolean value, or property default if no value has been set
     */
    public boolean getBoolean(String name) {
        return get(name, Boolean.class);
    }

    /**
     * Create a new {@link Duration}-valued property.
     *
     * <p>The parser configured for the property is one that accepts strings that follow the
     * requirements of the {@link Duration#parse(CharSequence)} method, but will automatically fill
     * in missing P, D, and T characters if they are missing. So e.g. "3m" turns into "PT3s", and
     * "1d2h3m4s" turns into "P1dT2h3m4s".</p>
     *
     * @param name property name
     * @return new property
     * @throws KibitzerActivityException if there's a problem creating the property
     */
    public ActivityConfigProperty<Duration> durationProperty(String name)
            throws KibitzerActivityException {
        return new ActivityConfigProperty<>(name, Duration.class)
                .withParser(s -> Duration.parse(fixUpDuration(s)));
    }

    /**
     * Get the value of a {@link Duration}-valued property.
     *
     * @param name property name
     * @return configured value, or property default
     */
    public Duration getDuration(String name) {
        return get(name, Duration.class);
    }

    private static String fixUpDuration(String valString) {
        String duration = valString.toUpperCase();
        // temporarily get the initial "P" out of the way if it's there
        if (duration.startsWith("P")) {
            duration = duration.substring(1);
        }
        if (!duration.contains("D") && !duration.startsWith("T")) {
            // a duration with no DAYS part must have a "T" before all its time parts (which must
            // be all there is). So we put one there if it's not already there.
            duration = "T" + duration;
        }
        // finally, add (or restore) the required initial "P"
        return "P" + duration;
    }

    @Override
    public String toString() {
        String props = properties.values().stream()
                .sorted(Comparator.comparing(ActivityConfigProperty::getName))
                .map(p -> String.format("%s=%s", p.getName(), p.get()))
                .collect(Collectors.joining(","));
        return "@" + System.identityHashCode(this) + "{" + props + "}";
    }

    public Collection<ActivityConfigProperty<?>> getProperties() {
        return properties.values();
    }
}
