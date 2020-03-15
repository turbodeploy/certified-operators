package com.vmturbo.history.stats.live;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;

/**
 * A representation of property types appearing in history records.
 *
 * <p>This is currently specifically designed to meet the needs of
 * {@link ComputedPropertiesProcessorFactory}, but it could evolve into something more
 * widely useful.</p>
 */
public abstract class PropertyType {

    /**
     * Map of "default" instances created on demand, so the same instance can always be used
     * for a given property type name.
     */
    private static Map<String, PropertyType> nameToTypeMap =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * Obtain an instance for the given property name.
     *
     * <p>We first check to see whether a {@link ConfiguredPropertyType} exists for the given
     * name. If not, we create one with default behaviors and stash it in our name lookup map.
     *
     * @param name property name
     * @return property type instance
     */
    public static PropertyType named(String name) {
        final PropertyType type = ConfiguredPropertyType.named(name);
        return type != null ? type : nameToTypeMap.computeIfAbsent(name, _name -> new PropertyType() {
            // we only define methods that are not abstract in this class, plus toString
            // to make debugging easier
            @Override
            public String getName() {
                return _name;
            }
        });
    }

    /**
     * Function that can be used by any computed property that is a ratio of two others.
     */
    protected static Function<List<Double>, Double> ratio = v -> v.get(1) > 0 ? v.get(0) / v.get(1) : 0;

    /**
     * Function that computes the minimum of values provided by some any number of others.
     */
    protected static Function<List<Double>, Double> min = v -> v.stream().reduce(Double.MAX_VALUE, Math::min);

    /**
     * Get the name of this instance.
     *
     * @return the name
     */
    public abstract String getName();

    /**
     * Check if this a computed property type.
     *
     * @return true if this is computed
     */
    public boolean isComputed() {
        return false;
    }

    /**
     * If this is a computed property type, return the properties that serve as operands
     * for this property's computation.
     *
     * @return list of operands
     */
    public List<PropertyType> getOperands() {
        return Collections.emptyList();
    }

    /**
     * Compute a value for this property, given values for its operands.
     *
     * @param values values of operand properties
     * @return computed value
     */
    public double compute(List<Double> values) {
        return 0.0;
    }

    /**
     * Check whether this is a count metric property type.
     *
     * @return true if this is a count metric
     */
    public boolean isCountMetric() {
        return false;
    }

    /**
     * Get the underlying entity type for a count metric property type.
     *
     * @return the underlying entity type
     */
    public EntityType getCountedEntityType() {
        return null;
    }

    /**
     * Check whether this property type is in the given category.
     *
     * @param category property type category
     * @return true if this property type is in the category
     */
    public boolean isInCategory(Category category) {
        return false;
    }

    /**
     * Get all the defined count metrics property types.
     *
     * @return the property types
     */
    public static Collection<PropertyType> getMetricPropertyTypes() {
        return ConfiguredPropertyType.allMetricPropertyTypes;
    }

    /**
     * Get all the entity types that underly any defined count metric properties.
     *
     * @return entity types
     */
    public static Collection<EntityType> getCountedEntityTypes() {
        return ConfiguredPropertyType.allCountedEntityTypes;
    }

    /**
     * Get all the defined computed property types.
     *
     * @return the property types
     */
    public static Collection<PropertyType> getComputedPropertyTypes() {
        return ConfiguredPropertyType.allComputedPropertyTypes;
    }

    @Override
    public String toString() {
        return "PropertyType[" + getName() + "]";
    }

    /**
     * Enum with property type categories that can be configured for specific property types.
     */
    public enum Category {
        /** property types related ot headroom stats. */
        Headroom;
    }
}
