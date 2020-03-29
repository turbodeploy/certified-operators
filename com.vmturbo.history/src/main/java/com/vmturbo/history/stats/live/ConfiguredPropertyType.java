package com.vmturbo.history.stats.live;

import static com.vmturbo.common.protobuf.utils.StringConstants.CONTAINER;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_STORAGES;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VDCS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_STORAGE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.TOTAL_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.VDC;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.stats.live.PropertyType.Category.Headroom;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.history.db.EntityType;

/**
 * This enum configures {@link PropertyType} instances for properties representing count metrics,
 * and for computed properties.
 *
 * <p>We also configure other properties whose values serve as operands for computed properities</p>
 */
public class ConfiguredPropertyType extends PropertyType {

    private static Map<String, ConfiguredPropertyType> nameToTypeMap = new HashMap<>();

    /** count metric for VMs. */
    private static final ConfiguredPropertyType numVMs =
            new ConfiguredPropertyType(NUM_VMS, counted(EntityType.get(VIRTUAL_MACHINE)));
    /** count metric for hosts. */
    private static final ConfiguredPropertyType numHosts =
            new ConfiguredPropertyType(NUM_HOSTS, counted(EntityType.get(PHYSICAL_MACHINE)));
    /** count metrics for storages. */
    private static final ConfiguredPropertyType numStorages =
            new ConfiguredPropertyType(NUM_STORAGES, counted(EntityType.get(STORAGE)));
    /** count metrics for containers. */
    private static final ConfiguredPropertyType numContainers =
            new ConfiguredPropertyType(NUM_CONTAINERS, counted(EntityType.get(CONTAINER)));
    /** count metrics for vdcs. */
    private static final ConfiguredPropertyType numVdcs =
            new ConfiguredPropertyType(NUM_VDCS, counted(EntityType.get(VDC)));
    /** computed property for vms-to-host ratio. */
    private static final ConfiguredPropertyType numVMsPerHost =
            new ConfiguredPropertyType(NUM_VMS_PER_HOST, computed(ratio, numVMs, numHosts));
    /** computed property for vm-to-storage ratio. */
    private static final ConfiguredPropertyType numVmsPerStorage =
            new ConfiguredPropertyType(NUM_VMS_PER_STORAGE, computed(ratio, numVMs, numStorages));
    /** computed property for containers-per-host ratio. */
    private static final ConfiguredPropertyType numContainersPerHost =
            new ConfiguredPropertyType(NUM_CNT_PER_HOST, computed(ratio, numContainers, numHosts));
    /** computed property for containers-per-storage ratio. */
    private static final ConfiguredPropertyType numContainersPerStorage =
            new ConfiguredPropertyType(NUM_CNT_PER_STORAGE, computed(ratio, numContainers, numStorages));

    /** cpu headroom, an operand for total headroom. */
    private static final ConfiguredPropertyType cpuHeadroom =
            new ConfiguredPropertyType(CPU_HEADROOM, Headroom);
    /** memory headroom, an operand for total headroom. */
    private static final ConfiguredPropertyType memHeadroom =
            new ConfiguredPropertyType(MEM_HEADROOM, Headroom);
    /** storage headroom, an operand for total headroom. */
    private static final ConfiguredPropertyType storageHeadroom =
            new ConfiguredPropertyType(STORAGE_HEADROOM, Headroom);
    /** total headroom, the min of component headrooms for both used and capacity subprops. */
    private static final ConfiguredPropertyType totalHeadroom =
            new ConfiguredPropertyType(TOTAL_HEADROOM, computed(min, cpuHeadroom, memHeadroom, storageHeadroom),
                    Headroom);

    private static CountedSpec counted(EntityType entityType) {
        return new CountedSpec(entityType);
    }

    private static ComputedSpec computed(
            Function<List<Double>, Double> computeFunction, PropertyType... prereqs) {
        return new ComputedSpec(computeFunction, prereqs);
    }

    private final String name;
    private ComputedSpec computedSpec;
    private CountedSpec countedSpec;
    private final ImmutableSet<Category> categories;

    /**
     * constructor for plain old property types.
     *
     * @param name       name of the property type
     * @param categories categories this property type belongs to
     */
    ConfiguredPropertyType(String name, Category... categories) {
        this(name, null, null, categories);
    }

    /**
     * constructor for computed properties.
     *
     * @param name         name of the property type
     * @param computedSpec specification of how to compute the property value
     * @param categories   categories this property type belongs to
     */
    ConfiguredPropertyType(String name, ComputedSpec computedSpec, Category... categories) {
        this(name, null, computedSpec, categories);
    }

    /**
     * constructor for count metrics properties.
     *
     * @param name        name of the propety type
     * @param countedSpec specification of the count metric
     * @param categories  categories this property belongs to
     */
    ConfiguredPropertyType(String name, CountedSpec countedSpec, Category... categories) {
        this(name, countedSpec, null, categories);
    }

    /**
     * common constructor used by the others.
     *
     * @param name         name of the property type
     * @param countedSpec  count metric spec, if this is a count metric property type
     * @param computedSpec compute spec, if this is a computed property type
     * @param categories   categories this property type belongs to
     */
    ConfiguredPropertyType(String name, CountedSpec countedSpec, ComputedSpec computedSpec,
            Category... categories) {
        this.name = name;
        this.countedSpec = countedSpec;
        this.computedSpec = computedSpec;
        this.categories = ImmutableSet.copyOf(categories);
        nameToTypeMap.put(name, this);
    }

    /**
     * Find a configured property type with the given name, if there is one.
     *
     * @param name name of desired property type
     * @return property type or null, if not foundxs
     */
    @Nullable
    public static PropertyType named(final String name) {
        return nameToTypeMap.get(name);
    }

    static final Set<PropertyType> allMetricPropertyTypes =
            nameToTypeMap.values().stream()
                    .filter(ConfiguredPropertyType::isCountMetric)
                    .collect(Collectors.collectingAndThen(toSet(), ImmutableSet::copyOf));

    static final Set<EntityType> allCountedEntityTypes =
            nameToTypeMap.values().stream()
                    .filter(ConfiguredPropertyType::isCountMetric)
                    .map(ConfiguredPropertyType::getCountedEntityType)
                    .collect(Collectors.collectingAndThen(toSet(), ImmutableSet::copyOf));

    static final Set<PropertyType> allComputedPropertyTypes =
            nameToTypeMap.values().stream()
                    .filter(ConfiguredPropertyType::isComputed)
                    .collect(Collectors.collectingAndThen(toSet(), ImmutableSet::copyOf));

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isComputed() {
        return computedSpec != null;
    }

    @Override
    public List<PropertyType> getOperands() {
        return computedSpec.prereqs;
    }

    @Override
    public double compute(final List<Double> values) {
        return computedSpec.computeFunction.apply(values);
    }


    @Override
    public boolean isCountMetric() {
        return countedSpec != null;
    }

    @Override
    public EntityType getCountedEntityType() {
        return isCountMetric() ? countedSpec.entityType : null;
    }

    @Override
    public boolean isInCategory(final Category category) {
        return categories.contains(category);
    }

    @Override
    public String toString() {
        return "PropertyType[" + name + "]";
    }

    /**
     * Information needed to compute a computed property type value.
     */
    private static class ComputedSpec {

        private final Function<List<Double>, Double> computeFunction;
        private final List<PropertyType> prereqs;

        /**
         * Create a new instance.
         *
         * @param computeFunction a function that computes the property value from a list of
         *                        prereq property values
         * @param prereqs         properties whose values used in this property's computation
         */
        ComputedSpec(Function<List<Double>, Double> computeFunction, PropertyType... prereqs) {
            this.computeFunction = computeFunction;
            this.prereqs = Arrays.asList(prereqs);
        }
    }

    /**
     * Information for a counted metrics property.
     */
    private static class CountedSpec {

        private final EntityType entityType;

        /**
         * Create a new instance.
         *
         * @param entityType the entity type counted by this metric
         */
        CountedSpec(EntityType entityType) {
            this.entityType = entityType;
        }
    }
}
