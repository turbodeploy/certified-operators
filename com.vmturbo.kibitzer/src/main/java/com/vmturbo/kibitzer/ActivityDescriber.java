package com.vmturbo.kibitzer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kibitzer.activities.ActivityConfigProperty;
import com.vmturbo.kibitzer.activities.ActivityRegistry;
import com.vmturbo.kibitzer.activities.KibitzerActivity;

/**
 * Class to print descriptions of activities.
 *
 * <p>This happens when the first argument to {@link Kibitzer#main(String[])} is `--describe`.
 * Following args are "activity specs", of the form "&lt;component&gt;:&lt;tag&gt;". Either
 * component or tag may be be the special string "*" which represents a wildcard.</p>
 */
public class ActivityDescriber {
    private static final Logger logger = LogManager.getLogger();
    private static final String WILD = "*";
    private static final String SEPARATOR = ":";

    private final Set<KibitzerActivity<?, ?>> activities;
    private final ActivityRegistry registry;

    /**
     * Create a new instance.
     *
     * @param args all the activity specs
     */
    public ActivityDescriber(String[] args) {
        this.registry = new ActivityRegistry();
        this.activities = findActivities(args);
    }

    /**
     * Describe all the activites that match the activity specs.
     *
     * <p>The activities are described in the order they matched activity specs. If more than
     * one spec matched a particular activity, that activity appears where it appeared in its first
     * match.</p>
     *
     * <p>For a wild activity spec, the list of matching activities are added in an order that
     * comes from sorting first by component name and then by actity tag.</p>
     *
     * <p>Currently, the only option for output is the job pod's log. We can probably do
     * better than that.</p>
     */
    public void describeAll() {
        activities.forEach(this::describe);
    }

    private LinkedHashSet<KibitzerActivity<?, ?>> findActivities(String[] names) {
        LinkedHashSet<KibitzerActivity<?, ?>> activities = new LinkedHashSet<>();
        for (String name : names) {
            if (!name.contains(SEPARATOR)) {
                name = WILD + SEPARATOR + name;
            }
            String[] parts = name.split(Pattern.quote(SEPARATOR), 2);
            activities.addAll(findActivities(wildIfEmpty(parts[0]), wildIfEmpty(parts[1])));
        }
        return activities;
    }

    private List<KibitzerActivity<?, ?>> findActivities(String componentName, String tag) {
        List<KibitzerActivity<?, ?>> foundActivities = new ArrayList<>();
        List<KibitzerComponent> components = new ArrayList<>();
        if (componentName.equals(WILD)) {
            Arrays.stream(KibitzerComponent.values())
                    .sorted(Comparator.comparing(KibitzerComponent::name))
                    .forEach(components::add);
        } else {
            try {
                components.add(KibitzerComponent.valueOf(componentName.toUpperCase()));
            } catch (IllegalArgumentException e) {
                logger.error("Skipping {}.{} due to invalid component name", componentName, tag);
            }
        }
        for (KibitzerComponent component : components) {
            if (tag.equals(WILD)) {
                registry.activitiesForComponent(component).stream()
                        .sorted(Comparator.comparing(KibitzerActivity::getTag))
                        .forEach(foundActivities::add);
            } else {
                registry.getActivity(component, tag).ifPresent(foundActivities::add);
            }
        }
        return foundActivities;
    }


    private String wildIfEmpty(String s) {
        return s.isEmpty() ? WILD : s;
    }

    private void describe(KibitzerActivity<?, ?> activity) {
        System.out.println(
                "================================================================================");
        System.out.printf("Activity tag '%s'\n", activity.getTag());
        System.out.printf("Component: %s\n", activity.getComponent());
        System.out.printf("Description: %s\n", activity.getDescription());
        System.out.println("Properties:");
        activity.getConfigProperties().stream()
                .sorted(Comparator.comparing(ActivityConfigProperty::getName))
                .forEach(this::describeProperty);
    }

    private void describeProperty(ActivityConfigProperty<?> property) {
        System.out.printf("  %s:\n", property.getName());
        String label = "description";
        for (String line : property.getDescription()) {
            System.out.printf("    %-11s: %s\n", label, line);
            label = "";
        }
        if (property.getDefault() != null) {
            System.out.printf("    %-11s: %s\n", "default", property.getDefault());
        }
        Class<?> type = property.getType();
        if (Enum.class.isAssignableFrom(type)) {
            String values = Arrays.stream(type.getEnumConstants())
                    .map(constant -> (Enum<?>)constant)
                    .map(Enum::name)
                    .collect(Collectors.joining(", "));
            System.out.printf("    %-11s: %s\n", "values", values);
        }
    }
}
