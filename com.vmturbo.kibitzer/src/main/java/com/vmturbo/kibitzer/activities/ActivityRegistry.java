package com.vmturbo.kibitzer.activities;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.kibitzer.Kibitzer;
import com.vmturbo.kibitzer.KibitzerComponent;
import com.vmturbo.kibitzer.activities.KibitzerActivity.KibitzerActivityException;

/**
 * Registry of all available activities.
 *
 * <p>Activities are automatically discovered during instantiation, so long as they all exist
 * within the package tree in rooted by {@link ActivityRegistry}'s package.</p>
 *
 * <p>Activity instances appearing in the registry are "templates" that reflect any defaults for
 * generic config properties (the ones for which specific getters are defined in {@link
 * KibitzerActivity}. Instances for execution are initialized by copying state from from the
 * template instance, and then updating with any config properties specified in the {@link Kibitzer}
 * args.</p>
 *
 * <p>Activities are organized by the component to which they apply. When an activity specifies the
 * special `ANY` component value, a copy of that template is created for each component, and all
 * copies are added to the registry accordingly.</p>
 */
@Configuration
public class ActivityRegistry {
    private static final Logger logger = LogManager.getLogger();

    private final Map<KibitzerComponent, Map<String, KibitzerActivity<?, ?>>> activities =
            new HashMap<>();

    /**
     * Create the registry, and discover and register all activities.
     */
    public ActivityRegistry() {
        registerAllActivities();
    }

    /**
     * Register an activity for its component.
     *
     * @param activity activity to be registered
     */
    public void registerActivity(KibitzerActivity<?, ?> activity) {
        if (activity.getComponent() == null) {
            logger.error("Activity {} does not specify a component and so cannot be registered.",
                    activity);
            return;
        }
        activities.computeIfAbsent(activity.getComponent(), _component -> new HashMap<>())
                .put(activity.getTag(), activity);
    }

    /**
     * Obtain the template instance for an activity in the given component with a given tag.
     *
     * @param component component activity will be used with
     * @param tag       tag value for the activity
     * @return the template instance of the activity, if it exists in the registry
     */
    public Optional<KibitzerActivity<?, ?>> getActivity(KibitzerComponent component, String tag) {
        return Optional.ofNullable(activities.get(component)).map(map -> map.get(tag));
    }

    /**
     * Discover (via reflection) all classes that extend {@link KibitzerActivity} and that reside in
     * packages within the tree rooted at {@link ActivityRegistry}'s package.
     */
    private void registerAllActivities() {
        Reflections reflections = new Reflections(getClass().getPackage().getName(),
                new SubTypesScanner());
        reflections.getSubTypesOf(KibitzerActivity.class).forEach(activityClass -> {
            KibitzerActivity<?, ?> template;
            try {
                // Create a new instance that can be used as a template for creating new instances'
                // template for activities to be executed
                template = activityClass.newInstance();
                if (template.getComponent() == KibitzerComponent.ANY) {
                    // create separate templates for activities that can apply to any component
                    for (KibitzerComponent component : KibitzerComponent.values()) {
                        if (component != KibitzerComponent.ANY) {
                            KibitzerActivity<?, ?> componentTemplate = template.newInstance();
                            componentTemplate.setComponent(component);
                            registerActivity(componentTemplate);
                        }
                    }
                } else {
                    registerActivity(template);
                }
            } catch (InstantiationException | IllegalAccessException | KibitzerActivityException e) {
                logger.error("Failed to register activity class {}", activityClass, e);
            }
        });
    }

    /**
     * Return all the activites registerd for the given component.
     *
     * <p>If the provided component is {@link KibitzerComponent#ANY}, the result is empty. Such
     * activities must be obtained from a specific container.</p>
     *
     * @param component the component
     * @return collection contiaining all the activites, in no particular order
     */
    public Collection<KibitzerActivity<?, ?>> activitiesForComponent(KibitzerComponent component) {
        return activities.getOrDefault(component, Collections.emptyMap()).values();
    }
}
