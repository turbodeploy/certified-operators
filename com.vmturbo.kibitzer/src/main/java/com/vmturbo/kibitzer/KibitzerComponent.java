package com.vmturbo.kibitzer;

import java.util.Arrays;

import javax.annotation.Nullable;

import com.vmturbo.kibitzer.components.HistoryComponentInfo;

/**
 * XL components currently supported as Kibitzer targets.
 */
public enum KibitzerComponent {
    /** history component. */
    HISTORY(HistoryComponentInfo.get()),
    /**
     * ANY value will never appear in an executing activity or even in a template instance in the
     * registry. When discovered by the registry, copies of the template will be created for all
     * components, and those copies will be registered.
     */
    ANY(null);

    private final ComponentInfo componentInfo;

    KibitzerComponent(ComponentInfo componentInfo) {
        this.componentInfo = componentInfo;
    }

    /**
     * Get a {@link KibitzerComponent} value by name.
     *
     * @param name component name
     * @return KibitzerComponent member
     */
    @Nullable
    public static KibitzerComponent named(String name) {
        return Arrays.stream(values())
                .filter(c -> c.componentInfo.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Get a list of {@link KibitzerComponent} excluding the ANY instance.
     *
     * @return all values except ANY
     */
    public static KibitzerComponent[] nonWildcardValues() {
        return Arrays.stream(values())
                .filter(c -> c != ANY)
                .toArray(KibitzerComponent[]::new);
    }

    public String getName() {
        return componentInfo.getName();
    }

    /**
     * Get the {@link ComponentInfo} for this component.
     *
     * @return component info
     */
    public ComponentInfo info() {
        return componentInfo;
    }
}
