package com.vmturbo.cloud.common.immutable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * A style annotation to hide the implementation class for immutable data classes.
 */
@Target({ElementType.PACKAGE, ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Style(
        visibility = ImplementationVisibility.PACKAGE,
        overshadowImplementation = true)
public @interface HiddenImmutableImplementation {
}
