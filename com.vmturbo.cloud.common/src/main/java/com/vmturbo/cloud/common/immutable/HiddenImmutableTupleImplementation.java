package com.vmturbo.cloud.common.immutable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * A meta-annotation for hidden immutable tuple implementations through the immutable framework.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Style(visibility = ImplementationVisibility.PACKAGE,
        allParameters = true,
        typeImmutable = "*Tuple")
public @interface HiddenImmutableTupleImplementation {
}
