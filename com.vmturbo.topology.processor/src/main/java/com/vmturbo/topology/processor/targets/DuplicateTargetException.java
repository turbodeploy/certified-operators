package com.vmturbo.topology.processor.targets;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Exception thrown during target registration if there is already an existing target in target store.
 */
public class DuplicateTargetException extends TargetStoreException {

    public DuplicateTargetException(@Nonnull final String targetAddr) {
        super(String.format("Target %s already exists.", targetAddr));
    }

    public DuplicateTargetException(@Nonnull final String newTarget,
            @Nonnull final Set<String> oldTargets) {
        super(formatMessage(newTarget, oldTargets));
    }

    private static String formatMessage(@Nonnull String newTarget, @Nonnull Set<String> oldTargets) {
        if (oldTargets.size() == 1) {
            return String.format("Target %s is a duplicate of target %s."
                    + " Please delete one of these targets.", newTarget,
                    oldTargets.iterator().next());
        } else {
            return String.format("Target %s is a duplicate of targets %s."
                    + " Please delete all of these targets save one.", newTarget,
                    oldTargets.stream().collect(Collectors.joining(", ")));
        }
    }
}
