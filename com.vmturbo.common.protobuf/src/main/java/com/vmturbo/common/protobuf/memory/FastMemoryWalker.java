package com.vmturbo.common.protobuf.memory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Traverse a graph of memory references using reflection in a way
 * that does not retain relationships between objects. A memory walk using this
 * type of walker has less performance overhead than one using a {@link RelationshipMemoryWalker}.
 * <p/>
 * Designed for use with {@link MemoryVisitor}'s that do not require information
 * about relationships to objects or their reachability paths (such as if you just
 * want sizes and counts of objects or you want to build a histogram of reachable
 * objects by their classes).
 */
public class FastMemoryWalker extends MemoryWalker {

    private static final Logger logger = LogManager.getLogger();

    private final Set<Object> visited;
    private final MemoryVisitor<?> visitor;

    /**
     * Create a new {@link FastMemoryWalker}.
     *
     * @param visitor The visitor to use for visits to objects traversed on the memory walk.
     *                The visitor's {@link MemoryVisitor#visit(Object, int, Object)} will be
     *                called for each object traversed during the memory walk. Note all calls
     *                to the visit method will provide a null {@code data} parameter.
     */
    public FastMemoryWalker(@Nonnull final MemoryVisitor<?> visitor) {
        this.visitor = Objects.requireNonNull(visitor);
        visited = Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * Walk the graph of memory references reachable from the roots in breadth-first order.
     *
     * @param roots The root set of objects from which to begin the memory walk.
     */
    public void traverse(final Object... roots) {
        List<Object> curLayer = new ArrayList<>();
        List<Object> nextLayer = new ArrayList<>();
        final Map<Class<?>, List<String>> inaccessibleFields = new HashMap<>();
        int depth = 0;

        for (Object root : roots) {
            // Only continue traversal deeper from this branch if the visitor says to.
            if (visited.add(root) && visitor.visit(root, depth, null)) {
                curLayer.add(root);
            }
        }

        // Use a stack instead of recursion to avoid OOM for deep memory graphs
        while (!curLayer.isEmpty()) {
            depth++;

            for (Object obj : curLayer) {
                if (obj.getClass().isArray()) {
                    if (obj.getClass().getComponentType().isPrimitive()) {
                        continue; // No need to traverse primitives
                    }

                    for (Object element : (Object[])obj) {
                        if (element != null && visited.add(element) && visitor.visit(element, depth, null)) {
                            nextLayer.add(element);
                        }
                    }
                } else {
                    traverseObject(obj, nextLayer, inaccessibleFields, depth);
                }
            }

            curLayer.clear();
            final List<Object> tmp = curLayer;
            curLayer = nextLayer;
            nextLayer = tmp;
        }

        if (!inaccessibleFields.isEmpty()) {
            logger.warn("Unable to access the following class fields:\n{}", inaccessibleFields.entrySet().stream()
                .map(entry -> entry.getKey().getName() + ": "
                    + entry.getValue().stream().collect(Collectors.joining(",")))
                .collect(Collectors.joining("\n"))
            );
        }
    }

    private void traverseObject(@Nonnull final Object obj,
                                @Nonnull final List<Object> nextLayer,
                                @Nonnull final Map<Class<?>, List<String>> inaccessibleFields,
                                final int depth) {
        final Field[] fields = getFieldsForTraversal(obj.getClass());
        for (int i = 0; i < fields.length; i++) {
            final Field field = fields[i];
            if (field != null) {
                Object child = null;
                try {
                    child = field.get(obj);
                } catch (Exception e) {
                    try {
                        field.setAccessible(true);
                        child = field.get(obj);
                    } catch (Exception e2) {
                        logger.debug("Unable to access field " + field.getName() + ": ", e);
                        inaccessibleFields.computeIfAbsent(obj.getClass(),
                            klass -> new ArrayList<>()).add(field.getName());
                        fields[i] = null; // Drop the field for the future since its inaccessible
                    }
                }
                if (child != null && visited.add(child) && visitor.visit(child, depth, null)) {
                    nextLayer.add(child);
                }
            }
        }
    }
}
