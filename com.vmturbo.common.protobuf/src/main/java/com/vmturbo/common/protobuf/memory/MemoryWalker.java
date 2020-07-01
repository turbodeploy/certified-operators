package com.vmturbo.common.protobuf.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Abstract class for traversing reachable objects from a root set. {@link MemoryWalker}
 * objects are designed to be used in conjunction with {@link MemoryVisitor} instances.
 * The {@link MemoryWalker} guarantees only one visit to an object reachable from the root
 * set during a given memory walk.
 * <p/>
 * Memory walks are performed in breadth-first fashion. This offers the following advantages:
 * 1. When there are multiple paths to an object reachable from a root set, we will always
 *    traverse a shortest path to that object first.
 * 2. It permits visits to objects in depth order to perform depth-based analysis of the graph
 *    of reachable objects from the root set without any sorting overhead.
 */
public abstract class MemoryWalker {

    /**
     * A cache of the fields for traversal for each class that we have
     * walked so far.
     */
    protected Map<Class<?>, Field[]> fieldsCache;

    /**
     * Create a new {@link MemoryWalker}.
     */
    protected MemoryWalker() {
        fieldsCache = new HashMap<>();
    }

    /**
     * Get the fields to traverse during a memory walk for a given class.
     * Will return fields for both the base class and the superclass.
     *
     * @param klass The class whose fields we want to traverse.
     * @return the fields to traverse during a memory walk for a given class.
     */
    protected Field[] getFieldsForTraversal(@Nonnull final Class<?> klass) {
        Field[] existing = fieldsCache.get(klass);
        if (existing != null) {
            return existing;
        }

        final ArrayList<Field> forTraversal = new ArrayList<>();

        // Handle base class
        for (Field field : klass.getDeclaredFields()) {
            if (!(Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive())) {
                forTraversal.add(field);
            }
        }

        // Handle superclasses
        Class<?> superClass = klass.getSuperclass();
        while (superClass != null) {
            for (Field field : superClass.getDeclaredFields()) {
                if (!(Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive())) {
                    forTraversal.add(field);
                }
            }

            superClass = superClass.getSuperclass();
        }

        Field[] fields = new Field[forTraversal.size()];
        fields = forTraversal.toArray(fields);
        fieldsCache.put(klass, fields);
        return fields;
    }
}
