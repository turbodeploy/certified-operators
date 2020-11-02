package com.vmturbo.common.protobuf.memory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;

/**
 * Traverse a graph of memory references using reflection in a way
 * that retains relationships between objects. A memory walk using this
 * type of walker has more performance overhead than one using a {@link FastMemoryWalker}.
 * <p/>
 * Designed for use with {@link MemoryVisitor}'s that do not require information
 * about relationships to objects or their reachability paths (such as if you just
 * want sizes and counts of objects or you want to build a histogram of reachable
 * objects by their classes).
 */
public class RelationshipMemoryWalker extends MemoryWalker {

    private static final Logger logger = LogManager.getLogger();

    final Set<Object> visited;
    final MemoryVisitor<MemoryReferenceNode> visitor;

    /**
     * Create a new {@link RelationshipMemoryWalker}.
     *
     * @param visitor The visitor to use for visits to objects traversed on the memory walk.
     *                The visitor's {@link MemoryVisitor#visit(Object, int, Object)} will be
     *                called for each object traversed during the memory walk. Note all calls
     *                to the visit method will provide a MemoryReferenceNode object as the
     *                {@code data} parameter.
     */
    public RelationshipMemoryWalker(@Nonnull final MemoryVisitor<MemoryReferenceNode> visitor) {
        this.visitor = Objects.requireNonNull(visitor);
        visited = Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * Walk the graph of memory references reachable from the roots in breadth-first order.
     * Multiple calls to {@link #traverse(Object...)} remember objects traversed in prior calls.
     *
     * @param roots The roots to use as a root set in the walk.
     */
    public void traverse(final Object... roots) {
        final List<NamedObject> namedRoots = new ArrayList<>(roots.length);
        for (int i = 0; i < roots.length; i++) {
            if (roots.length == 1) {
                namedRoots.add(new NamedObject("r", roots[i]));
            } else {
                namedRoots.add(new NamedObject("r" + i, roots[i]));
            }
        }

        traverseNamed(namedRoots);
    }

    /**
     * Walk the graph of memory references reachable from the roots in breadth-first order.
     * Multiple calls to {@link #traverseNamed(Collection)} remember objects traversed in prior calls.
     *
     * @param roots The roots to use as a root set in the walk.
     */
    public void traverseNamed(final Collection<NamedObject> roots) {
        List<MemoryReferenceNode> curLayer = new ArrayList<>();
        List<MemoryReferenceNode> nextLayer = new ArrayList<>();
        final Map<Class<?>, List<String>> inaccessibleFields = new HashMap<>();
        int depth = 0;

        for (NamedObject namedRoot : roots) {
            final Object root = namedRoot.rootObj;
            final MemoryReferenceNode node = new MemoryReferenceNode(null, root, namedRoot.name, depth);

            // Only continue traversal deeper from this branch if the visitor says to.
            if (visited.add(root) && visitor.visit(root, depth, node)) {
                curLayer.add(node);
            }
        }

        // Use a stack instead of recursion to avoid OOM for deep memory graphs
        while (!curLayer.isEmpty()) {
            depth++;

            for (MemoryReferenceNode parent : curLayer) {
                final Object obj = parent.obj;
                if (obj.getClass().isArray()) {
                    if (obj.getClass().getComponentType().isPrimitive()) {
                        continue; // No need to traverse primitives
                    }

                    final Object[] elements = (Object[])obj;
                    for (int index = 0; index < elements.length; index++) {
                        final Object element = elements[index];
                        if (element != null && visited.add(element)) {
                            final MemoryReferenceNode node =
                                new MemoryReferenceNode(parent, element, index, depth);
                            if (visitor.visit(element, depth, node)) {
                                nextLayer.add(node);
                            }
                        }
                    }
                } else {
                    traverseObject(obj, parent, nextLayer, inaccessibleFields, depth);
                }
            }

            curLayer.clear();
            final List<MemoryReferenceNode> tmp = curLayer;
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
                                @Nonnull final MemoryReferenceNode parent,
                                @Nonnull final List<MemoryReferenceNode> nextLayer,
                                @Nonnull final Map<Class<?>, List<String>> inaccessibleFields,
                                final int depth) {
        Field[] fields = getFieldsForTraversal(obj.getClass());
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
                if (child != null && visited.add(child)) {
                    final MemoryReferenceNode node =
                        new MemoryReferenceNode(parent, child, field.getName(), depth);
                    if (visitor.visit(child, depth, node)) {
                        nextLayer.add(node);
                    }
                }
            }
        }
    }

    /**
     * A memory reference traversed during the memory walk.
     */
    public static class MemoryReferenceNode {
        private final MemoryReferenceNode parent;
        private final Object obj;
        private final int depth;

        /**
         * When parent is an Object, this is the name of the member variable of this
         * object in the parent. When parent is an array, this is the string value of
         * the index in the parent.
         */
        private final String nameOrIndex;

        /**
         * Create a new {@link MemoryReferenceNode}.
         *
         * @param parent The parent of this node.
         * @param obj The object at this node.
         * @param name The name of the object. For a root object this will be a user-supplied name.
         *             For an object with a parent this will be the name or index of the field
         *             containing this object.
         * @param depth The depth at which we reached this object during the memory walk.
         */
        public MemoryReferenceNode(@Nullable final MemoryReferenceNode parent,
                                   @Nonnull final Object obj,
                                   @Nonnull final String name,
                                   final int depth) {
            this.parent = parent;
            this.obj = Objects.requireNonNull(obj);
            this.nameOrIndex = Objects.requireNonNull(name);
            this.depth = depth;
        }

        /**
         * Create new {@link MemoryReferenceNode} whose parent object is an array.
         *
         * @param parent The parent of this node.
         * @param obj The object at this node.
         * @param index The index of this object in the parent object array.
         * @param depth The depth at which we reached this object during the memory walk.
         */
        public MemoryReferenceNode(@Nullable final MemoryReferenceNode parent,
                                   @Nonnull final Object obj,
                                   final int index,
                                   final int depth) {
            this.parent = parent;
            this.obj = Objects.requireNonNull(obj);
            this.nameOrIndex = Integer.toString(index);
            this.depth = depth;
        }

        /**
         * Get the parent of this node.
         *
         * @return Get the parent of this node.
         */
        public MemoryReferenceNode getParent() {
            return parent;
        }

        /**
         * Get the reachability path to this node in the memory reference graph generated by
         * walking the object references from the root set. This will always be a shortest path.
         *
         * @return the path to this node in the memory reference graph.
         */
        public String pathDescriptor() {
            return parent == null
                ? nameOrIndex
                : parent.pathDescriptor() + descriptorAsChild();
        }

        /**
         * Get the object at this node.
         *
         * @return the object at this node.
         */
        public Object getObj() {
            return obj;
        }

        /**
         * Get the class of the object at this node.
         *
         * @return the class of the object at this node.
         */
        public Class<?> getKlass() {
            return obj.getClass();
        }

        /**
         * Get the name or index of the object. For a root object this will be a user-supplied name.
         * For an object with a parent this will be the name or index of the field containing this object.
         *
         * @return the name or index of the object at this node.
         */
        public String getNameOrIndex() {
            return nameOrIndex;
        }

        /**
         * Get the depth at which we reached this object during the memory walk.
         *
         * @return the depth at which we reached this object during the memory walk.
         */
        public int getDepth() {
            return depth;
        }

        /**
         * Helper method for appending this object nameOrIndex when constructing a reachability path
         * to the object at this node.
         *
         * @return A string for appending this object nameOrIndex when constructing a reachability path.
         */
        private String descriptorAsChild() {
            boolean childOfArray = parent != null && parent.getKlass().isArray();
            return childOfArray
                ? ("[" + nameOrIndex + "]")
                : ("." + nameOrIndex);
        }
    }
}
