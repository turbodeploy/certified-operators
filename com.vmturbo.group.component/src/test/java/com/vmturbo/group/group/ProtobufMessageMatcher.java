package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * Mockito matcher for protobuf messages. This matcher supports sets-based matching. As all the
 * sets are represented as lists in Protobuf, messages that are logically equal could be treated
 * differently by tests.
 *
 * @param <T> type of a message to match
 */
public class ProtobufMessageMatcher<T extends AbstractMessage> extends BaseMatcher<T> {
    private final T expected;
    private final Set<String> setPaths;

    /**
     * Constructs a matcher expecting the specified message to arrive. Fields listed in {@code
     * setPaths} are treated as sets instead of lists.
     *
     * @param expected expected protobuf message to match
     * @param setPaths a set of properties to ignore order for
     */
    public ProtobufMessageMatcher(@Nonnull T expected, @Nonnull Collection<String> setPaths) {
        this.expected = Objects.requireNonNull(expected);
        this.setPaths = validatePaths(expected.getDescriptorForType(), setPaths);
    }

    @Override
    public boolean matches(Object item) {
        if (!(item instanceof AbstractMessage)) {
            return false;
        }
        if (!(item.getClass().equals(expected.getClass()))) {
            return false;
        }
        final AbstractMessage actual = (AbstractMessage)item;
        return areEqual(expected, actual, "");
    }

    private boolean areEqual(@Nonnull AbstractMessage expected, @Nonnull AbstractMessage actual,
            @Nonnull String path) {
        if (!expected.getAllFields().keySet().equals(actual.getAllFields().keySet())) {
            return false;
        }
        for (Entry<FieldDescriptor, Object> entry : expected.getAllFields().entrySet()) {
            final FieldDescriptor descriptor = entry.getKey();
            final String fieldName = path + "." + descriptor.getName();
            final Object expectedSumbessage = entry.getValue();
            final Object actualSubmessage = actual.getAllFields().get(entry.getKey());
            final boolean fieldsEqual;
            if (descriptor.isMapField()) {
                fieldsEqual = areSetsEqual(descriptor, fieldName, (List<?>)expectedSumbessage,
                        (List<?>)actualSubmessage);
            } else if (descriptor.isRepeated()) {
                if (setPaths.contains(fieldName)) {
                    fieldsEqual = areSetsEqual(descriptor, fieldName, (List<?>)expectedSumbessage,
                            (List<?>)actualSubmessage);
                } else {
                    fieldsEqual = areListsEqual(descriptor, fieldName, (List<?>)expectedSumbessage,
                            (List<?>)actualSubmessage);
                }
            } else {
                fieldsEqual = areSingleObjectsEqual(descriptor, fieldName, expectedSumbessage,
                        actualSubmessage);
            }
            if (!fieldsEqual) {
                return false;
            }
        }
        return true;
    }

    private boolean areSingleObjectsEqual(@Nonnull FieldDescriptor descriptor,
            @Nonnull String fieldName, @Nonnull Object expected, @Nonnull Object actual) {
        if (descriptor.getJavaType() == JavaType.MESSAGE) {
            return areEqual((AbstractMessage)expected, (AbstractMessage)actual, fieldName);
        } else {
            return Objects.equals(expected, actual);
        }
    }

    private boolean areListsEqual(@Nonnull FieldDescriptor descriptor, @Nonnull String fieldName,
            @Nonnull List<?> expected, @Nonnull List<?> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        final Iterator<?> expectedIter = expected.iterator();
        final Iterator<?> actualIter = actual.iterator();
        while (expectedIter.hasNext()) {
            if (!areSingleObjectsEqual(descriptor, fieldName, expectedIter.next(),
                    actualIter.next())) {
                return false;
            }
        }
        return true;
    }

    private boolean areSetsEqual(@Nonnull FieldDescriptor descriptor, @Nonnull String fieldName,
            @Nonnull List<?> expected, @Nonnull List<?> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        final Set<?> expectedSet = new HashSet<>(expected);
        final Set<?> actualSet = new HashSet<>(actual);
        boolean removed = true;
        while (removed && !expectedSet.isEmpty()) {
            removed = false;
            final Iterator<?> expectedIter = expectedSet.iterator();
            while (expectedIter.hasNext() && !removed) {
                final Object expectedItem = expectedIter.next();
                final Iterator<?> actualIter = actualSet.iterator();
                while (actualIter.hasNext() && !removed) {
                    final Object actualItem = actualIter.next();
                    if (areSingleObjectsEqual(descriptor, fieldName, expectedItem, actualItem)) {
                        actualIter.remove();
                        expectedIter.remove();
                        removed = true;
                    }
                }
            }
        }
        return expectedSet.isEmpty();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(expected.toString());
    }

    private static Set<String> validatePaths(@Nonnull Descriptor message,
            @Nonnull Collection<String> paths) {
        final Set<String> result = new HashSet<>(paths.size());
        for (String path : paths) {
            final List<String> fields = Arrays.asList(path.split("\\."));
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("Fields must not be empty. Received: " + path);
            }
            validatePath(message, fields.iterator());
            result.add("." + path);
        }
        return Collections.unmodifiableSet(result);
    }

    private static void validatePath(@Nonnull Descriptor message,
            @Nonnull Iterator<String> iterator) {
        if (iterator.hasNext()) {
            final String fieldName = iterator.next();
            final FieldDescriptor descriptor = message.getFields()
                    .stream()
                    .filter(descr -> descr.getName().equals(fieldName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Failed to find field " + fieldName + " in protobuf message " +
                                    message.getFullName()));
            if (iterator.hasNext() && descriptor.getJavaType() == JavaType.MESSAGE) {
                validatePath(descriptor.getMessageType(), iterator);
            }
        }
    }
}
