package com.vmturbo.protoc.spring.rest.method;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.protoc.plugin.common.generator.EnumDescriptor;
import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor.MessageFieldVisitor;

class MethodGenerationFieldVisitor implements MessageFieldVisitor {
    private final Deque<String> path;
    private final Set<String> boundVariables;
    private final Map<String, FieldDescriptor> queryParamsWithType = new HashMap<>();
    private final Map<String, FieldDescriptor> boundVarsWithType = new HashMap<>();

    public MethodGenerationFieldVisitor(final Set<String> boundVariables) {
        this.boundVariables = boundVariables;
        path = new ArrayDeque<>();
    }

    @Nonnull
    public Map<String, FieldDescriptor> getQueryParamFields() {
        return queryParamsWithType;
    }

    @Nonnull
    public Map<String, FieldDescriptor> getPathFields() {
        return boundVarsWithType;
    }

    @Override
    public boolean startMessageField(@Nonnull final FieldDescriptor field, @Nonnull final MessageDescriptor messageType) {
        path.push(field.getProto().getName());
        return true;
    }

    @Override
    public void endMessageField(@Nonnull final FieldDescriptor field, @Nonnull final MessageDescriptor messageType) {
        path.pop();
    }

    @Override
    public void visitBaseField(@Nonnull final FieldDescriptor field) {
        final String fullPath = getFullPath(field);
        if (!boundVariables.contains(fullPath)) {
            queryParamsWithType.put(fullPath, field);
        } else {
            boundVarsWithType.put(fullPath, field);
        }
    }

    @Override
    public void visitEnumField(@Nonnull final FieldDescriptor field, @Nonnull final EnumDescriptor enumDescriptor) {
        final String fullPath = getFullPath(field);
        if (!boundVariables.contains(fullPath)) {
            queryParamsWithType.put(fullPath, field);
        } else {
            boundVarsWithType.put(fullPath, field);
        }
    }

    @Nonnull
    private String getFullPath(@Nonnull final FieldDescriptor field) {
        if (path.isEmpty()) {
            return field.getProto().getName();
        } else {
            return path.stream().collect(Collectors.joining(".")) +
                    "." + field.getProto().getName();
        }
    }
}
