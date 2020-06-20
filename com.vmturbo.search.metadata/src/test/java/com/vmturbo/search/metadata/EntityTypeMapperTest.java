package com.vmturbo.search.metadata;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.enums.EntityType;

public class EntityTypeMapperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testApiEntityTypeToProtoType() {
        for (EntityType entityType : EntityTypeMapper.SUPPORTED_ENTITY_TYPE_MAPPING.keySet()) {
            assertNotNull(EntityTypeMapper.fromApiEntityTypeToProto(entityType));
        }

        // for other api entity types, expect exception to be thrown
        Arrays.stream(EntityType.values())
                .filter(type -> !EntityTypeMapper.SUPPORTED_ENTITY_TYPE_MAPPING.containsKey(type))
                .forEach(type -> {
                    expectedException.expect(IllegalArgumentException.class);
                    EntityTypeMapper.fromApiEntityTypeToProto(type);
                });
    }
}