package com.vmturbo.topology.processor.stitching.prestitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.prestitching.ADGroupsPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test cases for {@link ADGroupsPreStitchingOperation}.
 */
public class ADGroupsPreStitchingOperationTest {

    private static final Gson GSON = new Gson();
    private static final String OID1 = "bu1";
    private static final String OID2 = "bu2";
    private static final String LOCAL_NAME = "local_name";
    private static final String DISTINGUISHED_NAME = "distinguished_name";
    private static final String RELATED_GROUPS1 = "[\"Group1\"]";
    private static final String RELATED_GROUPS2 = "[\"Group2\",\"Group3\",\"Group1\"]";

    private StitchingContext stitchingContext;
    private StitchingResultBuilder resultBuilder;
    private ADGroupsPreStitchingOperation operation =
        new ADGroupsPreStitchingOperation();

    private static EntityDTO.Builder createEntity(final EntityType entityType, final String oid,
            final String localName, final String distinguishedName, final String relatedGroups,
            final String secondRealtedGroups) {
        return EntityDTO.newBuilder()
            .setId(oid)
            .setDisplayName(oid)
            .setEntityType(entityType)
            .addEntityProperties(EntityBuilders.entityProperty()
                .named(SupplyChainConstants.LOCAL_NAME)
                .withValue(localName)
                .build())
            .addEntityProperties(EntityBuilders.entityProperty()
                .named(SupplyChainConstants.RELATED_GROUPS)
                .withValue(relatedGroups)
                .build())
            .addEntityProperties(EntityBuilders.entityProperty()
                .named("distinguishedName")
                .withValue(distinguishedName)
                .build())
            // another relatedGroups prop to simulate multiple props with the same name
            .addEntityProperties(EntityBuilders.entityProperty()
                .named(SupplyChainConstants.RELATED_GROUPS)
                .withValue(secondRealtedGroups)
                .build());
    }

    private void setupEntities(@Nonnull final EntityDTO.Builder... entities) {
        final long targetIncrement = 111L;
        final long lastUpdatedIncrement = 100L;
        final TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final long oid = 1L;
        long targetId = targetIncrement;
        long lastUpdated = lastUpdatedIncrement;

        final Collection<StitchingEntityData> entityDataList = new ArrayList<>();
        for (final EntityDTO.Builder dto : entities) {
            final StitchingEntityData stitchingData = StitchingEntityData.newBuilder(dto)
                .oid(oid)
                .targetId(targetId += targetIncrement)
                .lastUpdatedTime(lastUpdated += lastUpdatedIncrement)
                .build();

            entityDataList.add(stitchingData);
        }

        final StitchingContext.Builder builder = StitchingContext.newBuilder(entities.length, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        entityDataList.forEach(entity -> builder.addEntity(entity,
            ImmutableMap.of(entity.getLocalId(), entity)));

        stitchingContext = builder.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);
    }

    /**
     * Test Business User AD groups merge.
     */
    @Test
    public void testBusinessUserADGroupsMerge() {
        final EntityDTO.Builder buDto1 = createEntity(EntityType.BUSINESS_USER, OID1, LOCAL_NAME,
            DISTINGUISHED_NAME, RELATED_GROUPS1, RELATED_GROUPS1);
        final EntityDTO.Builder buDto2 = createEntity(EntityType.BUSINESS_USER, OID2, LOCAL_NAME,
            DISTINGUISHED_NAME, RELATED_GROUPS2, "");
        setupEntities(buDto1, buDto2);
        final TopologyStitchingEntity sourceBusinessUser = stitchingContext.getEntity(buDto1).get();
        final TopologyStitchingEntity destBusinessUser = stitchingContext.getEntity(buDto2).get();
        operation.performOperation(Stream.of(sourceBusinessUser, destBusinessUser), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(1, stitchingContext.size());
        assertEquals(3, destBusinessUser.getEntityBuilder().getEntityPropertiesCount());

        final Optional<EntityProperty> groupsProp = destBusinessUser.getEntityBuilder().getEntityPropertiesList()
            .stream().filter(p -> SupplyChainConstants.RELATED_GROUPS.equals(p.getName()))
            .findFirst();
        assertTrue(groupsProp.isPresent());
        final String groupsValue = groupsProp.get().getValue();
        final List<String> groups = GSON.fromJson(groupsValue, ArrayList.class);
        assertEquals(3, groups.size());
        assertThat(groups, containsInAnyOrder("Group1", "Group2", "Group3"));
        stitchingContext.removeEntity(sourceBusinessUser);
        stitchingContext.removeEntity(destBusinessUser);
    }

    /**
     * Test Business User AD groups merge.
     */
    @Test
    public void testDesktopPoolADGroupsMerge() {
        final EntityDTO.Builder dpDto1 = createEntity(EntityType.DESKTOP_POOL, OID1, LOCAL_NAME,
            DISTINGUISHED_NAME, RELATED_GROUPS1, RELATED_GROUPS1);
        final EntityDTO.Builder dpDto2 = createEntity(EntityType.DESKTOP_POOL, OID2, LOCAL_NAME,
            DISTINGUISHED_NAME, RELATED_GROUPS2, "");
        setupEntities(dpDto1, dpDto2);
        final TopologyStitchingEntity sourceDesktopPool = stitchingContext.getEntity(dpDto1).get();
        final TopologyStitchingEntity destinationDesktopPool = stitchingContext.getEntity(dpDto2).get();
        operation.performOperation(Stream.of(sourceDesktopPool, destinationDesktopPool), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(1, stitchingContext.size());
        assertEquals(3, destinationDesktopPool.getEntityBuilder().getEntityPropertiesCount());

        final Optional<EntityProperty> groupsProp = destinationDesktopPool.getEntityBuilder().getEntityPropertiesList()
            .stream().filter(p -> SupplyChainConstants.RELATED_GROUPS.equals(p.getName()))
            .findFirst();
        assertTrue(groupsProp.isPresent());
        final String groupsValue = groupsProp.get().getValue();
        final List<String> groups = GSON.fromJson(groupsValue, ArrayList.class);
        assertEquals(3, groups.size());
        assertThat(groups, containsInAnyOrder("Group1", "Group2", "Group3"));
        stitchingContext.removeEntity(sourceDesktopPool);
        stitchingContext.removeEntity(destinationDesktopPool);
    }
}
