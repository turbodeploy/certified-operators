package com.vmturbo.history.db;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static com.vmturbo.common.protobuf.utils.StringConstants.APPLICATION_COMPONENT;
import static com.vmturbo.common.protobuf.utils.StringConstants.DATA_CENTER;
import static com.vmturbo.common.protobuf.utils.StringConstants.NETWORK;
import static com.vmturbo.common.protobuf.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.db.EntityType.UseCase.PersistEntity;
import static com.vmturbo.history.db.EntityType.UseCase.PersistStats;
import static com.vmturbo.history.db.EntityType.UseCase.RollUp;
import static com.vmturbo.history.db.EntityType.UseCase.Spend;
import static com.vmturbo.history.db.EntityTypeDefinitions.APPLICATION_SPEND_ENTITY_TYPE;
import static com.vmturbo.history.db.EntityTypeDefinitions.ENTITY_TYPE_DEFINITIONS;
import static com.vmturbo.history.db.EntityTypeDefinitions.NAME_TO_ENTITY_TYPE_MAP;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Tests to sanity-check information in the EntityType enum.
 */
public class EntityTypeTest extends Assert {

    private static final EntityType VIRTUAL_MACHINE_ENTITY_TYPE = NAME_TO_ENTITY_TYPE_MAP.get(VIRTUAL_MACHINE);
    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = NAME_TO_ENTITY_TYPE_MAP.get(PHYSICAL_MACHINE);
    private static final EntityType DATA_CENTER_ENTITY_TYPE = NAME_TO_ENTITY_TYPE_MAP.get(DATA_CENTER);
    private static final EntityType APPLICATION_ENTITY_TYPE = NAME_TO_ENTITY_TYPE_MAP.get(APPLICATION_COMPONENT);
    private static final EntityType NETWORK_ENTITY_TYPE = NAME_TO_ENTITY_TYPE_MAP.get(NETWORK);

    /**
     * Set up for tests that are expected to throw exceptions.
     */
    @Rule
    public ExpectedException exceptionGrabber = ExpectedException.none();

    /**
     * Make sure that entity types know their names, with or without aliases.
     */
    @Test
    public void testThatGetNameWorks() {
        assertEquals(StringConstants.PHYSICAL_MACHINE, PHYSICAL_MACHINE_ENTITY_TYPE.getName());
        assertEquals(StringConstants.DATA_CENTER, DATA_CENTER_ENTITY_TYPE.getName());
    }

    /**
     * Make sure that aliasing works.
     */
    @Test
    public void testThatAliasingWorks() {
        assertThat(DATA_CENTER_ENTITY_TYPE.resolve(), is(PHYSICAL_MACHINE_ENTITY_TYPE));
        assertThat(PHYSICAL_MACHINE_ENTITY_TYPE.getName(), is(StringConstants.PHYSICAL_MACHINE));
        assertThat(DATA_CENTER_ENTITY_TYPE.getName(), is(StringConstants.DATA_CENTER));
    }


    /**
     * Make sure that instance sharing works.
     */
    @Test
    public void testThatEntityTypesAreSingletonPerName() {
        assertThat(EntityType.get(VIRTUAL_MACHINE), sameInstance(EntityType.named(VIRTUAL_MACHINE).get()));
    }

    /**
     * Make sure that an attempt to get an entity type for an unkown name throws an exception.
     */
    @Test
    public void testThatGetThrowsOnUnknownTypeName() {
        exceptionGrabber.expect(IllegalArgumentException.class);
        EntityType.get("foobar");
    }

    /**
     * Make sure that we get the correct tables associated with entity types, and vice-versa.
     */
    @Test
    public void testThatTableDeliveryWorks() {

        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getTablePrefix().get(), is("vm_stats"));

        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getLatestTable().get(), is(VmStatsLatest.VM_STATS_LATEST));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getHourTable().get(), is(VmStatsByHour.VM_STATS_BY_HOUR));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getDayTable().get(), is(VmStatsByDay.VM_STATS_BY_DAY));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getMonthTable().get(), is(VmStatsByMonth.VM_STATS_BY_MONTH));

        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.LATEST).get(), is(VmStatsLatest.VM_STATS_LATEST));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.HOUR).get(), is(VmStatsByHour.VM_STATS_BY_HOUR));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.DAY).get(), is(VmStatsByDay.VM_STATS_BY_DAY));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.MONTH).get(), is(VmStatsByMonth.VM_STATS_BY_MONTH));

        assertThat(EntityType.fromTable(VmStatsLatest.VM_STATS_LATEST).get(), is(VIRTUAL_MACHINE_ENTITY_TYPE));
        assertThat(EntityType.fromTable(VmStatsByHour.VM_STATS_BY_HOUR).get(), is(VIRTUAL_MACHINE_ENTITY_TYPE));
        assertThat(EntityType.fromTable(VmStatsByDay.VM_STATS_BY_DAY).get(), is(VIRTUAL_MACHINE_ENTITY_TYPE));
        assertThat(EntityType.fromTable(VmStatsByMonth.VM_STATS_BY_MONTH).get(), is(VIRTUAL_MACHINE_ENTITY_TYPE));
        assertThat(EntityType.fromTable(Entities.ENTITIES), isEmpty());
    }

    /**
     * Make sure that when we provide an unsupported time frame asking for tables, the method
     * throws an exception.
     */
    @Test
    public void testThatBogusTimeFrameThrows() {
        exceptionGrabber.expect(IllegalArgumentException.class);
        VIRTUAL_MACHINE_ENTITY_TYPE.getTimeFrameTable(TimeFrame.YEAR);
    }

    /**
     * Make sure that we get the correct SDK entity types from entity types, and vice-versa.
     */
    @Test
    public void testThatSdkAssociationsWork() {
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.getSdkEntityType(), isPresent());
        assertThat(EntityDTO.EntityType.VIRTUAL_MACHINE, sameInstance(VIRTUAL_MACHINE_ENTITY_TYPE.getSdkEntityType().get()));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE, sameInstance(EntityType.fromSdkEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE).get()));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE, sameInstance(EntityType.fromSdkEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber()).get()));
        // special cases
        assertThat(EntityType.fromSdkEntityType(EntityDTO.EntityType.APPLICATION_COMPONENT).get(),
                is(APPLICATION_ENTITY_TYPE));
    }

    /**
     * Make sure that use-case queries and related convenience methods work correctly.
     */
    @Test
    public void testThatUseCasesWork() {
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.hasUseCase(PersistEntity), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.hasUseCase(PersistStats), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.hasUseCase(RollUp), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.hasUseCase(Spend), is(false));

        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.persistsEntity(), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.persistsStats(), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.rollsUp(), is(true));
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.isSpend(), is(false));
    }

    /**
     * Make sure we properly report whether entity types persist price index data.
     */
    @Test
    public void testThatPersistsPriceIndexWorks() {
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.persistsPriceIndex(), is(true));
        assertThat(NETWORK_ENTITY_TYPE.persistsPriceIndex(), is(false));
        assertThat(APPLICATION_SPEND_ENTITY_TYPE.persistsPriceIndex(), is(false));
    }

    /**
     * Make sure that all our configured entity definitions appear in the
     * {@link EntityType#allEntityTypes()} method, and no others.
     */
    @Test
    public void testThatEnumeratorsWork() {
        final EntityType[] configured = ENTITY_TYPE_DEFINITIONS.toArray(new EntityType[0]);
        assertThat(EntityType.allEntityTypes(), hasItems(configured));
    }

    /**
     * Make sure that our {@link Object#toString()} override works.
     */
    @Test
    public void testThatToStringWorks() {
        assertThat(VIRTUAL_MACHINE_ENTITY_TYPE.toString(), is("EntityType[VirtualMachine]"));
    }

    /**
     * Test that OM-56003 is fixed.
     *
     * <p>The issue reported potential NPEs when processing topologies, which came down to the use
     * of {@link Optional#of(Object)} where {@link Optional#ofNullable(Object)} was required, in
     * {@link EntityType#fromSdkEntityType(int)}. This test breaks without the fix and passes
     * with the fix.</p>
     */
    @Test
    public void testThatOM56603IsFixed() {
        // we don't know what entity types might be added to EntityTypeDefinitions in the future,
        // so we'll just loop through all the SDK entity types and make sure none of them NPEs.
        for (final EntityDTO.EntityType sdkEntityType : EntityDTO.EntityType.values()) {
            EntityType.fromSdkEntityType(sdkEntityType);
        }
    }
}
