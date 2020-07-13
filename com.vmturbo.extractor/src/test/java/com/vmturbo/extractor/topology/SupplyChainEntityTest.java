package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.topology.SupplyChainEntity.Builder;

/**
 * Tests the {@link SupplyChainEntity} class.
 */
public class SupplyChainEntityTest {

    private SupplyChainEntity vm;
    private SupplyChainEntity pm;
    private TopologyEntityDTO vmEntity;
    private TopologyEntityDTO pmEntity;
    private Builder pmBuilder;
    private Builder vmBuilder;

    /**
     * Build up a couple of entities with connections between them, to be used in the tests.
     */
    @Before
    public void before() {
        this.pmEntity = mkEntity(PHYSICAL_MACHINE);
        this.pmBuilder = SupplyChainEntity.newBuilder(pmEntity);
        this.vmEntity = mkEntity(VIRTUAL_MACHINE);
        this.vmBuilder = SupplyChainEntity.newBuilder(vmEntity);
        pmBuilder.addAggregatedEntity(vmBuilder);
        vmBuilder.addAggregator(pmBuilder);
        pmBuilder.addOwnedEntity(vmBuilder);
        vmBuilder.addOwner(pmBuilder);
        pmBuilder.addOutboundAssociation(vmBuilder);
        vmBuilder.addInboundAssociation(pmBuilder);
        pmBuilder.addControlledEntity(vmBuilder);
        vmBuilder.addController(pmBuilder);
        pmBuilder.addConsumer(vmBuilder);
        vmBuilder.addProvider(pmBuilder);
        this.vm = vmBuilder.build();
        this.pm = pmBuilder.build();

    }

    /**
     * Test that the builder operations performed in {@link #before()} correctly built the entities
     * and created their connections.
     */
    @Test
    public void testSupplyChainBuilder() {
        assertThat(vm.getEntityType(), is(VIRTUAL_MACHINE_VALUE));
        assertThat(pm.getEntityType(), is(PHYSICAL_MACHINE_VALUE));
        assertThat(vm.getOid(), is(vmEntity.getOid()));
        assertThat(vm.getDisplayName(), is(vmEntity.getDisplayName()));
        // following are all hard-cded in our class
        assertThat(vm.getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(vm.getEntityState(), is(EntityState.POWERED_ON));
        assertThat(vm.getDiscoveringTargetIds().count(), is(0L));
        assertThat(vm.getVendorId(0L), is(""));
        assertThat(vm.getAllVendorIds().count(), is(0L));
        assertThat(vm.toString(), is(vmEntity.getDisplayName() + "@" + vmEntity.getOid()));
        // following should reflect our connections
        assertThat(vm.getProviders(), contains(pm));
        assertThat(pm.getConsumers(), contains(vm));
        assertThat(vm.getInboundAssociatedEntities(), contains(pm));
        assertThat(pm.getOutboundAssociatedEntities(), contains(vm));
        assertThat(vm.getOwner().orElse(null), is(pm));
        assertThat(pm.getOwnedEntities(), contains(vm));
        assertThat(vm.getAggregators(), contains(pm));
        assertThat(pm.getAggregatedEntities(), contains(vm));
        assertThat(vm.getControllers(), contains(pm));
        assertThat(pm.getControlledEntities(), contains(vm));
    }

    /**
     * Check that the clear operation works properly.
     */
    @Test
    public void testClearConsumersAndProviders() {
        pmBuilder.clearConsumersAndProviders();
        pm = pmBuilder.build();
        assertThat(pm.getProviders(), is(empty()));
        assertThat(pm.getConsumers(), is(empty()));
        assertThat(pm.getInboundAssociatedEntities(), is(empty()));
        assertThat(pm.getOutboundAssociatedEntities(), is(empty()));
        assertThat(pm.getOwnedEntities(), is(empty()));
        assertThat(pm.getOwner().orElse(null), is(nullValue()));
        assertThat(pm.getAggregators(), is(empty()));
        assertThat(pm.getAggregatedEntities(), is(empty()));
        assertThat(pm.getControllers(), is(empty()));
        assertThat(pm.getControlledEntities(), is(empty()));
        vmBuilder.clearConsumersAndProviders();
        vm = vmBuilder.build();
        assertThat(vm.getProviders(), is(empty()));
        assertThat(vm.getConsumers(), is(empty()));
        assertThat(vm.getInboundAssociatedEntities(), is(empty()));
        assertThat(vm.getOutboundAssociatedEntities(), is(empty()));
        assertThat(vm.getOwnedEntities(), is(empty()));
        assertThat(vm.getOwner().orElse(null), is(nullValue()));
        assertThat(vm.getAggregators(), is(empty()));
        assertThat(vm.getAggregatedEntities(), is(empty()));
        assertThat(vm.getControllers(), is(empty()));
        assertThat(vm.getControlledEntities(), is(empty()));
    }
}
