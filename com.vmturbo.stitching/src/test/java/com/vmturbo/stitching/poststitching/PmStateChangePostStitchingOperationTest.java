package com.vmturbo.stitching.poststitching;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.TopologyEntityBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for {@link PmStateChangePostStitchingOperation}.
 */
public class PmStateChangePostStitchingOperationTest {
    private EntitySettingsCollection settingsMock;
    private PmStateChangePostStitchingOperation operation;
    private EntityChangesBuilder<TopologyEntity> resultBuilder;
    private IStitchingJournal<TopologyEntity> journal;

    /**
     * Set up for each unit test.
     */
    @Before
    public void setup() {
        journal = Mockito.mock(IStitchingJournal.class);
        settingsMock = Mockito.mock(EntitySettingsCollection.class);
        resultBuilder = new UnitTestResultBuilder();
        operation = new PmStateChangePostStitchingOperation();
    }

    /**
     * Tests that the failover hosts state remains unchanged if other hosts in the cluster have the
     * active state.
     */
    @Test
    public void testFailoverPMStateOthersActive() {
        testFailoverPMState(EntityState.POWERED_ON, 0);
    }

    /**
     * Tests that the failover hosts state changes to active if any other host in the cluster has
     * any other state than active and failover.
     */
    @Test
    public void testFailoverPMStateOthersNoActive() {
        testFailoverPMState(EntityState.MAINTENANCE, 1);
    }

    private void testFailoverPMState(@Nonnull EntityState pm4State, int expectedChanges) {
        final Builder pm1 = getPmBuilder(1, EntityState.POWERED_ON);
        final Builder pm2 = getPmBuilder(2, EntityState.POWERED_ON);
        final Builder pm3 = getPmBuilder(3, EntityState.FAILOVER);
        final Builder pm4 = getPmBuilder(4, pm4State);
        final List<Builder> hosts = Arrays.asList(pm1, pm2, pm3, pm4);
        final TopologyEntity virtualDatacenter = TopologyEntityBuilder.newBuilder().withEntityType(
                EntityType.VIRTUAL_DATACENTER_VALUE).withProviders(hosts).getBuilder().build();
        final TopologicalChangelog<TopologyEntity> resultOperation = operation.performOperation(
                Stream.of(virtualDatacenter), settingsMock, resultBuilder);
        Assert.assertEquals(expectedChanges, resultOperation.getChanges().size());
        resultOperation.getChanges().forEach(c -> c.applyChange(journal));
        final Iterator<TopologyEntity> hostsIterator = virtualDatacenter.getProviders().iterator();
        Assert.assertEquals(EntityState.POWERED_ON, hostsIterator.next().getEntityState());
        Assert.assertEquals(EntityState.POWERED_ON, hostsIterator.next().getEntityState());
        final EntityState expectedPm3State =
                pm4State != EntityState.POWERED_ON ? EntityState.POWERED_ON : EntityState.FAILOVER;
        Assert.assertEquals(expectedPm3State, hostsIterator.next().getEntityState());
        Assert.assertEquals(pm4State, hostsIterator.next().getEntityState());
    }

    @Nonnull
    private static Builder getPmBuilder(int i, @Nonnull EntityState poweredOn) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setOid(i)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(poweredOn));
    }
}
