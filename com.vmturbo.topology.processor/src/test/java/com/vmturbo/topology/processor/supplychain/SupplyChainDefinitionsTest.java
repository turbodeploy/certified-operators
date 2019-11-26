package com.vmturbo.topology.processor.supplychain;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.stitching.TopologyEntity;

public class SupplyChainDefinitionsTest extends AbstractSupplyChainTest {

    private SupplyChainDefinitions supplyChainDefinitions;

    @Before
    public void init() {
        super.init();
        supplyChainDefinitions = new SupplyChainDefinitions(getProbeStore(), getTargetStore());
    }

    /**
     * Tests an entity with a single supply chain validation base template and an entity with two.
     * In the second case, one template has priority.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testRetrieveSupplyChainTemplates() throws Exception {
        final DiscoveryOrigin.Builder originWithOneTarget = DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(HYPERVISOR_TARGET_ID, PerTargetEntityInformation.getDefaultInstance());
        final DiscoveryOrigin.Builder originWithTwoTargets = DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(HYPERVISOR_TARGET_ID, PerTargetEntityInformation.getDefaultInstance())
                .putDiscoveredTargetData(STORAGE_TARGET_ID, PerTargetEntityInformation.getDefaultInstance());

        // create VM entity with only 1 discovering target with base template
        final TopologyEntityDTO.Builder entityWithOneTargetBuilder = TopologyEntityDTO.newBuilder().setOid(1L)
                .setDisplayName("VM entity with 1 target discovered").setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(originWithOneTarget));
        final TopologyEntity entityWithOneTarget = TopologyEntity.newBuilder(entityWithOneTargetBuilder).build();

        // create Storage entity with 2 discovering targets, both with base templates
        // note that the storage template has priority
        final TopologyEntityDTO.Builder entityWithTwoTargetsBuilder = TopologyEntityDTO.newBuilder().setOid(2L)
                .setDisplayName("Storage entity with 2 targets discovered").setEntityType(EntityType.STORAGE_VALUE)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(originWithTwoTargets));
        final TopologyEntity entityWithTwoTargets = TopologyEntity.newBuilder(entityWithTwoTargetsBuilder).build();

        // Retrieve related supply chain for the entities
        final Collection<TemplateDTO> templates1 =
            supplyChainDefinitions.retrieveSupplyChainTemplates(entityWithOneTarget);
        final Collection<TemplateDTO> templates2 =
            supplyChainDefinitions.retrieveSupplyChainTemplates(entityWithTwoTargets);

        // Retrieve VM supply chain from hypervisor supply chain templates
        final TemplateDTO expectedTemplate1 =
            SupplyChainTestUtils.retrieveEntityTemplate(
                SupplyChainTestUtils.hypervisorSupplyChain(), entityWithOneTarget);
        final TemplateDTO expectedTemplate2 =
            SupplyChainTestUtils.retrieveEntityTemplate(
                SupplyChainTestUtils.storageSupplyChain(), entityWithTwoTargets);

        // the hypervisor base template should be found for the VM
        Assert.assertEquals(1, templates1.size());
        Assert.assertEquals(expectedTemplate1, templates1.stream().findFirst().orElse(null));

        // only the storage base template should be found for the storage entity
        Assert.assertEquals(1, templates2.size());
        Assert.assertEquals(expectedTemplate2, templates2.stream().findFirst().orElse(null));
    }

    /**
     * Tests the case of two base templates with the same priority.  One of the templates should be chosen,
     * but the choice is arbitrary.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testTwoEqualPriorityBases() throws Exception {
        final TopologyEntity vm =
            makeTopologyEntity(EntityType.VIRTUAL_MACHINE, COMPETING_TARGET_ID, HYPERVISOR_TARGET_ID);
        final Collection<TemplateDTO>
            templates = supplyChainDefinitions.retrieveSupplyChainTemplates(vm);

        // a single template obtained
        Assert.assertEquals(1, templates.size());

        // one of the two competing templates is found
        final TemplateDTO templateFound = templates.stream().findFirst().orElse(null);
        Assert.assertTrue(
            templateFound.equals(
                SupplyChainTestUtils.retrieveEntityTemplate(SupplyChainTestUtils.hypervisorSupplyChain(),
                vm)) ||
            templateFound.equals(
                SupplyChainTestUtils.retrieveEntityTemplate(SupplyChainTestUtils.competingSupplyChain(),
                vm)));
    }

    /**
     * Tests the case with three discovery targets: two bases with different priority and one extension.
     * We should get a collection of two templates, one base and one extension.  We should get no errors.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testThreeTemplates() throws Exception {
        final TopologyEntity st =
            makeTopologyEntity(EntityType.STORAGE, BROKEN_TARGET_ID, HYPERVISOR_TARGET_ID, STORAGE_TARGET_ID);

        final Collection<TemplateDTO> templates = supplyChainDefinitions.retrieveSupplyChainTemplates(st);

        // two templates found
        Assert.assertEquals(2, templates.size());

        // obtain the two templates
        boolean baseTemplateExists = false;
        boolean extensionTemplateExists = false;
        for (TemplateDTO template : templates) {
            if (template.getTemplateType() == TemplateType.BASE) {
                // the base template must be that of the storage probe
                Assert.assertEquals(
                    SupplyChainTestUtils.retrieveEntityTemplate(
                        SupplyChainTestUtils.storageSupplyChain(), st),
                    template);
                baseTemplateExists  = true;
            } else {
                // the extension template must be that of the "broken" probe
                Assert.assertEquals(
                    SupplyChainTestUtils.retrieveEntityTemplate(
                        SupplyChainTestUtils.brokenSupplyChain(), st),
                    template);
                extensionTemplateExists = true;
            }
        }

        // both a base and an extension template were found
        Assert.assertTrue(baseTemplateExists);
        Assert.assertTrue(extensionTemplateExists);
    }

    /**
     * Tests the case of an entity that gets discovered by two targets of the same probe.
     * The supply chain definition should keep one template.  No errors should be thrown.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testTwoTargetsFromTheSameProbe() throws Exception {
        final TopologyEntity vm =
            makeTopologyEntity(EntityType.VIRTUAL_MACHINE, HYPERVISOR_TARGET_1_ID, HYPERVISOR_TARGET_ID);
        final Collection<TemplateDTO> templates = supplyChainDefinitions.retrieveSupplyChainTemplates(vm);

        // only one storage base template should be found
        Assert.assertEquals(1, templates.size());
        Assert.assertEquals(
            SupplyChainTestUtils.retrieveEntityTemplate(SupplyChainTestUtils.hypervisorSupplyChain(), vm),
            templates.stream().findFirst().orElse(null));
    }

    @Nonnull
    private TopologyEntity makeTopologyEntity(@Nonnull EntityType entityType, @Nonnull Long ... targetIds) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        Arrays.stream(targetIds).forEach(id -> origin
                        .putDiscoveredTargetData(id,
                                                 PerTargetEntityInformation.getDefaultInstance()));
        return
            TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder().setOid(1L).setDisplayName("name").
                setEntityType(entityType.ordinal()).setOrigin(Origin.newBuilder().setDiscoveryOrigin(origin)))
            .build();
    }
}
