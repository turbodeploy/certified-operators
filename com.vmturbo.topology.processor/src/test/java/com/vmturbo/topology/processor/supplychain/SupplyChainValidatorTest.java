package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityBoughtNotFoundFailure;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityNotFoundFailure;
import com.vmturbo.topology.processor.supplychain.errors.ProviderCardinalityFailure;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;

/**
 * The main class to test supply chain validation.  Tests are based on the two supply chains defined
 * in {@link SupplyChainTestUtils}, a simple hypervisor chain and a simple storage chain.
 * The hypervisor probe discovers VMs, PMs, storage.  The storage probe discovers storage, and diskarrays.
 */
public class SupplyChainValidatorTest extends AbstractSupplyChainTest {

    private SupplyChainValidator supplychainValidator;

    private final long PEAK = 100L;
    private final long USED = 50L;

    // ids for 5 entities: VM, PM, Datacenter, Storage, Diskarray
    private final long vmOid = 1024L;
    private final long pmOid = 2333L;
    private final long dcOid = 8888L;
    private final long stOid = 12345654321L;
    private final long daOid = 437L;
    private final long lpOid = 199437L;

    // sold commodities
    private final Collection<CommoditySoldDTO> vmSoldCommodities =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.VCPU_VALUE, null),
                createCommoditySoldDTO(CommodityType.VMEM_VALUE, null),
                createCommoditySoldDTO(CommodityType.APPLICATION_VALUE, SupplyChainTestUtils.KEY));
    private final Collection<CommoditySoldDTO> pmSoldCommodities =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.CPU_VALUE, null),
                createCommoditySoldDTO(CommodityType.MEM_VALUE, null),
                createCommoditySoldDTO(CommodityType.BALLOONING_VALUE, null),
                createCommoditySoldDTO(CommodityType.IO_THROUGHPUT_VALUE, null),
                createCommoditySoldDTO(CommodityType.CLUSTER_VALUE, SupplyChainTestUtils.KEY));
    private final Collection<CommoditySoldDTO> dcSoldCommodities =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.POWER_VALUE, null),
                createCommoditySoldDTO(CommodityType.COOLING_VALUE, null),
                createCommoditySoldDTO(CommodityType.DATACENTER_VALUE, SupplyChainTestUtils.KEY));
    private final Collection<CommoditySoldDTO> stSoldCommodities =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                createCommoditySoldDTO(CommodityType.STORAGE_AMOUNT_VALUE, null),
                createCommoditySoldDTO(CommodityType.STORAGE_CLUSTER_VALUE, SupplyChainTestUtils.KEY),
                createCommoditySoldDTO(CommodityType.DSPM_ACCESS_VALUE, SupplyChainTestUtils.KEY));
    private final Collection<CommoditySoldDTO> daSoldCommodities =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                createCommoditySoldDTO(CommodityType.STORAGE_AMOUNT_VALUE, null),
                createCommoditySoldDTO(CommodityType.EXTENT_VALUE, SupplyChainTestUtils.KEY));

    // bought commodities
    // these are defined as builders so they can change in various tests
    private final CommoditiesBoughtFromProvider.Builder vmBoughtCommoditiesFromPM =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(pmOid).
            setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE).addAllCommodityBought(
                    Arrays.asList(
                        createCommodityBoughtDTO(CommodityType.CPU_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.MEM_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.BALLOONING_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.NET_THROUGHPUT_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.IO_THROUGHPUT_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.CLUSTER_VALUE, SupplyChainTestUtils.KEY),
                        createCommodityBoughtDTO(CommodityType.DATASTORE_VALUE, SupplyChainTestUtils.KEY)));
    private final CommoditiesBoughtFromProvider.Builder vmBoughtCommoditiesFromSt =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(stOid).
            setProviderEntityType(EntityType.STORAGE_VALUE).addAllCommodityBought(
                    Arrays.asList(
                        createCommodityBoughtDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.STORAGE_AMOUNT_VALUE, null),
                        createCommodityBoughtDTO(
                            CommodityType.STORAGE_CLUSTER_VALUE, SupplyChainTestUtils.KEY),
                        createCommodityBoughtDTO(
                            CommodityType.DSPM_ACCESS_VALUE, SupplyChainTestUtils.KEY)));
    private final CommoditiesBoughtFromProvider.Builder pmBoughtCommoditiesFromDC =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(dcOid).
            setProviderEntityType(EntityType.DATACENTER_VALUE).addAllCommodityBought(
                    Arrays.asList(
                        createCommodityBoughtDTO(CommodityType.POWER_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.COOLING_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.DATACENTER_VALUE, SupplyChainTestUtils.KEY)));
    private final CommoditiesBoughtFromProvider.Builder pmBoughtCommoditiesFromSt =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(stOid).
                    setProviderEntityType(EntityType.STORAGE_VALUE).addAllCommodityBought(
                    Arrays.asList(
                        createCommodityBoughtDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.STORAGE_LATENCY_VALUE, null)));
    private final CommoditiesBoughtFromProvider.Builder stBoughtCommoditiesFromDa =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(daOid).
                    setProviderEntityType(EntityType.DISK_ARRAY_VALUE).addAllCommodityBought(
                    Arrays.asList(
                        createCommodityBoughtDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.STORAGE_AMOUNT_VALUE, null),
                        createCommodityBoughtDTO(CommodityType.EXTENT_VALUE, SupplyChainTestUtils.KEY)));

    // entity "prototypes"
    // these are defined as builders so they can change in various tests
    private final TopologyEntityDTO.Builder vmPrototype =
            TopologyEntityDTO.newBuilder().setOid(vmOid).setDisplayName("VM entity").
            setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
    private final TopologyEntityDTO.Builder pmPrototype =
            TopologyEntityDTO.newBuilder().setOid(pmOid).setDisplayName("PM entity").
            setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);
    private final TopologyEntityDTO.Builder dcPrototype =
            TopologyEntityDTO.newBuilder().setOid(dcOid).setDisplayName("Datacenter entity").
            setEntityType(EntityType.DATACENTER_VALUE);
    private final TopologyEntityDTO.Builder stPrototype =
            TopologyEntityDTO.newBuilder().setOid(stOid).setDisplayName("Storage entity").
            setEntityType(EntityType.STORAGE_VALUE);
    private final TopologyEntityDTO.Builder daPrototype =
            TopologyEntityDTO.newBuilder().setOid(daOid).setDisplayName("Diskarray entity").
            setEntityType(EntityType.DISK_ARRAY_VALUE);


    @Before
    public void init() {
        super.init();
        supplychainValidator = new SupplyChainValidator(getProbeStore(), getTargetStore());
    }

    /**
     * This test checks a valid scenario in which VM buys from PM and ST, PM buys from DC and ST,
     * and ST buys from DA.
     * Hypervisor probe discovers VM, PM, ST and storage probe discovers ST, DA.
     */
    @Test
    public void testValidTopology() {
        // make topology
        final TopologyEntity.Builder validVM =
            TopologyEntity.newBuilder(buildEntity(
                vmPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), vmSoldCommodities,
                Arrays.asList(
                    vmBoughtCommoditiesFromPM.build(), vmBoughtCommoditiesFromSt.build())));
        final TopologyEntity.Builder validPM =
            TopologyEntity.newBuilder(buildEntity(
                pmPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), pmSoldCommodities,
                Arrays.asList(
                    pmBoughtCommoditiesFromDC.build(), pmBoughtCommoditiesFromSt.build())));
        final TopologyEntity.Builder validDC =
            TopologyEntity.newBuilder(buildEntity(
                dcPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), dcSoldCommodities, Collections.emptyList()));
        final TopologyEntity.Builder validSt =
            TopologyEntity.newBuilder(buildEntity(
                stPrototype, Arrays.asList(HYPERVISOR_TARGET_ID, STORAGE_TARGET_ID), stSoldCommodities,
                    Collections.singletonList(stBoughtCommoditiesFromDa.build())));
        final TopologyEntity.Builder validDa =
            TopologyEntity.newBuilder(buildEntity(
                daPrototype, Arrays.asList(HYPERVISOR_TARGET_ID, STORAGE_TARGET_ID), daSoldCommodities,
                Collections.emptyList()));

        // VM consumes from PM
        validVM.addProvider(validPM);
        validPM.addConsumer(validVM);

        // VM consumes from ST
        validVM.addProvider(validSt);
        validSt.addConsumer(validVM);

        // PM consumes from DC
        validPM.addProvider(validDC);
        validDC.addConsumer(validPM);

        // PM consumes from ST
        validPM.addProvider(validSt);
        validSt.addConsumer(validPM);

        // ST consumes from DA
        validSt.addProvider(validDa);
        validDa.addConsumer(validSt);

        // validate the topology
        final List<SupplyChainValidationFailure> errors =
            supplychainValidator.validateTopologyEntities(
                Stream.of(
                    validVM.build(), validPM.build(), validDC.build(), validSt.build(), validDa.build()));

        // no errors are expected
        Assert.assertEquals(0, errors.size());
    }

    /**
     * We only introduce a single VM in the topology.  One of the mandatory commodities sold is missing.
     *
     * As a result we get three errors:
     * - VM is missing a sold commodity.
     * - VM does not have a storage provider.
     * - VM does not have a PM provider.
     *
     * The following features are tested:
     * - the validator catches the missing commodity.
     * - the validator catches a missing hosting provider (the PM).
     * - the validator catches a missing mandatory underlying provider (the storage).
     * - the validator does not stop on first error: it produces all the errors it finds.
     */
    @Test
    public void testSingleVMWithMissingSoldCommodity() {
        // VM with missing one commodity
        final Collection<CommoditySoldDTO> missingSoldCommodity =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.VCPU_VALUE, null),
                createCommoditySoldDTO(CommodityType.APPLICATION_VALUE, SupplyChainTestUtils.KEY));
        final TopologyEntityDTO.Builder vm_missingSoldCommodity =
            buildEntity(vmPrototype,
                Collections.singletonList(HYPERVISOR_TARGET_ID), missingSoldCommodity,
                Collections.singletonList(vmBoughtCommoditiesFromPM.build()));

        final List<SupplyChainValidationFailure> errors =
            supplychainValidator.validateTopologyEntities(
                Stream.of(TopologyEntity.newBuilder(vm_missingSoldCommodity).build()));

        // three errors
        Assert.assertEquals(3, errors.size());

        // first error: commodity VMEM is not sold by the VM
        Assert.assertTrue(errors.get(0) instanceof MandatoryCommodityNotFoundFailure);
        final MandatoryCommodityNotFoundFailure commodityNotFoundException =
            (MandatoryCommodityNotFoundFailure)errors.get(0);
        Assert.assertEquals(
            CommodityType.VMEM, commodityNotFoundException.getCommodity().getCommodityType());

        // second and third error: VM is not hosted by any PM and is not layered over any storage
        Assert.assertTrue(errors.get(1) instanceof ProviderCardinalityFailure);
        Assert.assertTrue(errors.get(2) instanceof ProviderCardinalityFailure);
    }

    /**
     * From the valid topology, we remove one bought commodity and the mandatory key of another.
     * The validator should detect and report both errors as missing bought commodities.
     */
    @Test
    public void testMissingBoughtCommodity() {
        // VM commodities bought from storage: STORAGE_ACCESS is missing and STORAGE_CLUSTER has no key
        final CommoditiesBoughtFromProvider.Builder vmMissingBoughtCommoditiesFromSt =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(stOid).
                setProviderEntityType(EntityType.STORAGE_VALUE).addAllCommodityBought(
                Arrays.asList(
                    createCommodityBoughtDTO(CommodityType.STORAGE_AMOUNT_VALUE, null),
                    createCommodityBoughtDTO(CommodityType.STORAGE_CLUSTER_VALUE, ""),
                    createCommodityBoughtDTO(CommodityType.DSPM_ACCESS_VALUE, SupplyChainTestUtils.KEY)));
        final TopologyEntity.Builder vmMissingBoughtCommodity =
            TopologyEntity.newBuilder(buildEntity(
                vmPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), vmSoldCommodities,
                Arrays.asList(
                   vmMissingBoughtCommoditiesFromSt.build(), vmBoughtCommoditiesFromPM.build())));
        final TopologyEntity.Builder validPM =
            TopologyEntity.newBuilder(buildEntity(
                pmPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), pmSoldCommodities,
                Arrays.asList(
                    pmBoughtCommoditiesFromDC.build(), pmBoughtCommoditiesFromSt.build())));
        final TopologyEntity.Builder validDC =
            TopologyEntity.newBuilder(buildEntity(
                dcPrototype, Collections.singletonList(HYPERVISOR_TARGET_ID), dcSoldCommodities, Collections.emptyList()));
        final TopologyEntity.Builder validSt =
            TopologyEntity.newBuilder(buildEntity(
                stPrototype, Arrays.asList(HYPERVISOR_TARGET_ID, STORAGE_TARGET_ID), stSoldCommodities,
                    Collections.singletonList(stBoughtCommoditiesFromDa.build())));
        final TopologyEntity.Builder validDa =
            TopologyEntity.newBuilder(buildEntity(
                daPrototype, Arrays.asList(HYPERVISOR_TARGET_ID, STORAGE_TARGET_ID), daSoldCommodities,
                Collections.emptyList()));

        // VM consumes from PM
        vmMissingBoughtCommodity.addProvider(validPM);
        validPM.addConsumer(vmMissingBoughtCommodity);

        // VM consumes from ST
        vmMissingBoughtCommodity.addProvider(validSt);
        validSt.addConsumer(vmMissingBoughtCommodity);

        // PM consumes from DC
        validPM.addProvider(validDC);
        validDC.addConsumer(validPM);

        // PM consumes from ST
        validPM.addProvider(validSt);
        validSt.addConsumer(validPM);

        // ST consumes from DA
        validSt.addProvider(validDa);
        validDa.addConsumer(validSt);

        // validate the topology
        final List<SupplyChainValidationFailure> errors =
            supplychainValidator.validateTopologyEntities(
                Stream.of(
                    vmMissingBoughtCommodity.build(),
                    validPM.build(),
                    validDC.build(),
                    validSt.build(),
                    validDa.build()));

        // two errors expected
        Assert.assertEquals(2, errors.size());

        // these are the commodities that are expected to be missing
        // notice that, in the case if the storage cluster commodity, only the key is missing
        // this is still a supply chain validation error
        final CommodityType[] expectedMissingTypes =
            { CommodityType.STORAGE_ACCESS, CommodityType.STORAGE_CLUSTER};

        // check the details of both errors: they should be "mandatory bought commodity not found" errors,
        // the provider should be the storage, and their types should be those mentioned in the
        // expectedMissingTypes array
        for (int i = 0; i < 2; ++i) {
            Assert.assertTrue(errors.get(i) instanceof MandatoryCommodityBoughtNotFoundFailure);
            final MandatoryCommodityBoughtNotFoundFailure commodityNotFoundException =
                (MandatoryCommodityBoughtNotFoundFailure)errors.get(i);
            Assert.assertEquals(
                expectedMissingTypes[i], commodityNotFoundException.getCommodity().getCommodityType());
            Assert.assertEquals(validSt.getOid(), commodityNotFoundException.getProvider().getOid());
        }
    }

    // yet another prototype, for the disjunctive specification test
    private final TopologyEntityDTO.Builder lpPrototype =
        TopologyEntityDTO.newBuilder().setOid(lpOid).setDisplayName("Logical pool entity").
        setEntityType(EntityType.LOGICAL_POOL_VALUE);

    /**
     * Helper method for disjunction specification tests.
     *
     * The disjunction specification used for these tests introduces three types of entities:
     * storage (ST), diskarray (DA), and logical pool (LP).  The specification is as follows:
     * - a ST must have exactly one DA provider and buy commodity STORAGE_ACCESS from it AND
     * - At least one of the following is true:
     *   - a ST has exactly one LP provider and buys commodity EXTENT from it OR
     *   - a ST has exactly one DA provider and buys commodity EXTENT from it
     * (see {@link SupplyChainTestUtils#supplyChainWithDisjunction})
     *
     * The testing topology has one ST, one DA, one LP.  The DA and the LP are providers for ST.
     * Define the following conditions
     * - storageAccessBought = (ST buys STORAGE_ACCESS from DA)
     * - extentBoughtFromLP = (ST buys EXTENT from LP)
     * - extentBoughtFromDA = (ST buys EXTENT from DA)
     * Our specification can be written storageAccessBought && (extentBoughtFromLP || extentBoughtFromDA)
     *
     * This helper method takes values for storageAccessBought, extentBoughtFromLP, extentBoughtFromDA as
     * parameters, constructs the topology, and adds the corresponding commodities.
     *
     * @param storageAccessBought true if and only if ST buys STORAGE_ACCESS from DA.
     * @param extentBoughtFromLP true if and only if ST buys EXTENT from LP.
     * @param extentBoughtFromDA true if and only if ST buys EXTENT from DA.
     * @return list of errors produced by validation.
     */
    public List<SupplyChainValidationFailure> createAndValidateTopology(
            boolean storageAccessBought, boolean extentBoughtFromLP, boolean extentBoughtFromDA) {
        // sold commodities (by DA and LP)
        final Collection<CommoditySoldDTO> commoditiesSoldByDA =
            Arrays.asList(
                createCommoditySoldDTO(CommodityType.STORAGE_ACCESS_VALUE, null),
                createCommoditySoldDTO(CommodityType.EXTENT_VALUE, null));
        final Collection<CommoditySoldDTO> commoditiesSoldByLP =
            Collections.singleton(createCommoditySoldDTO(CommodityType.EXTENT_VALUE, null));

        // bought commodities (by ST)
        final Collection<CommodityBoughtDTO> commoditiesBoughtbySTfromDA = new ArrayList<>();
        if (storageAccessBought) {
            commoditiesBoughtbySTfromDA.add(
                createCommodityBoughtDTO(CommodityType.STORAGE_ACCESS_VALUE, null));
        }
        if (extentBoughtFromDA) {
            commoditiesBoughtbySTfromDA.add(
                createCommodityBoughtDTO(CommodityType.EXTENT_VALUE, null));
        }
        final Collection<CommodityBoughtDTO> commoditiesBoughtbySTfromLP = new ArrayList<>();
        if (extentBoughtFromLP) {
            commoditiesBoughtbySTfromLP.add(
                createCommodityBoughtDTO(CommodityType.EXTENT_VALUE, null));
        }
        final CommoditiesBoughtFromProvider stFromDA =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(daOid).
            setProviderEntityType(EntityType.DISK_ARRAY_VALUE).
            addAllCommodityBought(commoditiesBoughtbySTfromDA).build();
        final CommoditiesBoughtFromProvider lpFromDA =
            CommoditiesBoughtFromProvider.newBuilder().setProviderId(lpOid).
            setProviderEntityType(EntityType.LOGICAL_POOL_VALUE).
            addAllCommodityBought(commoditiesBoughtbySTfromLP).build();

        // entities
        final TopologyEntity.Builder storage =
            TopologyEntity.newBuilder(buildEntity(
                stPrototype, Collections.singleton(DISJUNCTION_TARGET_ID),
                Collections.emptyList(), Arrays.asList(stFromDA, lpFromDA)));
        final TopologyEntity.Builder diskarray =
            TopologyEntity.newBuilder(buildEntity(
                daPrototype, Collections.singleton(DISJUNCTION_TARGET_ID),
                commoditiesSoldByDA, Collections.emptyList()));
        final TopologyEntity.Builder logicalPool =
            TopologyEntity.newBuilder(buildEntity(
                lpPrototype, Collections.singleton(DISJUNCTION_TARGET_ID),
                commoditiesSoldByLP, Collections.emptyList()));

        // ST consumes from DA
        storage.addProvider(diskarray);
        diskarray.addConsumer(storage);

        // ST consumes from LP
        storage.addProvider(logicalPool);
        logicalPool.addConsumer(storage);

        // validate the topology and return all errors
        return
            supplychainValidator.validateTopologyEntities(
                Stream.of(storage.build(), diskarray.build(), logicalPool.build()));
    }

    /**
     * This helper method takes a list of validation errors and looks for three specific
     * "missing mandatory bought commodity" errors:
     * - storage does not buy STORAGE_ACCESS from diskarray
     * - storage does not buy EXTENT by logical pool
     * - and storage does not buy EXTENT by diskarray
     * The boolean parameters of this method inform it which of these error messages is expected.
     *
     * If the list contains any validation error other than the above, then the method fails.
     *
     * @param errors the list of validation errors.
     * @param expectStorageAccessCommodityMissing true if and only if the list is expected to contain the
     *                                            error "STORAGE_ACCESS bought commodity is missing".
     * @param expectExtentByLPMissing true if and only if the list is expected to contain the error "EXTENT
     *                                bought from logical pool is missing".
     * @param expectExtentByDAMissing true if and only if the list is expected to contain the error "EXTENT
     *                                bought from diskarray is missing".
     */
    public void checkExpectedErrors(
            @Nonnull List<SupplyChainValidationFailure> errors,
            boolean expectStorageAccessCommodityMissing,
            boolean expectExtentByLPMissing,
            boolean expectExtentByDAMissing) {
        boolean foundStorageAccessCommodityMissingError = false;
        boolean foundExtentCommodityFromLPMissingError = false;
        boolean foundExtentCommodityFromDAMissingError = false;
        for (int i = 0; i < errors.size(); i++) {
            Assert.assertTrue(errors.get(i) instanceof MandatoryCommodityBoughtNotFoundFailure);
            final MandatoryCommodityBoughtNotFoundFailure failure =
                (MandatoryCommodityBoughtNotFoundFailure)errors.get(i);
            switch (failure.getProvider().getEntityType()) {
                case EntityType.DISK_ARRAY_VALUE:
                    switch (failure.getCommodity().getCommodityType()) {
                        case STORAGE_ACCESS:
                            foundStorageAccessCommodityMissingError = true;
                            break;
                        case EXTENT:
                            foundExtentCommodityFromDAMissingError = true;
                            break;
                        default:
                            Assert.fail();
                    }
                    break;
                case EntityType.LOGICAL_POOL_VALUE:
                    Assert.assertEquals(CommodityType.EXTENT, failure.getCommodity().getCommodityType());
                    foundExtentCommodityFromLPMissingError = true;
                    break;
                default:
                    Assert.fail();
            }
        }
        Assert.assertEquals(expectStorageAccessCommodityMissing, foundStorageAccessCommodityMissingError);
        Assert.assertEquals(expectExtentByLPMissing, foundExtentCommodityFromLPMissingError);
        Assert.assertEquals(expectExtentByDAMissing, foundExtentCommodityFromDAMissingError);
    }

    /**
     * Test the case that all mandatory bought commodities are missing.
     */
    @Test
    public void testDisjunctionNoCommodities() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(false, false, false);

        // all mandatory commodities bought by storage are missing: we are expecting three errors
        checkExpectedErrors(errors, true, true, true);
    }

    /**
     * Test the case that storage buys EXTENT from diskarray, but nothing else.
     */
    @Test
    public void testDisjunctionExtentBoughtFromDA() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(false, false, true);

        // disjunctive specification is satisfied.  only "STORAGE_ACCESS is missing" error is expected
        checkExpectedErrors(errors, true, false, false);
    }

    /**
     * Test the case that storage buys EXTENT from logical pool, but nothing else.
     */
    @Test
    public void testDisjunctionExtentBoughtFromLP() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(false, true, false);

        // disjunctive specification is satisfied.  only "STORAGE_ACCESS is missing" error is expected
        checkExpectedErrors(errors, true, false, false);
    }

    /**
     * Test the case that storage buys STORAGE_ACCESS and EXTENT from diskarray, but nothing else.
     */
    @Test
    public void testDisjunctionStorageAccessBoughtAndLPBoughtFromDA() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(false, true, true);

        // disjunctive specification is satisfied.  only "STORAGE_ACCESS is missing" error is expected
        checkExpectedErrors(errors, true, false, false);
    }

    /**
     * Test the case that storage buys STORAGE_ACCESS, but nothing else.
     */
    @Test
    public void testDisjunctionOnlyStorageAccessBought() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(true, false, false);

        // disjunctive specification is not satisfied.  two errors should be returned, one per disjunct
        checkExpectedErrors(errors, false, true, true);
    }

    /**
     * Test the case that storage buys EXTENT and STORAGE_ACCESS from diskarray.
     */
    @Test
    public void testDisjunctionStorageAccessBoughtAndExtentBoughtFromDA() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(true, false, true);

        // all specifications are satisfied
        checkExpectedErrors(errors, false, false, false);
    }

    /**
     * Test the case that storage buys EXTENT from logical pool and STORAGE_ACCESS from diskarray.
     */
    @Test
    public void testDisjunctionStorageAccessBoughtAndExtentBoughtFromLP() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(true, true, false);

        // all specifications are satisfied
        checkExpectedErrors(errors, false, false, false);
    }

    /**
     * Test the case that storage buys all mandatory commodities
     */
    @Test
    public void testDisjunctionLPBoughtFromDAandLP() {
        final List<SupplyChainValidationFailure> errors =
            createAndValidateTopology(true, true, true);

        // all specifications are satisfied
        checkExpectedErrors(errors, false, false, false);
    }

    private CommoditySoldDTO createCommoditySoldDTO(int commodityTypeValue, String key) {
        final TopologyDTO.CommodityType.Builder commType = TopologyDTO.CommodityType.newBuilder();
        if (key != null) {
            commType.setKey(key);
        }
        final long CAP = 200L;
        return CommoditySoldDTO.newBuilder().setCommodityType(commType.setType(commodityTypeValue)).setCapacity(CAP)
                .setUsed(USED).setPeak(PEAK).build();
    }

    private CommodityBoughtDTO createCommodityBoughtDTO(int commodityTypeValue, String key) {
        final TopologyDTO.CommodityType.Builder commType = TopologyDTO.CommodityType.newBuilder();
        if (key != null) {
            commType.setKey(key);
        }
        return CommodityBoughtDTO.newBuilder().setCommodityType(commType.setType(commodityTypeValue))
                .setUsed(USED).setPeak(PEAK).build();
    }

    private TopologyEntityDTO.Builder buildEntity(
            TopologyEntityDTO.Builder prototype, Collection<Long> targetIds,
            Collection<? extends CommoditySoldDTO> commoditySoldDTOs,
            Collection<? extends CommoditiesBoughtFromProvider> commodityBoughtDTOs) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        targetIds.forEach(id -> origin
                        .putDiscoveredTargetData(id,
                                                 PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityDTO.newBuilder(prototype.build())
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(origin))
                .addAllCommoditySoldList(commoditySoldDTOs).addAllCommoditiesBoughtFromProviders(commodityBoughtDTOs);
    }
}
