package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.enums.AspectName;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * EntityAspectMapper test.
 */
public class EntityAspectMapperTest {
    private final StorageTierAspectMapper storageTierAspectMapper = mock(StorageTierAspectMapper.class);
    private final VirtualVolumeAspectMapper virtualVolumeAspectMapper = mock(VirtualVolumeAspectMapper.class);
    private final CloudAspectMapper cloudAspectMapper = mock(CloudAspectMapper.class);
    private final VirtualMachineAspectMapper virtualMachineMapper = mock(VirtualMachineAspectMapper.class);
    private final DesktopPoolAspectMapper desktopPoolAspectMapper = mock(DesktopPoolAspectMapper.class);
    private final MasterImageEntityAspectMapper masterImageEntityAspectMapper = mock(MasterImageEntityAspectMapper.class);
    private final PhysicalMachineAspectMapper physicalMachineAspectMapper = mock(PhysicalMachineAspectMapper.class);
    private final StorageAspectMapper storageAspectMapper = mock(StorageAspectMapper.class);
    private final DiskArrayAspectMapper diskArrayAspectMapper = mock(DiskArrayAspectMapper.class);
    private final LogicalPoolAspectMapper logicalPoolAspectMapper = mock(LogicalPoolAspectMapper.class);
    private final StorageControllerAspectMapper storageControllerAspectMapper = mock(StorageControllerAspectMapper.class);
    private final PortsAspectMapper portsAspectMapper = mock(PortsAspectMapper.class);
    private final DatabaseAspectMapper databaseAspectMapper = mock(DatabaseAspectMapper.class);
    private final DatabaseServerAspectMapper databaseServerAspectMapper = mock(DatabaseServerAspectMapper.class);
    private final RegionAspectMapper regionAspectMapper = mock(RegionAspectMapper.class);
    private final WorkloadControllerAspectMapper workloadControllerAspectMapper = mock(WorkloadControllerAspectMapper.class);
    private final ComputeTierAspectMapper computeTierAspectMapper = mock(ComputeTierAspectMapper.class);
    private final DatabaseServerTierAspectMapper databaseServerTierAspectMapper = mock(DatabaseServerTierAspectMapper.class);
    private final DatabaseTierAspectMapper databaseTierAspectMapper = mock(DatabaseTierAspectMapper.class);
    private final BusinessUserAspectMapper businessUserAspectMapper = mock(BusinessUserAspectMapper.class);
    private final VirtualVolumeEntityAspectMapper virtualVolumeEntityAspecMapper = mock(VirtualVolumeEntityAspectMapper.class);
    private final CloudCommitmentAspectMapper cloudCommitmentAspectMapper = mock(CloudCommitmentAspectMapper.class);
    private final ContainerPlatformContextAspectMapper containerPlatformContextAspectMapper = mock(ContainerPlatformContextAspectMapper.class);
    private final ApplicationServiceAspectMapper appSvcAspectMapper = mock(ApplicationServiceAspectMapper.class);
    private final CloudApplicationAspectMapper cloudApplicationAspectMapper = mock(
            CloudApplicationAspectMapper.class);
    final long realtimeTopologyContextId = 99L;

    private final EntityAspectMapper mapper = new EntityAspectMapper(storageTierAspectMapper, virtualVolumeAspectMapper,
            cloudAspectMapper, virtualMachineMapper, desktopPoolAspectMapper, masterImageEntityAspectMapper,
            physicalMachineAspectMapper, storageAspectMapper, diskArrayAspectMapper, logicalPoolAspectMapper,
            storageControllerAspectMapper, portsAspectMapper, databaseAspectMapper, databaseServerAspectMapper,
            regionAspectMapper, workloadControllerAspectMapper, computeTierAspectMapper, databaseServerTierAspectMapper,
            databaseTierAspectMapper, businessUserAspectMapper, virtualVolumeEntityAspecMapper, cloudCommitmentAspectMapper,
            containerPlatformContextAspectMapper, appSvcAspectMapper, cloudApplicationAspectMapper,
            realtimeTopologyContextId);

    /**
     * Setup mocks.
     */
    @Before
    public void setup() {
        when(storageTierAspectMapper.getAspectName()).thenReturn(AspectName.STORAGE_TIER);
        when(virtualVolumeAspectMapper.getAspectName()).thenReturn(AspectName.VIRTUAL_VOLUME);
        when(cloudAspectMapper.getAspectName()).thenReturn(AspectName.CLOUD);
        when(virtualMachineMapper.getAspectName()).thenReturn(AspectName.VIRTUAL_MACHINE);
        when(desktopPoolAspectMapper.getAspectName()).thenReturn(AspectName.DESKTOP_POOL);
        when(masterImageEntityAspectMapper.getAspectName()).thenReturn(AspectName.MASTER_IMAGE);
        when(physicalMachineAspectMapper.getAspectName()).thenReturn(AspectName.PHYSICAL_MACHINE);
        when(storageAspectMapper.getAspectName()).thenReturn(AspectName.STORAGE);
        when(diskArrayAspectMapper.getAspectName()).thenReturn(AspectName.DISK_ARRAY);
        when(logicalPoolAspectMapper.getAspectName()).thenReturn(AspectName.LOGICAL_POOL);
        when(storageControllerAspectMapper.getAspectName()).thenReturn(AspectName.STORAGE_CONTROLLER);
        when(portsAspectMapper.getAspectName()).thenReturn(AspectName.PORTS);
        when(databaseAspectMapper.getAspectName()).thenReturn(AspectName.DATABASE);
        when(databaseServerAspectMapper.getAspectName()).thenReturn(AspectName.DATABASE_SERVER);
        when(regionAspectMapper.getAspectName()).thenReturn(AspectName.REGION);
        when(workloadControllerAspectMapper.getAspectName()).thenReturn(AspectName.WORKLOAD_CONTROLLER);
        when(computeTierAspectMapper.getAspectName()).thenReturn(AspectName.COMPUTE_TIER);
        when(databaseServerTierAspectMapper.getAspectName()).thenReturn(AspectName.DATABASE_SERVER);
        when(databaseTierAspectMapper.getAspectName()).thenReturn(AspectName.DATABASE_TIER);
        when(businessUserAspectMapper.getAspectName()).thenReturn(AspectName.BUSINESS_USER);
        when(virtualVolumeEntityAspecMapper.getAspectName()).thenReturn(AspectName.VIRTUAL_VOLUME_ENTITY);
        when(cloudCommitmentAspectMapper.getAspectName()).thenReturn(AspectName.CLOUD_COMMITMENT);
        when(containerPlatformContextAspectMapper.getAspectName()).thenReturn(AspectName.CONTAINER_PLATFORM_CONTEXT);
        when(appSvcAspectMapper.getAspectName()).thenReturn(AspectName.APP_SERVICE);
        when(cloudApplicationAspectMapper.getAspectName()).thenReturn(AspectName.CLOUD_APPLICATION);

    }

    /**
     * test { @link EntityAspectMapper#getMappersAndEntityTypes }.
     */
    @Test
    public void testGetMappersAndEntityTypes() {
        Map<IAspectMapper, List<Integer>> result = mapper.getMappersAndEntityTypes(Collections.singletonList("cloudAspect"));
        Optional<IAspectMapper> key = result.keySet().stream().findFirst();
        assertTrue(key.isPresent());
        assertEquals(cloudAspectMapper, key.get());
        assertTrue(result.get(key.get()).contains(EntityType.APPLICATION_COMPONENT_VALUE));
        assertTrue(result.get(key.get()).contains(EntityType.VIRTUAL_MACHINE_SPEC_VALUE));

    }
}