package com.vmturbo.stitching.vdi;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterData;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * Stitching Operation to resolve the vm reference id in the desktop pool. The probe sends in the master image
 * as the uuid of underlying VC Virtual Machine and this needs to be resolved to the OID
 * generated by the Topology Processor. In addition, we will be adding the master image source
 * property to entity properties to identify if the UID was a VM.
 */
public class DesktopPoolMasterImageStitchingOperation implements StitchingOperation<String, String> {


    private static final String MASTER_IMAGE_SOURCE = "masterImageSource";
    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>>
    getScope(@Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return Optional.of(
                stitchingScopeFactory.probeEntityTypeScope(SDKProbeType.VCENTER.getProbeType(),
                        EntityType.VIRTUAL_MACHINE));
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.DESKTOP_POOL;
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.VIRTUAL_MACHINE);
    }

    @Override
    public Optional<String> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        if (!internalEntity.getEntityBuilder().hasVirtualDatacenterData() ||
                !internalEntity.getEntityBuilder().getVirtualDatacenterData().hasDesktopPoolData()
                || !internalEntity.getEntityBuilder().getVirtualDatacenterData()
                .getDesktopPoolData().hasMasterImage()) {
            return Optional.empty();
        }
        return Optional.of(internalEntity.getEntityBuilder()
                .getVirtualDatacenterData().getDesktopPoolData().getMasterImage());
    }

    @Override
    public Optional<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Optional.ofNullable(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {

        for (StitchingPoint stitchingPoint : stitchingPoints) {
            if (stitchingPoint.getExternalMatches().size() == 0) {
                logger.error("No external matches found for internal entity {}",
                        stitchingPoint.getInternalEntity().getEntityBuilder().toString());
                continue;
            }
            if (stitchingPoint.getExternalMatches().size() > 1) {
                logger.warn("Encountered more than one VM with the same uuid for stitching point" +
                        " and will continuing stitching with the first match. ",
                        stitchingPoint.getInternalEntity().getEntityBuilder().toString());
            }
            final Optional<? extends StitchingEntity> vmEntity = stitchingPoint.getExternalMatches()
                    .stream().findFirst();
            if (!vmEntity.isPresent()) {
                return resultBuilder.build();
            }
            resultBuilder.queueUpdateEntityAlone(stitchingPoint.getInternalEntity(),
                    entityToUpdate -> updateMasterImage(entityToUpdate, vmEntity.get()));
        }
        return resultBuilder.build();

    }

    private void updateMasterImage(final StitchingEntity entityToUpdate,
                                   final StitchingEntity vmEntity) {
        final Builder dpBuilder = entityToUpdate.getEntityBuilder();
        VirtualDatacenterData.Builder vdcDataBuilder = dpBuilder.getVirtualDatacenterDataBuilder();
        DesktopPoolData.Builder dpPoolDataBuilder = vdcDataBuilder.getDesktopPoolDataBuilder();
        dpPoolDataBuilder.setMasterImage(String.valueOf(vmEntity.getOid())).build();
        vdcDataBuilder.setDesktopPoolData(dpPoolDataBuilder.build());
        dpBuilder.addEntityProperties(EntityProperty.newBuilder()
                .setName(MASTER_IMAGE_SOURCE)
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setValue(EntityType.VIRTUAL_MACHINE.name()).build())
                .setVirtualDatacenterData(vdcDataBuilder.build())
                .build();
    }
}
