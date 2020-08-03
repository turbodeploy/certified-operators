package com.vmturbo.stitching.vdi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessUserData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A Stitching operation that stitches the VDI discovered VM with the underlying VCenter VM.
 */
public class VDIVMStitchingOperation extends VDIStitchingOperation {
    private static final Logger logger = LogManager.getLogger();
    private VDIVMStitchingOperation keepAllocationCommoditiesStitchingOperation;

    /**
     * Create an instance of {@link VDIVMStitchingOperation} class.
     */
    public VDIVMStitchingOperation() {
        this(EntityType.VIRTUAL_MACHINE,
              Collections.singleton(CommodityType.ACTIVE_SESSIONS),
              Arrays.asList(
                              CommodityBoughtMetadata.newBuilder()
                                              .setProviderType(EntityType.DESKTOP_POOL)
                                              .setReplacesProvider(EntityType.VIRTUAL_DATACENTER)
                                              .addCommodityMetadata(CommodityType.CPU_ALLOCATION)
                                              .addCommodityMetadata(CommodityType.MEM_ALLOCATION)
                                              .build(),
                              CommodityBoughtMetadata.newBuilder()
                                              .setProviderType(EntityType.STORAGE)
                                              .addCommodityMetadata(CommodityType.STORAGE_CLUSTER)
                                              .build(),
                              CommodityBoughtMetadata.newBuilder()
                                              .setProviderType(EntityType.PHYSICAL_MACHINE)
                                              .addCommodityMetadata(CommodityType.CLUSTER)
                                              .addCommodityMetadata(CommodityType.VMPM_ACCESS)
                                              .build()
              ));
    }

    private VDIVMStitchingOperation(EntityType entityType, Set<CommodityType> soldCommodityTypes,
                                         List<CommodityBoughtMetadata> boughtMetaDataList) {
        super(entityType, soldCommodityTypes, boughtMetaDataList);
    }

    @Override
    protected void stitch(@Nonnull StitchingEntity internalEntity,
            @Nonnull StitchingEntity externalEntity,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        if (shouldStitchAllocationCommodities(internalEntity, externalEntity)) {
            doStitch(internalEntity, externalEntity, resultBuilder);
        } else {
            // Special case when VM is not in default resource pool of VM's desktop pool.
            if (keepAllocationCommoditiesStitchingOperation == null) {
                keepAllocationCommoditiesStitchingOperation = new VDIVMStitchingOperation(
                                EntityType.VIRTUAL_MACHINE,
                                Collections.singleton(CommodityType.ACTIVE_SESSIONS),
                                Arrays.asList(
                                                CommodityBoughtMetadata.newBuilder()
                                                                .setProviderType(EntityType.DESKTOP_POOL)
                                                                .addCommodityMetadata(CommodityType.CPU_ALLOCATION)
                                                                .addCommodityMetadata(CommodityType.MEM_ALLOCATION)
                                                                .build(),
                                                CommodityBoughtMetadata.newBuilder()
                                                              .setProviderType(EntityType.STORAGE)
                                                              .addCommodityMetadata(CommodityType.STORAGE_CLUSTER)
                                                              .build(),
                                              CommodityBoughtMetadata.newBuilder()
                                                              .setProviderType(EntityType.PHYSICAL_MACHINE)
                                                              .addCommodityMetadata(CommodityType.CLUSTER)
                                                              .build()));
            }
            keepAllocationCommoditiesStitchingOperation.doStitch(internalEntity, externalEntity, resultBuilder);
        }
    }

    private void doStitch(@Nonnull StitchingEntity internalEntity,
                          @Nonnull StitchingEntity externalEntity,
                          @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        super.stitch(internalEntity, externalEntity, resultBuilder);
        // In addition to the standard stitching, this will also resolve the VM OID
        // for the session data in the business users consumed by this VM.
        resolveVMCloneSourceReference(internalEntity, externalEntity, resultBuilder);
    }

    private void resolveVMCloneSourceReference(final StitchingEntity vdiVMEntity,
            final StitchingEntity vcVMEntity,
            final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        for (StitchingEntity consumerEntity : vdiVMEntity.getConsumers()) {
            if (consumerEntity.getEntityType() == EntityType.BUSINESS_USER &&
                    consumerEntity.getEntityBuilder().hasBusinessUserData()) {
                resultBuilder.queueUpdateEntityAlone(consumerEntity, updateSessionDataVMId -> {
                    final BusinessUserData.Builder buDataBuilder = consumerEntity.getEntityBuilder()
                            .getBusinessUserData().toBuilder();
                    final List<SessionData> newSessionList = new ArrayList<>();
                    boolean vmSessionFound = false;
                    for (SessionData sessionData : buDataBuilder.getSessionDataList()) {
                        if (sessionData.hasVirtualMachine()) {
                            if (sessionData.getVirtualMachine().equals(vdiVMEntity.getLocalId())) {
                                newSessionList.add(sessionData.toBuilder()
                                .setVirtualMachine(String.valueOf(vcVMEntity.getOid()))
                                .build());
                                vmSessionFound = true;
                            } else {
                                newSessionList.add(sessionData);
                            }
                        } else {
                            logger.error(" No Virtual machine data in session data for" +
                                            " consumer {} of VM {}. Session data  retained as is" +
                                            " for the VM oid resolution.",
                                    consumerEntity.getOid(), vcVMEntity.getOid());
                            newSessionList.add(sessionData);
                        }
                    }
                    if (!vmSessionFound) {
                        logger.error(" No session data was found for" + " consumer {} of VM {}" +
                                        " even though the BU is consuming from this VM.",
                                consumerEntity.getOid(), vcVMEntity.getOid());
                    }
                    buDataBuilder.clearSessionData();
                    buDataBuilder.addAllSessionData(newSessionList);
                    consumerEntity.getEntityBuilder().setBusinessUserData(buDataBuilder.build());
                });
            }
        }
    }

    private boolean shouldStitchAllocationCommodities(@Nonnull StitchingEntity vdiVMEntity,
                                                      @Nonnull StitchingEntity vcVMEntity) {
        final Optional<StitchingEntity> vdcOfVM =
                        getFirstProviderByType(vcVMEntity, EntityType.VIRTUAL_DATACENTER);
        if (!vdcOfVM.isPresent()) {
            logger.warn("There is no VDC that sells to VM {}. Assume stitch is needed", vcVMEntity.getOid());
            return true;
        }

        final Optional<StitchingEntity> dpOfVM =
                        getFirstProviderByType(vdiVMEntity, EntityType.DESKTOP_POOL);
        if (!dpOfVM.isPresent()) {
            logger.warn("VM {} has no desktopPool. Assume stitch is needed.", vdiVMEntity.getOid());
            return true;
        }

        final Optional<StitchingEntity> vdcOfDP =
                        getFirstProviderByType(dpOfVM.get(), EntityType.VIRTUAL_DATACENTER);
        if (!vdcOfDP.isPresent()) {
            logger.warn("DesktopPool {} has no VDC. Assume stitch is needed.", dpOfVM.get().getOid());
            return true;
        }

        return getInternalName(vdcOfDP.get()).equals(getInternalName(vdcOfVM.get()));
    }

    private static Optional<StitchingEntity> getFirstProviderByType(@Nonnull StitchingEntity from,
                                                                    @Nonnull EntityType entityType) {
        return from.getCommodityBoughtListByProvider().keySet().stream()
                        .filter(entity -> entity.getEntityBuilder().getEntityType() == entityType)
                        .findFirst();
    }

    private static Optional<String> getInternalName(@Nonnull StitchingEntity vdc) {
        return vdc.getEntityBuilder().getEntityPropertiesList().stream()
                        .filter(prop -> SupplyChainConstants.INTERNAL_NAME_TGT_ID
                                        .equals(prop.getName()))
                        .map(CommonDTO.EntityDTO.EntityProperty::getValue).findFirst();
    }
}
