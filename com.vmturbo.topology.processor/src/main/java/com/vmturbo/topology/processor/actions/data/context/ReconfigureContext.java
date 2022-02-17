package com.vmturbo.topology.processor.actions.data.context;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A class for collecting data needed for Reconfigure action execution.
 */
public class ReconfigureContext extends AbstractActionExecutionContext {

    protected ReconfigureContext(@Nonnull final ExecuteActionRequest request,
                            @Nonnull final ActionDataManager dataManager,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final EntityRetriever entityRetriever,
                            @Nonnull final TargetStore targetStore,
                            @Nonnull final ProbeStore probeStore,
                            @Nonnull final GroupAndPolicyRetriever groupAndPolicyRetriever,
                            @Nonnull final SecureStorageClient secureStorageClient)
            throws ContextCreationException {
        super(Objects.requireNonNull(request),
              Objects.requireNonNull(dataManager),
              Objects.requireNonNull(entityStore),
              Objects.requireNonNull(entityRetriever),
              targetStore,
              probeStore,
              groupAndPolicyRetriever,
              secureStorageClient);
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possibility of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getReconfigure().getTarget().getId();
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     * data. A builder is returned so that further modifications can be made by subclasses, before
     * the final build is done.
     * This implementation additionally sets commodity data needed for resize action execution.
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     * @throws ContextCreationException if exception faced while building action
     */
    @Override
    protected List<Builder> initActionItemBuilders() throws ContextCreationException {
        final List<ActionItemDTO.Builder> builders = super.initActionItemBuilders();
        final ActionItemDTO.Builder actionItemBuilder = getPrimaryActionItemBuilder(builders);
        final Reconfigure reconfigure = getActionInfo().getReconfigure();
        final Map<EntityAttribute, SettingChange> attributeToChange =
                        reconfigure.getSettingChangeList().stream()
                                        .collect(Collectors.toMap(SettingChange::getEntityAttribute,
                                                        sc -> sc));
        if (!attributeToChange.isEmpty()) {
            actionItemBuilder.setCommodityAttribute(CommodityAttribute.Capacity);
            final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
            if (fullEntityDTO.hasVirtualMachineData()) {
                final VirtualMachineData vmData = fullEntityDTO.getVirtualMachineData();
                final int sockets = getSockets(attributeToChange, vmData);
                final int cps = getCps(attributeToChange, vmData);
                actionItemBuilder.setNewComm(CommodityDTO.newBuilder().setCapacity(sockets * cps)
                                .setCommodityType(CommodityType.VCPU).build());
            }
        }
        return builders;
    }

    private static int getCps(Map<EntityAttribute, SettingChange> attributeToChange,
                    VirtualMachineData vmData) {
        final SettingChange change = attributeToChange.get(EntityAttribute.CORES_PER_SOCKET);
        if (change == null) {
            return vmData.getCoresPerSocketRatio();
        }
        return (int)change.getNewValue();
    }

    private static int getSockets(Map<EntityAttribute, SettingChange> attributeToChange,
                    VirtualMachineData vmData) {
        final SettingChange change = attributeToChange.get(EntityAttribute.SOCKET);
        if (change == null) {
            return (int)Math.ceil(vmData.getNumCpus() / (float)vmData.getCoresPerSocketRatio());
        }
        return (int)change.getNewValue();
    }

    @Override
    protected EntityDTO getFullEntityDTO(long entityId) throws ContextCreationException {
        final EntityDTO result = super.getFullEntityDTO(entityId);
        final Optional<SettingChange> cpsChange =
                        getActionInfo().getReconfigure().getSettingChangeList().stream()
                                        .filter(sc -> sc.getEntityAttribute()
                                                        == EntityAttribute.CORES_PER_SOCKET)
                                        .findAny();
        if (cpsChange.isPresent() && result.hasVirtualMachineData()) {
            final VirtualMachineData virtualMachineData = result.getVirtualMachineData();
            if (virtualMachineData.hasCoresPerSocketChangeable()) {
                final VirtualMachineData.Builder vmDataBuilder =
                                VirtualMachineData.newBuilder(virtualMachineData)
                                                .setCoresPerSocketRatio(
                                                                (int)cpsChange.get().getNewValue());
                return EntityDTO.newBuilder(result).setVirtualMachineData(vmDataBuilder).build();
            }
        }
        return result;
    }
}
