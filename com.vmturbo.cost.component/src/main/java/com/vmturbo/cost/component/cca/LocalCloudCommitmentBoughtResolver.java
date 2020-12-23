package com.vmturbo.cost.component.cca;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitment;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

/**
 * A local class implementing the CloudCommitmentBoughtResolver. Retrieves all the ReservedInstanceBought
 * objects from the ReservedInstanceBoughtStore.
 */
public class LocalCloudCommitmentBoughtResolver implements CloudCommitmentBoughtResolver {

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    /**
     * Constructs the LocalCloudCommitmentBoughtResolver.
     *
     * @param reservedInstanceBoughtStore The ReservedInstanceBoughtStore we query to get the reservedInstanceBought.
     * @param reservedInstanceSpecStore The ReservedInstanceSpecStore we query to get the RI specs.
     */
    public LocalCloudCommitmentBoughtResolver(@Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore) {
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
    }

    @Override
    public List<CloudCommitmentData> getCloudCommitment(CloudCommitmentInventory cloudCommitmentList) {

        final Set<Long> reservedInstanceOids = new HashSet<>();
        for (CloudCommitment cloudCommitment: cloudCommitmentList.getCloudCommitmentList()) {
            if (cloudCommitment.getType().equals(CloudCommitmentType.RESERVED_INSTANCE)) {
                reservedInstanceOids.add(cloudCommitment.getOid());
            } else {
                throw new UnsupportedOperationException("A cloud commitment of type other than an Reserved Instance was found in the request");
            }
        }

        if (!reservedInstanceOids.isEmpty()) {
            // Get the list of ReservedInstanceBought
            List<ReservedInstanceBought> reservedInstanceBoughtList = reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                    ReservedInstanceBoughtFilter.newBuilder()
                            .riBoughtFilter(Cost.ReservedInstanceBoughtFilter.newBuilder()
                                    .addAllRiBoughtId(reservedInstanceOids)
                                    .build())
                            .build());

            final Set<Long> reservedInstanceIds = reservedInstanceBoughtList.stream()
                    .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                    .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                    .collect(Collectors.toSet());

            Map<Long, ReservedInstanceSpec> reservedInstanceSpecById =
                    reservedInstanceSpecStore.getReservedInstanceSpecByIds(reservedInstanceIds)
                            .stream()
                            .collect(Collectors.toMap(
                                    ReservedInstanceSpec::getId,
                                    Function.identity()));

            List<CloudCommitmentData> riDataList = new ArrayList<>();
            for (ReservedInstanceBought riBought : reservedInstanceBoughtList) {
                riDataList.add(ReservedInstanceData.builder()
                        .commitment(riBought)
                        .spec(reservedInstanceSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                        .build());
            }
            return riDataList;
        } else {
            return Collections.EMPTY_LIST;
        }
    }
}
