package com.vmturbo.topology.processor.topology;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;


/**
 * The ApplicationCommodityKeyChanger is created specifically to overcome the bug OM-39541.
 * This bug is happening if there are vms that are replicated in the customer environment (which means
 * that they will have the same uuid, but they are in separate targets).
 * Because of that, and because of how the probe is assigning the commodities key, each guestload app
 * in the replica vms will buy an ApplicationCommodity from the respective vm.
 * Today the commodity key is in the form: VirtualMachine::423f26a7-1132-df1c-64e2-8a040480b046
 * where the last part is the vm uuid.
 * Given that the replica vms have the same uuid, this also means that the guestload applications
 * that they have on top can buy from one vm or the other with no constraints, because the key is the same.
 * This is only an artifact of the probe key assignment, and the replica effect.
 * When we assign OIDs to those vms and applications, the assignment is correct, and each one will get its
 * own separate oid (so the guestload app is buying from the correct vm).
 * The problem is that the ApplicationCommodity key is not changed, so the guestload can potentially buy
 * also from the vm that he is not connected with. This can manifest itself because the scoping algorithm
 * can bring unexpected vms and hosts into the scoped topology.
 * In order to fix this issue, we are changing the commodity key of those vms and applications
 * to the vm oid, so that it's unique.
 * Note: for now we are doing this only for vms and applications, and only if the vm is selling
 * a single ApplicationCommodity. We are not covering the case when multiple of those are sold.
 */
public class ApplicationCommodityKeyChanger {

    private final Logger logger = LogManager.getLogger();

    private static final String EMPTY_STRING_KEY = "";

    /**
     * Executes the key change to all the replica vms.
     *
     * @param topologyGraph graph of the current discovered topology.
     * @return Number of modified commodity keys.
     */
    public int execute(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {

        final AtomicInteger numChanged = new AtomicInteger(0);
        // iterate over all the VMs and check which ones needs to be changed
        topologyGraph.entitiesOfType(EntityType.VIRTUAL_MACHINE).forEach(vm -> {

            // get the list of ApplicationCommodity sold
            List<Builder> appCommoditySoldList = vm.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.APPLICATION_VALUE)
                .collect(Collectors.toList());

            // for now we are doing this only for vms and applications, and only if the vm is selling
            // a single ApplicationCommodity
            if (appCommoditySoldList.size() == 1) {

                TopologyDTO.CommodityType.Builder appComm = appCommoditySoldList.get(0).getCommodityTypeBuilder();
                // remember the old key for later, when we need to scan the apps
                String oldAppCommKey = appComm.getKey();

                // if the key is null or empty, don't change it because it's not representing a
                // constraint for the consumer
                if (StringUtils.isNotEmpty(oldAppCommKey)) {

                    // change the key to something that is unique, and cannot clash with the
                    // same app commodity in the replicated vm
                    // we are using the vm oid, because it's guaranteed to be unique
                    String newCommKey = Long.toString(vm.getOid());
                    numChanged.incrementAndGet();
                    appComm.setKey(newCommKey);

                    // change app key on apps that are consuming from the vm
                    topologyGraph.getConsumers(vm)
                        .filter(consumer -> consumer.getEntityType() == EntityType.APPLICATION_VALUE ||
                                consumer.getEntityType() == EntityType.APPLICATION_SERVER_VALUE ||
                                consumer.getEntityType() == EntityType.DATABASE_SERVER_VALUE)
                        .forEach(app -> {
                            // find the commodity set that is buying from that same vm
                            CommoditiesBoughtFromProvider.Builder commBoughtFromProv = app.getTopologyEntityDtoBuilder()
                                .getCommoditiesBoughtFromProvidersBuilderList()
                                .stream()
                                .filter(commFromProvider -> commFromProvider.getProviderId() == vm.getOid())
                                .findFirst().get();

                            // find Application commodity from this set and change key to new one
                            commBoughtFromProv.getCommodityBoughtBuilderList().stream()
                                .filter(commodity -> commodity.getCommodityType().getType() ==
                                    CommodityType.APPLICATION_VALUE)
                                .forEach(comm -> comm.getCommodityTypeBuilder().setKey(newCommKey));
                        });
                }

            } else if (appCommoditySoldList.size() > 1) {
                // TODO support key change when vms selling more than 1 app commodity
                // in this case there is more than 1 app commodity sold by the vm
                // it's not clear at this point which one we need to change, and which one to keep
                // so we are not supporting this for now
                // We need to check this logic again when we will implement APM/ACM probes in XL
                logger.warn("Unable to change ApplicationCommodity key because VM OID:{} " +
                    "displayName:{} is selling more than one of those", vm.getOid(), vm.getDisplayName());
            }
        });

        return numChanged.get();
    }

}
