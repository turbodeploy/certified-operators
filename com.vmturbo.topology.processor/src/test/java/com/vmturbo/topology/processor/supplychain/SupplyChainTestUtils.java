package com.vmturbo.topology.processor.supplychain;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.logging.log4j.util.Strings;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.Provider;
import com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderOrSet;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderProp;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;

/**
 * This class introduces two simplified supply chain definitions.
 */
public class SupplyChainTestUtils {

    public static final String KEY = "KEY";

    /**
     * Retrieve the sample supply chain templates of a hypervisor probe.
     *
     * @return the sample supply chain of hypervisor.
     */
    public static Collection<TemplateDTO> hypervisorSupplyChain() {
        return HypervisorSupplyChain.getInstance();
    }

    /**
     * Retrieve the sample supply chain templates of a storage probe.
     *
     * @return the sample supply chain of storage.
     */
    public static Collection<TemplateDTO> storageSupplyChain() {
        return StorageSupplyChain.getInstance();
    }

    /**
     * Util method to retrieve the {@link TemplateDTO} of a {@link TopologyEntityDTO} from a supply chain
     * collection.
     *
     * @param templates collection of templates.
     * @param entity entity for which the {@link TemplateDTO} object must be retrieved.
     * @return {@link TemplateDTO} object retrieved.
     * @throws Exception the retrieval failed.
     */
    public static TemplateDTO retrieveEntityTemplate(
            Collection<TemplateDTO> templates, TopologyEntity entity) throws Exception {
        return templates.stream()
                .filter(templateDTO -> templateDTO.getTemplateClass().getNumber() == entity.getEntityType())
                .findAny()
                .orElseThrow(() -> new Exception("Retrieving supply chain template failed."));
    }

    private static TemplateDTO createEntityTemplate(
            EntityType entityType, TemplateType templateType, int priority,
            Collection<? extends TemplateCommodity> commoditySoldTemplates,
            Collection<? extends CommBoughtProviderProp> commodityBoughtTemplates,
            Collection<CommBoughtProviderOrSet> disjunctiveSpecifications) {
        return TemplateDTO.newBuilder()
                .setTemplateClass(entityType)
                .setTemplateType(templateType)
                .setTemplatePriority(priority)
                .addAllCommoditySold(commoditySoldTemplates)
                .addAllCommodityBought(commodityBoughtTemplates)
                .addAllCommBoughtOrSet(disjunctiveSpecifications)
                .build();
    }

    private static TemplateDTO createEntityTemplate(
            EntityType entityType, TemplateType templateType, int priority,
            Collection<? extends TemplateCommodity> commoditySoldTemplates,
            Collection<? extends CommBoughtProviderProp> commodityBoughtTemplates) {
        return
            createEntityTemplate(
                entityType, templateType, priority,
                commoditySoldTemplates, commodityBoughtTemplates, Collections.emptyList());
    }

    private static TemplateDTO createEntityTemplate(EntityType entityType,
             Collection<? extends TemplateCommodity> commoditySoldTemplates,
             Collection<? extends CommBoughtProviderProp> commodityBoughtTemplates) {
         return createEntityTemplate(entityType, TemplateType.BASE, 0, commoditySoldTemplates,
                 commodityBoughtTemplates);
    }

    private static TemplateCommodity createCommodityTemplate(CommodityType commodityType, String key) {
        return createCommodityTemplate(commodityType, key, false);
    }

    private static TemplateCommodity createCommodityTemplate(CommodityType commodityType,
            String key, boolean optional) {
        final TemplateCommodity.Builder builder = TemplateCommodity.newBuilder()
                .setCommodityType(commodityType)
                .setOptional(optional);
        if (!Strings.isEmpty(key)) {
            builder.setKey(key);
        }
        return builder.build();
    }

    private static CommBoughtProviderProp createCommBoughtProviderProp(Provider provider,
            Collection<TemplateCommodity> boughtCommodityTemplates, boolean optional) {
        return CommBoughtProviderProp.newBuilder()
                .setKey(provider)
                .addAllValue(boughtCommodityTemplates)
                .setIsOptional(optional)
                .build();
    }

    private static Provider createHostingProviderTemplate(EntityType entityType) {
        return createProviderTemplate(entityType, ProviderType.HOSTING, 1, 1);
    }

    private static Provider createLayeredOverProviderTemplate(EntityType entityType, int cardinalityMin,
            int cardinalityMax) {
        return createProviderTemplate(entityType, ProviderType.LAYERED_OVER, cardinalityMin, cardinalityMax);
    }

    private static Provider createProviderTemplate(EntityType entityType, ProviderType providerType, int cardinalityMin,
            int cardinalityMax) {
        return Provider.newBuilder()
                .setTemplateClass(entityType)
                .setProviderType(providerType)
                .setCardinalityMin(cardinalityMin)
                .setCardinalityMax(cardinalityMax)
                .build();
    }

    private static CommBoughtProviderOrSet createDisjunctiveSpecification(
            CommBoughtProviderProp ... disjuncts) {
        return CommBoughtProviderOrSet.newBuilder().addAllCommBought(Arrays.asList(disjuncts)).build();
    }

    /**
     * Util class to get a simple Hypervisor supply chain instance, which has virtual machine, physical
     * machine, virtual datacenter, storage, disk array and datacenter templates.
     */
    private static class HypervisorSupplyChain {
        private static Collection<TemplateDTO> instance = null;

        public static Collection<TemplateDTO> getInstance() {
            if (instance == null) {
                create();
            }
            return instance;
        }

        private static void create() {
            final TemplateDTO vm = createEntityTemplate(EntityType.VIRTUAL_MACHINE, getVMSoldCommodityTemplates(),
                    getVMBoughtCommodityTemplates());
            final TemplateDTO pm = createEntityTemplate(EntityType.PHYSICAL_MACHINE, getPMSoldCommodityTemplates(),
                    getPMBoughtCommodityTemplates());
            final TemplateDTO st = createEntityTemplate(EntityType.STORAGE, getStorageSoldCommodityTemplates(),
                    getStorageBoughtCommodityTemplates());
            final TemplateDTO dc = createEntityTemplate(EntityType.DATACENTER, getDCSoldCommodityTemplates(), Collections.emptyList());
            final TemplateDTO da = createEntityTemplate(EntityType.DISK_ARRAY, getDASoldCommodityTemplates(), Collections.emptyList());

            instance = Collections.unmodifiableCollection(Arrays.asList(vm, pm, st, dc, da));
        }

        private static Collection<TemplateCommodity> getVMSoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.VCPU, null),
                    createCommodityTemplate(CommodityType.VMEM, null),
                    createCommodityTemplate(CommodityType.APPLICATION, KEY),
                    createCommodityTemplate(CommodityType.MEM_ALLOCATION, null, true));
        }

        private static Collection<CommBoughtProviderProp> getVMBoughtCommodityTemplates() {
            return Arrays.asList(
                    createCommBoughtProviderProp(createHostingProviderTemplate(EntityType.PHYSICAL_MACHINE),
                            getPMSoldCommodityTemplates(), false),
                    createCommBoughtProviderProp(createLayeredOverProviderTemplate(EntityType.STORAGE, 1, Integer.MAX_VALUE),
                            getStorageSoldCommodityTemplates(), false));
        }

        private static Collection<TemplateCommodity> getPMSoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.CPU, null),
                    createCommodityTemplate(CommodityType.MEM, null),
                    createCommodityTemplate(CommodityType.BALLOONING, null),
                    createCommodityTemplate(CommodityType.IO_THROUGHPUT, null),
                    createCommodityTemplate(CommodityType.CLUSTER, KEY)
                    );
        }

        private static Collection<CommBoughtProviderProp> getPMBoughtCommodityTemplates() {
            return Arrays.asList(
                    createCommBoughtProviderProp(
                            createLayeredOverProviderTemplate(EntityType.STORAGE, 0, Integer.MAX_VALUE),
                            Arrays.asList(
                                    createCommodityTemplate(CommodityType.STORAGE_ACCESS, null),
                                    createCommodityTemplate(CommodityType.STORAGE_LATENCY, null)),
                            true),
                    createCommBoughtProviderProp(createHostingProviderTemplate(EntityType.DATACENTER),
                            getDCSoldCommodityTemplates(), false));
        }

        private static Collection<TemplateCommodity> getStorageSoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.STORAGE_ACCESS, null),
                    createCommodityTemplate(CommodityType.STORAGE_AMOUNT, null),
                    createCommodityTemplate(CommodityType.STORAGE_CLUSTER, KEY),
                    createCommodityTemplate(CommodityType.DSPM_ACCESS, KEY),
                    createCommodityTemplate(CommodityType.STORAGE_ALLOCATION, null, true)
                    );
        }

        private static Collection<CommBoughtProviderProp> getStorageBoughtCommodityTemplates() {
            return Collections.singletonList(
                    createCommBoughtProviderProp(
                            createLayeredOverProviderTemplate(EntityType.DISK_ARRAY, 1, Integer.MAX_VALUE),
                            getDASoldCommodityTemplates(),
                            false));
        }

        private static Collection<TemplateCommodity> getDCSoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.COOLING, null),
                    createCommodityTemplate(CommodityType.POWER, null),
                    createCommodityTemplate(CommodityType.DATACENTER, KEY)
                    );
        }

        private static Collection<TemplateCommodity> getDASoldCommodityTemplates() {
            return Collections.singletonList(createCommodityTemplate(CommodityType.EXTENT, KEY));
        }
    }

    /**
     * Util class to get storage supply chain instance, we use NetApp as the example.
     */
    private static class StorageSupplyChain {
        private static Collection<TemplateDTO> instance = null;

        public static Collection<TemplateDTO> getInstance() {
            if (instance == null) {
                create();
            }
            return instance;
        }

        private static void create() {
            final TemplateDTO st = createEntityTemplate(EntityType.STORAGE, TemplateType.BASE, 1,
                    getStorageSoldCommodityTemplates(), getStorageBoughtCommodityTemplates());
            final TemplateDTO da = createEntityTemplate(EntityType.DISK_ARRAY, TemplateType.BASE, 1,
                    getDASoldCommodityTemplates(), Collections.emptyList());

            instance = Collections.unmodifiableCollection(Arrays.asList(st, da));
        }

        private static Collection<TemplateCommodity> getStorageSoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.STORAGE_ACCESS, null),
                    createCommodityTemplate(CommodityType.STORAGE_AMOUNT, null),
                    createCommodityTemplate(CommodityType.STORAGE_CLUSTER, KEY),
                    createCommodityTemplate(CommodityType.DSPM_ACCESS, KEY)
                    );
        }

        private static Collection<CommBoughtProviderProp> getStorageBoughtCommodityTemplates() {
            return Collections.singletonList(
                    createCommBoughtProviderProp(
                            createLayeredOverProviderTemplate(EntityType.DISK_ARRAY, 1, Integer.MAX_VALUE),
                            getDASoldCommodityTemplates(),
                            false));
        }

        private static Collection<TemplateCommodity> getDASoldCommodityTemplates() {
            return Arrays.asList(
                    createCommodityTemplate(CommodityType.STORAGE_ACCESS, null),
                    createCommodityTemplate(CommodityType.STORAGE_AMOUNT, null),
                    createCommodityTemplate(CommodityType.EXTENT, KEY)
                    );
        }
    }

    public static Collection<TemplateDTO> brokenSupplyChain() {
        return Arrays.asList(
            createEntityTemplate(EntityType.VIRTUAL_MACHINE, TemplateType.EXTENSION, 1,
                Collections.emptyList(), Collections.emptyList()),
            createEntityTemplate(EntityType.STORAGE, TemplateType.EXTENSION, 1,
                Collections.emptyList(), Collections.emptyList()));
    }

    public static Collection<TemplateDTO> competingSupplyChain() {
        return Collections.singletonList(
                createEntityTemplate(EntityType.VIRTUAL_MACHINE, TemplateType.BASE, 0,
                        Collections.emptyList(), Collections.emptyList()));
    }

    public static Collection<TemplateDTO> supplyChainWithDisjunction() {
        return
            Arrays.asList(
                createEntityTemplate(
                    EntityType.STORAGE, TemplateType.BASE, 0,
                    Collections.emptyList(), Collections.emptyList(),
                    Arrays.asList(
                        createDisjunctiveSpecification(
                            createCommBoughtProviderProp(
                                createHostingProviderTemplate(EntityType.DISK_ARRAY),
                                Collections.singletonList(
                                        createCommodityTemplate(CommodityType.EXTENT, null)),
                                false),
                            createCommBoughtProviderProp(
                                createHostingProviderTemplate(EntityType.LOGICAL_POOL),
                                Collections.singletonList(
                                    createCommodityTemplate(CommodityType.EXTENT, null)),
                                false)),
                        createDisjunctiveSpecification(
                            createCommBoughtProviderProp(
                                createHostingProviderTemplate(EntityType.DISK_ARRAY),
                                Collections.singletonList(
                                    createCommodityTemplate(CommodityType.STORAGE_ACCESS, null)),
                                false)))),
                createEntityTemplate(
                    EntityType.LOGICAL_POOL, TemplateType.BASE, 0,
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                createEntityTemplate(
                    EntityType.DISK_ARRAY, TemplateType.BASE, 0,
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }
}
