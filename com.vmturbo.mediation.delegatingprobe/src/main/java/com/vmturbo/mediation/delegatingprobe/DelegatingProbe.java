package com.vmturbo.mediation.delegatingprobe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainLink;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainLinkBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.ISupplyChainAwareProbe;

/**
 * A probe that delegates discovery to an external service, essentially acting as a man-in-the-middle.
 * Discovery requests are delegated out to a discovery driver and the discovery results
 * from the driver are passed back through to the original requester.
 */
public class DelegatingProbe implements IDiscoveryProbe<DelegatingProbeAccount>,
        ISupplyChainAwareProbe<DelegatingProbeAccount> {

    private final Logger logger = LogManager.getLogger(getClass());
    private AtomicInteger discoveryIndex = new AtomicInteger(0);

    /**
     * A simple request for a delegated discovery.
     * Sent to the discovery driver for this probe when performing a delegated discovery.
     */
    public static class DelegatingDiscoveryRequest {
        private final int discoveryIndex;

        public DelegatingDiscoveryRequest() {
            this.discoveryIndex = 0;
        }

        public DelegatingDiscoveryRequest(final int discoveryIndex) {
            this.discoveryIndex = discoveryIndex;
        }

        /**
         * Get the index of the discovery. The initial discovery index is 0 and each subsequent
         * discovery increments the index by 1.
         *
         * @return The discovery index.
         */
        public int getDiscoveryIndex() {
            return discoveryIndex;
        }
    }

    /**
     * Execute discovery of the target.
     *
     * @param accountValues Account definition map.
     * @return A set of entity DTOs for retrieved service entities.
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull DelegatingProbeAccount accountValues) throws InterruptedException {
        logger.info("Retrieving results from driver at " + accountValues.getDriverRootUri() +
            " with endpoint \"" + accountValues.getDriverEndpoint() + "\"");

        final HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);
        requestHeaders.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));

        // Using the default list of message converters is error-prone inside a probe due to
        // the way probes get wrapped in the mediation component. We explicitly define only
        // the converters we want to use - a JSON converter and an OCTET_STREAM converter - and
        // create a Rest Template with those.
        final List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
        messageConverters.add(new ByteArrayHttpMessageConverter());
        final GsonHttpMessageConverter gsonHttpMessageConverter = new GsonHttpMessageConverter();
        gsonHttpMessageConverter.setGson(ComponentGsonFactory.createGson());
        messageConverters.add(gsonHttpMessageConverter);

        final RestTemplate restTemplate = new RestTemplate(messageConverters);
        final DefaultUriTemplateHandler handler = new DefaultUriTemplateHandler();
        handler.setBaseUrl(accountValues.getDriverRootUri());
        restTemplate.setUriTemplateHandler(handler);

        final DelegatingDiscoveryRequest request = new DelegatingDiscoveryRequest(discoveryIndex.getAndIncrement());
        final ResponseEntity<byte[]> response =
                restTemplate.exchange("/" + accountValues.getDriverEndpoint(), HttpMethod.POST,
                        new HttpEntity<>(request, requestHeaders), byte[].class);
        return parseDiscoveryResponse(response.getBody() == null ? new byte[]{} : response.getBody());
    }

    /**
     * Parse the discovery response from the driver. Note that we must exchange data with the driver
     * in a raw byte format because most utilities for performing automatic serialization and deserialization
     * are incompatible with the infrastructure used to set up the class loaders for probe.
     *
     * Using libraries with custom classloaders such as Spring, most webservers, most deserialization frameworks
     * including protobuf-utils and GSON all result in runtime errors of various stripes being thrown
     * if you attempt to use them in a probe.
     *
     * @param bytes The bytestream to deserialize to a discovery response.
     *              If deserializtion fails an empty response is returned.
     * @return The deserialized {@link DiscoveryResponse}.
     */
    private DiscoveryResponse parseDiscoveryResponse(final byte[] bytes) {
        logger.debug("Received " + bytes.length + " bytes.");
        DiscoveryResponse discoveryResponse = DiscoveryResponse.getDefaultInstance();

        try {
            discoveryResponse = DiscoveryResponse.parseFrom(bytes);
            logger.info("Received discovery response with " + discoveryResponse.getEntityDTOList().size() + " entities.");
        } catch (Exception e) {
            logger.error("Error while parsing discovery request: ", e);
        }

        return discoveryResponse;
    }

    /**
     * Get the supply chain for this probe. Buying / Selling relationship between service entities:
     * Data centers sell commodities to hosts. Hosts sell commodities to virtual machines. A disk
     * array sells commodities to storages. Storages sell commodities to physical and virtual
     * machines.
     *
     * @return A set of template DTOs for this probe.
     */
    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        logger.info("Get supply chain");
        SupplyChainBuilder scb = new SupplyChainBuilder();

        // APP COMPONENT
        SupplyChainNodeBuilder appComponentNode = new SupplyChainNodeBuilder()
                .entity(EntityType.APPLICATION_COMPONENT)
                .selling(CommodityType.TRANSACTION);

        // VM
        SupplyChainNodeBuilder vmNode = new SupplyChainNodeBuilder()
                .entity(EntityType.VIRTUAL_MACHINE)
                .selling(CommodityType.VCPU)
                .selling(CommodityType.VMEM)
                .selling(CommodityType.VSTORAGE);

        // PM
        SupplyChainNodeBuilder pmNode = new SupplyChainNodeBuilder()
                .entity(EntityType.PHYSICAL_MACHINE)
                .selling(CommodityType.CPU)
                .selling(CommodityType.MEM)
                .selling(CommodityType.NET_THROUGHPUT);

        // Storage
        SupplyChainNodeBuilder stNode = new SupplyChainNodeBuilder().entity(EntityType.STORAGE)
                .selling(CommodityType.STORAGE_AMOUNT)
                .selling(CommodityType.STORAGE_ACCESS)
                .selling(CommodityType.STORAGE_LATENCY)
                .selling(CommodityType.STORAGE_PROVISIONED);

        // DiskArray
        SupplyChainNodeBuilder daNode = new SupplyChainNodeBuilder().entity(EntityType.DISK_ARRAY)
                .selling(CommodityType.EXTENT);

        // Datacenter
        SupplyChainNodeBuilder dcNode = new SupplyChainNodeBuilder().entity(EntityType.DATACENTER)
                .selling(CommodityType.SPACE)
                .selling(CommodityType.POWER)
                .selling(CommodityType.COOLING);

        // Link from APP COMP to VM
        final SupplyChainLink appComp2vmLink = new SupplyChainLinkBuilder()
                .link(EntityType.APPLICATION_COMPONENT, EntityType.VIRTUAL_MACHINE,
                        ProviderType.HOSTING)
                .commodity(CommodityType.VCPU).commodity(CommodityType.VMEM)
                .build();

        // Link from VM to PM
        final SupplyChainLink vm2pmLink = new SupplyChainLinkBuilder()
                .link(EntityType.VIRTUAL_MACHINE, EntityType.PHYSICAL_MACHINE,
                        ProviderType.HOSTING)
                .commodity(CommodityType.CPU)
                .commodity(CommodityType.MEM)
                .commodity(CommodityType.NET_THROUGHPUT)
                .build();

        // Link from VM to ST
        final SupplyChainLink vm2stLink = new SupplyChainLinkBuilder()
                .link(EntityType.VIRTUAL_MACHINE, EntityType.STORAGE,
                        ProviderType.LAYERED_OVER)
                .commodity(CommodityType.STORAGE_AMOUNT)
                .commodity(CommodityType.STORAGE_ACCESS)
                .commodity(CommodityType.STORAGE_PROVISIONED)
                .commodity(CommodityType.STORAGE_LATENCY)
                .build();

        // Link from DS to ST
        final SupplyChainLink da2stLink = new SupplyChainLinkBuilder()
                .link(EntityType.STORAGE, EntityType.DISK_ARRAY, ProviderType.HOSTING)
                .commodity(CommodityType.EXTENT)
                .build();

        // Link from PM to DC
        final SupplyChainLink pm2dcLink = new SupplyChainLinkBuilder()
                .link(EntityType.PHYSICAL_MACHINE, EntityType.DATACENTER,
                        ProviderType.HOSTING)
                .commodity(CommodityType.COOLING)
                .commodity(CommodityType.POWER)
                .commodity(CommodityType.SPACE)
                .build();

        return scb.top(appComponentNode)
                // Next Node - Connect APP to VM
                .entity(appComponentNode).connectsTo(vmNode, appComp2vmLink)
                // Next Node - Connect VM to PM and ST
                .entity(vmNode).connectsTo(pmNode, vm2pmLink).connectsTo(stNode, vm2stLink)
                // Next Node - Connect PM to DC
                .entity(pmNode).connectsTo(dcNode, pm2dcLink)
                // Next node - Connects ST to DA
                .entity(stNode).connectsTo(daNode, da2stLink)
                // Last Node - no more connections
                .entity(dcNode).configure();
    }

    @Nonnull
    @Override
    public Class<DelegatingProbeAccount> getAccountDefinitionClass() {
        return DelegatingProbeAccount.class;
    }

    /**
     * Validate the target.
     *
     * @param accountValues Account definition map.
     * @return The message of target validation status.
     */
    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull DelegatingProbeAccount accountValues) {
        logger.info("Validate Target");
        return ValidationResponse.newBuilder().build();
    }
}
