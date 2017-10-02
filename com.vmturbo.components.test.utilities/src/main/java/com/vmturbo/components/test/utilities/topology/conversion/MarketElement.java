package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The JAXB model for parsing a classic topology Market object.
 *
 * Usually it is the MainMarket that is of interest as opposed to, say,
 * the shadow objects or plan markets.
 */
@XmlType(name = "",
    propOrder = {
        "ServiceEntities"
    }
)
@XmlAccessorType(XmlAccessType.FIELD)
public class MarketElement implements IntoSdkProto<DiscoveryResponse> {

    @XmlElement(name="ServiceEntities")
    private List<ServiceEntityElement> ServiceEntities = new ArrayList<>();

    @XmlAttribute(name="mainMarket")
    private boolean mainMarket;

    @XmlAttribute(name="uuid")
    private String uuid;

    @XmlAttribute(name="name")
    private String name;

    public boolean isMainMarket() {
        return mainMarket;
    }

    public void setMainMarket(boolean mainMarket) {
        this.mainMarket = mainMarket;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ServiceEntityElement> getServiceEntities() {
        return ServiceEntities;
    }

    public void setServiceEntities(List<ServiceEntityElement> entities) {
        this.ServiceEntities = entities;
    }

    @Override
    public String toString() {
        return "name: " + name + ", uuid: " + uuid + ", mainMarket: " + mainMarket + ", #SE: "
            + ServiceEntities.size();
    }

    /**
     * Convert the market object into a {@link DiscoveryResponse}. For now, only
     * includes {@link ServiceEntityElement} objects as converted to {@link EntityDTO} protobufs.
     *
     * @return
     */
    @Nonnull
    @Override
    public DiscoveryResponse toSdkProto() {
        final Map<String, String> commoditySoldIdToProviderIdMap = new HashMap<>(ServiceEntities.size());
        ServiceEntities.forEach(entity ->
            entity.getCommodities().forEach(commodity ->
                commoditySoldIdToProviderIdMap.put(commodity.getUuid(), entity.getUuid())));

        return DiscoveryResponse.newBuilder()
            .addAllEntityDTO(
                getServiceEntities().stream()
                    .filter(ServiceEntityElement::isServiceEntity)
                    .map(entity -> {
                        final EntityDTO.Builder builder = entity.toSdkProto();
                        entity.setupCommoditiesBought(builder, commoditySoldIdToProviderIdMap);
                        return builder.build();
                    })
                    .collect(Collectors.toList()))
            .build();
    }
}
