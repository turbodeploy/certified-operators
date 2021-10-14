package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.KubernetesServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.KubernetesServiceData.ServiceType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ServiceData;

/**
 * Test {@link ServiceInfoMapper}.
 */
public class ServiceInfoMapperTest {
    private final ServiceData.Builder data = ServiceData.newBuilder();
    private final EntityDTO.Builder entity = EntityDTO.newBuilder()
            .setEntityType(EntityType.SERVICE)
            .setId("foo");
    private final ServiceInfoMapper mapper = new ServiceInfoMapper();

    /**
     * Test Kubernetes ClusterIP service.
     */
    @Test
    public void testKubernetesClusterIPService() {
        entity.setServiceData(data.setKubernetesServiceData(
                KubernetesServiceData.newBuilder().setServiceType(ServiceType.ClusterIP)));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ServiceType.ClusterIP, info.getService().getKubernetesServiceData().getServiceType());
    }

    /**
     * Test Kubernetes NodePort service.
     */
    @Test
    public void testKubernetesNodePortService() {
        entity.setServiceData(data.setKubernetesServiceData(
                KubernetesServiceData.newBuilder().setServiceType(ServiceType.NodePort)));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ServiceType.NodePort, info.getService().getKubernetesServiceData().getServiceType());
    }

    /**
     * Test Kubernetes LoadBalancer service.
     */
    @Test
    public void testKubernetesLoadBalancerService() {
        entity.setServiceData(data.setKubernetesServiceData(
                KubernetesServiceData.newBuilder().setServiceType(ServiceType.LoadBalancer)));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ServiceType.LoadBalancer, info.getService().getKubernetesServiceData().getServiceType());
    }

    /**
     * Test Kubernetes ExternalName service.
     */
    @Test
    public void testKubernetesExternalNameService() {
        entity.setServiceData(data.setKubernetesServiceData(
                KubernetesServiceData.newBuilder().setServiceType(ServiceType.ExternalName)));
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(ServiceType.ExternalName, info.getService().getKubernetesServiceData().getServiceType());
    }
}
