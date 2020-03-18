package com.vmturbo.mediation.conversion.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Todo: here's hoping that someday soon the conversion logic is absorbed into the probes, and
 * this test is no longer necessary. We can dream, right?
 */
public class CloudDiscoveryConverterTest {
    /**
     * Test converting discovery response with a single Service Provider entity.
     */
    @Test
    public void testConvertServiceProvider() {
        // ARRANGE

        // Original discovery response consists of a single Service Provider entity
        final EntityDTO serviceProviderDto = EntityDTO.newBuilder()
                .setEntityType(EntityType.SERVICE_PROVIDER)
                .setId("serviceProviderId")
                .build();
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                .addEntityDTO(serviceProviderDto)
                .build();

        // Cloud conversion probe adds one Cloud Service entity
        final CloudProviderConversionContext context = mock(CloudProviderConversionContext.class);
        when(context.getCloudServicesToCreate()).thenReturn(ImmutableSet.of(CloudService.AWS_EBS));

        // ACT
        final CloudDiscoveryConverter converter = new CloudDiscoveryConverter(response, context);
        final DiscoveryResponse convertedResponse = converter.convert();

        // ASSERT

        // Result should contain 2 entities: Service Provider and Cloud Service
        assertEquals(2, convertedResponse.getEntityDTOCount());

        // Get Service Provider and check that Cloud Service was added to ConsistsOf list
        final Optional<EntityDTO> serviceProvider = convertedResponse.getEntityDTOList()
                .stream()
                .filter(e -> e.getEntityType() == EntityType.SERVICE_PROVIDER)
                .findAny();
        assertTrue(serviceProvider.isPresent());
        assertEquals(1, serviceProvider.get().getConsistsOfCount());
        final String consistsOfId = serviceProvider.get().getConsistsOf(0);
        assertEquals(CloudService.AWS_EBS.getId(), consistsOfId);
    }
}
