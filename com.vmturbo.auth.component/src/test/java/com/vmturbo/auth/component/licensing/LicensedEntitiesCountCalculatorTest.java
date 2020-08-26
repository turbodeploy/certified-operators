package com.vmturbo.auth.component.licensing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.auth.component.licensing.LicensedEntitiesCountCalculator.LicensedEntitiesCount;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link LicensedEntitiesCountCalculator}.
 */
public class LicensedEntitiesCountCalculatorTest {

    private SearchServiceMole searchServiceMole = Mockito.spy(SearchServiceMole.class);

    /**
     * Test server for gRPC.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(searchServiceMole);

    private LicensedEntitiesCountCalculator licensedEntitiesCountCalculator;

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        licensedEntitiesCountCalculator = new LicensedEntitiesCountCalculator(
            SearchServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    /**
     * Test getting entity counts when under limit.
     */
    @Test
    public void testGetEntitiesCountVms() {
        LicenseDTO l1 = LicenseDTO.newBuilder()
            .setTurbo(TurboLicense.newBuilder()
                .setExpirationDate(ILicense.PERM_LIC)
                .setCountedEntity(CountedEntity.VM.name())
                .setNumLicensedEntities(100))
            .build();
        LicenseDTO l2 = LicenseDTO.newBuilder()
            .setTurbo(TurboLicense.newBuilder()
                .setExpirationDate(ILicense.PERM_LIC)
                .setCountedEntity(CountedEntity.VM.name())
                .setNumLicensedEntities(100))
            .build();

        Mockito.when(searchServiceMole.countEntities(Mockito.any()))
            .thenReturn(EntityCountResponse.newBuilder()
                    .setEntityCount(75)
                    .build());

        Optional<LicensedEntitiesCount> count =
                licensedEntitiesCountCalculator.getLicensedEntitiesCount(Arrays.asList(l1, l2));

        assertTrue(count.isPresent());
        assertThat(count.get().getNumInUse().get(), is(75));
        assertThat(count.get().getNumLicensed(), is(200));
        assertFalse(count.get().isOverLimit());
    }


    /**
     * Test getting entity counts when over limit.
     */
    @Test
    public void testGetEntitiesCountVmsOverLimit() {
        LicenseDTO l1 = LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                        .setExpirationDate(ILicense.PERM_LIC)
                        .setCountedEntity(CountedEntity.VM.name())
                        .setNumLicensedEntities(100))
                .build();
        LicenseDTO l2 = LicenseDTO.newBuilder()
                .setTurbo(TurboLicense.newBuilder()
                        .setExpirationDate(ILicense.PERM_LIC)
                        .setCountedEntity(CountedEntity.VM.name())
                        .setNumLicensedEntities(100))
                .build();

        Mockito.when(searchServiceMole.countEntities(Mockito.any()))
                .thenReturn(EntityCountResponse.newBuilder()
                        .setEntityCount(300)
                        .build());

        Optional<LicensedEntitiesCount> count =
                licensedEntitiesCountCalculator.getLicensedEntitiesCount(Arrays.asList(l1, l2));

        assertTrue(count.isPresent());
        assertThat(count.get().getNumInUse().get(), is(300));
        assertThat(count.get().getNumLicensed(), is(200));
        assertTrue(count.get().isOverLimit());
    }


}