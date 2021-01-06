package com.vmturbo.repository.api;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;

/**
 * Test RepositoryClient class.
 */
public class RepositoryClientTest {

    private long awsMasterAccountId = 1L;
    private long awsSubAccountId1 = 2L;
    private long awsSubAccountId2 = 3L;
    private long awsSubAccountId3 = 4L;
    private long azureAcountId1 = 5L;
    private long azureAcountId2 = 6L;
    private long standaloneAccountId = 7L;

    private RepositoryClient repositoryClient;

    // The repository service mole.
    private final RepositoryServiceMole testRepositoryService = spy(new RepositoryServiceMole());

    /**
     * The grpc Test Server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testRepositoryService);

    /**
     * The  real-time topology context Id.
     */
    private static final Long realtimeTopologyContextId = 777777L;

    /**
     * Setup to run before tests.
     */
    @Before
    public void setup() {
        repositoryClient = spy(new RepositoryClient(grpcServer.getChannel(), realtimeTopologyContextId));
    }

    /**
     * Test getAllRelatedBusinessAccountOids method.
     */
    @Test
    public void testGetAllRelatedBusinessAccountOids() {
        List<TopologyEntityDTO> allAccounts = createAllBusinessAccounts();
        Mockito.when(repositoryClient.getAllBusinessAccounts(Mockito.anyLong())).thenReturn(allAccounts);

        Set<Long> expectedRelatedAwsAccounts = new HashSet<>();
        expectedRelatedAwsAccounts.add(awsMasterAccountId);
        expectedRelatedAwsAccounts.add(awsSubAccountId1);
        expectedRelatedAwsAccounts.add(awsSubAccountId2);
        expectedRelatedAwsAccounts.add(awsSubAccountId3);

        Set<Long> expectedRelatedAzureAccounts = new HashSet<>();
        expectedRelatedAzureAccounts.add(azureAcountId1);
        expectedRelatedAzureAccounts.add(azureAcountId2);

        Set<Long> expectedStandaloneAccounts = new HashSet<>();
        expectedStandaloneAccounts.add(standaloneAccountId);

        //
        // Given the AWS master account ID, return the master account ID and IDs of all sub-accounts.
        //
        Set<Long> relatedAccounts = repositoryClient.getAllRelatedBusinessAccountOids(awsMasterAccountId);
        assertTrue(expectedRelatedAwsAccounts.containsAll(relatedAccounts)
                && expectedRelatedAwsAccounts.size() == relatedAccounts.size());

        //
        // Given the ID of an AWS sub-account, return the master account ID and IDs of all sub-account.
        //
        relatedAccounts = repositoryClient.getAllRelatedBusinessAccountOids(awsSubAccountId1);
        assertTrue(expectedRelatedAwsAccounts.containsAll(relatedAccounts)
                && expectedRelatedAwsAccounts.size() == relatedAccounts.size());

        //
        // Given the ID of an Azure account, return the IDs of all Azure accounts with the same
        // enrollment number.
        //
        relatedAccounts = repositoryClient.getAllRelatedBusinessAccountOids(azureAcountId1);
        assertTrue(expectedRelatedAzureAccounts.containsAll(relatedAccounts)
                && expectedRelatedAzureAccounts.size() == relatedAccounts.size());

        //
        // Given the ID of a standalone account, return the same ID.
        //
        relatedAccounts = repositoryClient.getAllRelatedBusinessAccountOids(standaloneAccountId);
        assertTrue(expectedStandaloneAccounts.containsAll(relatedAccounts)
                && expectedStandaloneAccounts.size() == relatedAccounts.size());
    }

    private List<TopologyEntityDTO> createAllBusinessAccounts() {
        // AWS accounts
        List<ConnectedEntity> subAccounts = new ArrayList<>();
        subAccounts.add(createConnectedEntity(awsSubAccountId1));
        subAccounts.add(createConnectedEntity(awsSubAccountId2));
        subAccounts.add(createConnectedEntity(awsSubAccountId3));

        TopologyEntityDTO masterAccount = createAwsBusinessAccount(awsMasterAccountId, subAccounts);
        TopologyEntityDTO subAccount1 = createAwsBusinessAccount(awsSubAccountId1, Collections.emptyList());
        TopologyEntityDTO subAccount2 = createAwsBusinessAccount(awsSubAccountId2, Collections.emptyList());
        TopologyEntityDTO subAccount3 = createAwsBusinessAccount(awsSubAccountId3, Collections.emptyList());

        // Azure accounts
        TopologyEntityDTO azureAccount1 = createAzureAccount(azureAcountId1);
        TopologyEntityDTO azureAccount2 = createAzureAccount(azureAcountId2);

        // Standalone account
        TopologyEntityDTO standaloneAccount = createAwsBusinessAccount(standaloneAccountId, Collections.emptyList());

        return ImmutableList.of(masterAccount, subAccount1, subAccount2, subAccount3, azureAccount1,
                azureAccount2, standaloneAccount);
    }

    private TopologyEntityDTO createAwsBusinessAccount(long accountId, List<ConnectedEntity> connectedEntities) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(accountId)
                .addAllConnectedEntityList(connectedEntities)
                .build();
    }

    private TopologyEntityDTO createAzureAccount(long accountId) {
        String enrollmentNumber = "EN1234";
        TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
                .setBusinessAccount(BusinessAccountInfo.newBuilder()
                        .addPricingIdentifiers(PricingIdentifier.newBuilder()
                                .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                                .setIdentifierValue(enrollmentNumber)
                                .build())
                        .build())
                .build();
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(accountId)
                .setTypeSpecificInfo(typeSpecificInfo)
                .build();
    }

    private ConnectedEntity createConnectedEntity(long entityId) {
        return ConnectedEntity.newBuilder()
                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setConnectedEntityId(entityId)
                .build();
    }

}
