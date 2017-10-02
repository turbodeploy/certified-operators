package com.vmturbo.repository.graph.executor;

import static com.vmturbo.repository.graph.ArangoDBResultFixtures.TEST_COLLECTION;
import static com.vmturbo.repository.graph.ArangoDBResultFixtures.TEST_DATABASE;
import static com.vmturbo.repository.graph.result.ResultsFixture.APP_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DC_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoDB;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.ArangoDBCursorTestData;
import com.vmturbo.repository.graph.ArangoDBResultFixtures;
import com.vmturbo.repository.graph.ImmutableArangoDBCursorTestData;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.graph.result.SupplyChainFluxResult;
import com.vmturbo.repository.graph.result.TypeAndOids;

@RunWith(MockitoJUnitRunner.class)
public class ReactiveArangoDBExecutorTest {

    @Mock
    private ArangoDB arangoDB;

    @Mock
    private ArangoDatabaseFactory arangoDatabaseFactory;

    private ReactiveArangoDBExecutor reactiveArangoDBExecutor;

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new GuavaModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        reactiveArangoDBExecutor = new ReactiveArangoDBExecutor(arangoDatabaseFactory, objectMapper);
    }

    @Test
    public void testFluxify() {
        final List<ScopedEntity> entities = List.of(new ScopedEntity(VM_TYPE, "VM-1", 1L),
                                                    new ScopedEntity(PM_TYPE, "PM-1", 2L));
        final ArangoDBCursorTestData<ScopedEntity> cursorTestData = ImmutableArangoDBCursorTestData.<ScopedEntity>builder()
                .data(entities)
                .klass(ScopedEntity.class)
                .build();

        ArangoDBResultFixtures.givenArangoDBQueryWillReturnResults(arangoDatabaseFactory,
                                                                   TEST_DATABASE,
                                                                   Collections.singletonList(cursorTestData),
                                                                   objectMapper);

        final Flux<ScopedEntity> resultFlux = reactiveArangoDBExecutor.fetchScopedEntities(TEST_DATABASE, TEST_COLLECTION, List.empty());

        final List<ScopedEntity> entitiesFromFlux = List.ofAll(resultFlux.collectList().block());

        assertThat(entitiesFromFlux).isEqualTo(entities);
    }

    @Test(expected = RuntimeException.class)
    public void testFluxifyWithException() {
        ArangoDBResultFixtures.givenArangoDBWillThrowException(arangoDB, new RuntimeException("Testing: Mock exception"));

        final Flux<ScopedEntity> resultFlux = reactiveArangoDBExecutor.fetchScopedEntities(TEST_DATABASE, TEST_COLLECTION, List.empty());

        resultFlux.doOnNext(v -> fail("The flux should not have any output"));
        resultFlux.collectList().block();
    }

    @Test
    public void testGlobalSupplyChain() {

        final List<TypeAndOids> referenceTypeAndOids = List.of(
                new TypeAndOids("VirtualMachine", Lists.newArrayList(1L, 2L, 3L)),
                new TypeAndOids("PhysicalMachine", Lists.newArrayList(4L, 5L, 6L, 7L)));

        final ArangoDBCursorTestData<TypeAndOids> testData = ImmutableArangoDBCursorTestData.<TypeAndOids>builder()
                .data(referenceTypeAndOids)
                .klass(TypeAndOids.class)
                .build();

        ArangoDBResultFixtures.givenArangoDBQueryWillReturnResults(arangoDatabaseFactory,
                                                                   TEST_DATABASE,
                                                                   Collections.singleton(testData),
                                                                   objectMapper);

        final GraphCmd.GetGlobalSupplyChain globalSupplyChainCmd = new GraphCmd.GetGlobalSupplyChain(
                TEST_DATABASE, TEST_COLLECTION, HashMultimap.create());

        final GlobalSupplyChainFluxResult globalResults = reactiveArangoDBExecutor
                .executeGlobalSupplyChainCmd(globalSupplyChainCmd);

        assertThat(List.ofAll(globalResults.entities().collectList().block())).isEqualTo(referenceTypeAndOids);
    }

    @Test
    public void testSupplyChain() throws JsonProcessingException {
        // Test input data
        final ServiceEntityRepoDTO origin = new ServiceEntityRepoDTO();
        origin.setDisplayName("unit test service entity");
        origin.setOid("9000");

        final List<TypeAndOids> providerResults = List.of(
                new TypeAndOids(DC_TYPE, Lists.newArrayList(1L)),
                new TypeAndOids(PM_TYPE, Lists.newArrayList(2L, 3L, 4L)));
        final ArangoDBCursorTestData<TypeAndOids> providerTestData = ImmutableArangoDBCursorTestData.<TypeAndOids>builder()
                .data(providerResults)
                .klass(TypeAndOids.class)
                .build();

        final List<TypeAndOids> consumerResults = List.of(
                new TypeAndOids(APP_TYPE, Lists.newArrayList(5L, 6L)));
        final ArangoDBCursorTestData<TypeAndOids> consumerTestData = ImmutableArangoDBCursorTestData.<TypeAndOids>builder()
                .data(consumerResults)
                .klass(TypeAndOids.class)
                .build();

        final List<ServiceEntityRepoDTO> originResult = List.of(origin);
        final ArangoDBCursorTestData<ServiceEntityRepoDTO> originTestData =
                ImmutableArangoDBCursorTestData.<ServiceEntityRepoDTO>builder()
                        .data(originResult)
                        .klass(ServiceEntityRepoDTO.class)
                        .build();

        // Stub the database results
        ArangoDBResultFixtures.givenSupplyCahinQueryWillReturnResults(arangoDatabaseFactory, TEST_DATABASE,
                providerTestData, consumerTestData, originTestData, objectMapper);

        final GraphCmd.GetSupplyChain supplyChainCmd = new GraphCmd.GetSupplyChain("start",
                TEST_DATABASE, "graphName", TEST_COLLECTION);
        final SupplyChainFluxResult supplyChainResult = reactiveArangoDBExecutor.executeSupplyChainCmd(supplyChainCmd);

        assertThat(List.ofAll(supplyChainResult.providerResults().collectList().block())).isEqualTo(providerResults);
        assertThat(List.ofAll(supplyChainResult.consumerResults().collectList().block())).isEqualTo(consumerResults);
        assertThat(supplyChainResult.origin().block()).isEqualTo(origin);
    }
}