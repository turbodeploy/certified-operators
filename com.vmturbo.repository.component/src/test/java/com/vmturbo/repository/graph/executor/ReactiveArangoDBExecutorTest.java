package com.vmturbo.repository.graph.executor;

import static com.vmturbo.repository.graph.ArangoDBResultFixtures.TEST_COLLECTION;
import static com.vmturbo.repository.graph.ArangoDBResultFixtures.TEST_DATABASE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Collections;
import java.util.Optional;

import com.arangodb.ArangoDB;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

import com.vmturbo.repository.graph.ArangoDBCursorTestData;
import com.vmturbo.repository.graph.ArangoDBResultFixtures;
import com.vmturbo.repository.graph.ImmutableArangoDBCursorTestData;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.graph.result.SupplyChainOidsGroup;

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

        final List<SupplyChainOidsGroup> referenceTypeAndOids = List.of(
                new SupplyChainOidsGroup("VirtualMachine", "ACTIVE", Lists.newArrayList(1L, 2L, 3L)),
                new SupplyChainOidsGroup("PhysicalMachine", "ACTIVE", Lists.newArrayList(4L, 5L, 6L, 7L)));

        final ArangoDBCursorTestData<SupplyChainOidsGroup> testData = ImmutableArangoDBCursorTestData.<SupplyChainOidsGroup>builder()
                .data(referenceTypeAndOids)
                .klass(SupplyChainOidsGroup.class)
                .build();

        ArangoDBResultFixtures.givenArangoDBQueryWillReturnResults(arangoDatabaseFactory,
                                                                   TEST_DATABASE,
                                                                   Collections.singleton(testData),
                                                                   objectMapper);

        final GraphCmd.GetGlobalSupplyChain globalSupplyChainCmd = new GraphCmd.GetGlobalSupplyChain(
                TEST_DATABASE, TEST_COLLECTION, Optional.empty(), Optional.empty(), Collections.emptySet());

        final GlobalSupplyChainFluxResult globalResults = reactiveArangoDBExecutor
                .executeGlobalSupplyChainCmd(globalSupplyChainCmd);

        assertThat(List.ofAll(globalResults.entities().collectList().block())).isEqualTo(referenceTypeAndOids);
    }
}