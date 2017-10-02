package com.vmturbo.repository.graph.result;

import static com.vmturbo.repository.graph.result.ResultsFixture.APP_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.DA_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.PM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.ST_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.VM_TYPE;
import static com.vmturbo.repository.graph.result.ResultsFixture.fillOne;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import javaslang.Value;
import javaslang.collection.Stream;
import javaslang.test.Arbitrary;
import javaslang.test.Gen;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

/**
 * A centralised place for all the {@link Arbitrary} instances.
 *
 * These are used as generators during testing. The code here may look clunky.
 * That is because java lacks the syntactic sugar for monadic operations.
 * Syntactic sugar like do-notation will help a great deal.
 *
 * During tests, an arbitrary object of a given type is generated, and that arbitrary object
 * will be used as test input.
 */
public class ArbitraryFixture {

    public static Arbitrary<ServiceEntityRepoDTO> arbitraryServiceEntityRepoDTO(final String entityType,
                                                                                final Collection<ServiceEntityRepoDTO> providers) {
        return n -> genOid()
                       .flatMap(oid -> Arbitrary.list(Gen.choose(providers).arbitrary()).apply(providers.size())
                       .flatMap(ps  -> {
                           final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
                           serviceEntityRepoDTO.setOid(Long.toString(oid));
                           serviceEntityRepoDTO.setUuid(Long.toString(oid));
                           serviceEntityRepoDTO.setEntityType(entityType);
                           serviceEntityRepoDTO.setDisplayName(Joiner.on('-').join(entityType, oid));
                           serviceEntityRepoDTO.setProviders(ps.map(ServiceEntityRepoDTO::getOid).toJavaList());

                           return Gen.of(serviceEntityRepoDTO);
                       }));
    }

    public static Arbitrary<ServiceEntityRepoDTO> arbitraryServiceEntityRepoDTO(final Collection<Long> providerOids) {
        return n -> Gen.choose(Arrays.asList(PM_TYPE, VM_TYPE, APP_TYPE, DA_TYPE, ST_TYPE))
                .flatMap(entityType -> genOid()
                .flatMap(oid -> Arbitrary.list(Gen.choose(providerOids).arbitrary()).apply(providerOids.size())
                .flatMap(providers -> {
                    final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
                    serviceEntityRepoDTO.setOid(Long.toString(oid));
                    serviceEntityRepoDTO.setUuid(Long.toString(oid));
                    serviceEntityRepoDTO.setEntityType(entityType);
                    serviceEntityRepoDTO.setDisplayName(Joiner.on('-').join(entityType, oid));
                    serviceEntityRepoDTO.setProviders(providers.distinct().toJavaList()
                              .stream().map(p -> Long.toString(p)).collect(Collectors.toList()));

                    return Gen.of(serviceEntityRepoDTO);
                })));
    }

    public static Arbitrary<ServiceEntityRepoDTO> arbitraryServiceEntityRepoDTO() {
        return n -> Gen.choose(Arrays.asList(PM_TYPE, VM_TYPE, APP_TYPE, DA_TYPE, ST_TYPE))
                       .flatMap(entityType -> Gen.of(fillOne(entityType)));
    }

    public static Arbitrary<CommodityBoughtRepoDTO> arbitraryCommodityBoughtRepoDTO() {
        return n -> Gen.choose(0.0, 10.0)
                       .flatMap(used -> Gen.choose(0.0, 10.0)
                       .flatMap(peaked -> genCommodityType()
                       .flatMap(type -> {
                           final CommodityBoughtRepoDTO genBought = new CommodityBoughtRepoDTO();
                           genBought.setPeak(peaked);
                           genBought.setUsed(used);
                           genBought.setType(type);
                           return Gen.of(genBought);
                       })));
    }

    public static Gen<Long> genOid() {
        return Gen.choose(0, Long.MAX_VALUE);
    }

    /**
     *  Create a generator for {@link CommodityType}.
     *
     *  @return A {@link Gen}.
     */
    private static Gen<String> genCommodityType() {
        return Gen.choose(Stream.ofAll(Arrays.asList(CommodityType.values())).map(Enum::name));
    }

    private static Arbitrary<ServiceEntityRepoDTO> arbitraryServiceEntityRepoDTO(final String entityType,
                                                                                 final int providerSizeHint,
                                                                                 final List<ServiceEntityRepoDTO> providers) {
        return n -> Gen.choose(0L, Long.MAX_VALUE)
                .flatMap(oid -> {
                    Gen<javaslang.collection.List<ServiceEntityRepoDTO>> potentialProviders =
                            providers.isEmpty()? Gen.of(javaslang.collection.List.<ServiceEntityRepoDTO>empty())
                                               : Arbitrary.list(Gen.choose(providers).arbitrary()).apply(providerSizeHint);
                    return potentialProviders
                .flatMap(chosenProviders -> {
                    final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
                    serviceEntityRepoDTO.setOid(Long.toString(oid));
                    serviceEntityRepoDTO.setUuid(Long.toString(oid));
                    serviceEntityRepoDTO.setEntityType(entityType);
                    serviceEntityRepoDTO.setDisplayName(Joiner.on('-').join(entityType, oid));
                    serviceEntityRepoDTO.setProviders(chosenProviders.map(ServiceEntityRepoDTO::getOid).distinct().toJavaList());

                    return Gen.of(serviceEntityRepoDTO);
                });
                });
    }

    private static Arbitrary<List<ServiceEntityRepoDTO>> arbitraryRepoDTOs(
            final String entityType, final int providerSizeHint, final List<ServiceEntityRepoDTO> providers) {
        return Arbitrary.list(arbitraryServiceEntityRepoDTO(entityType, providerSizeHint, providers)).map(Value::toJavaList);
    }

    /**
     * Generate an arbitrary topology.
     *
     * Note that the generated topology may not be a valid topology.
     * A side note on the size hint parameters; those parameters give an estimate on size of
     * the <em>things</em> to generate. They are not used as exact numbers.
     *
     * The relationships mappings <em>MUST</em> not contain cycle.
     *
     * @param random A random number generator that drives the generation of the topology.
     * @param n A size hint for the number of entities this will generate per entity type.
     * @param relationships The provider relationships.
     * @param providerSizeHint The size hint for number of providers an entity may have.
     * @return A list of {@link ServiceEntityRepoDTO}.
     */
    private static List<ServiceEntityRepoDTO> arbitraryTopology(final Random random,
                                                                final int n,
                                                                final Multimap<String, String> relationships,
                                                                final int providerSizeHint) {
        final TopologicalSort tsort = new TopologicalSort(relationships);
        final List<String> sortedRels = tsort.sort();

        // Construct a map with entity type as the key and a list of entities of that type as the value.
        final Map<String, List<ServiceEntityRepoDTO>> mapOfArbs =
        Stream.ofAll(sortedRels).foldLeft(Maps.<String, List<ServiceEntityRepoDTO>>newHashMap(),
                (typeInsts, type) -> {
                    final Collection<String> pTypes = relationships.get(type);
                    // Get the list of providers based on the relationships mapping.
                    final List<ServiceEntityRepoDTO> arbitraryProvidersList =
                            Stream.ofAll(pTypes).map(typeInsts::get).flatMap(Stream::ofAll).toJavaList();

                    // Create an arbitrary list of service entity of the current type.
                    final Arbitrary<List<ServiceEntityRepoDTO>> arbitraryInstances =
                            arbitraryRepoDTOs(type, providerSizeHint, arbitraryProvidersList);

                    // Run the generators
                    final List<ServiceEntityRepoDTO> instances = arbitraryInstances.apply(n).apply(random);
                    typeInsts.put(type, instances);
                    return typeInsts;
                });

        return Stream.ofAll(mapOfArbs.values()).flatMap(Stream::ofAll).toJavaList();
    }

    private static class TopologicalSort {
        private final Multimap<String, String> graph;
        private final Set<String> marked;
        private final Set<String> tempMarked;
        private final List<String> sorted;

        public TopologicalSort(final Multimap<String, String> graph) {
            this.graph = graph;
            this.marked = new HashSet<>();
            this.tempMarked = new HashSet<>();
            this.sorted = new ArrayList<>();
        }

        public List<String> sort() {
            this.graph.keySet().forEach(this::visit);
            return sorted;
        }

        public  void visit(final String n) {
            if (tempMarked.contains(n)) {
                throw new IllegalStateException(this.graph + " is not a DAG");
            }

            if (!marked.contains(n)) {
                tempMarked.add(n);
                this.graph.get(n).forEach(this::visit);
                marked.add(n);
                tempMarked.remove(n);
                sorted.add(n);
            }
        }
    }
}
