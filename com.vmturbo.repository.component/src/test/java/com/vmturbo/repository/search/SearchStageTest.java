package com.vmturbo.repository.search;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class SearchStageTest {

    @Test
    public void searchStageComposeInOrder() {
        final SearchStage<String, Collection<String>, Collection<String>> addFoo =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-foo").collect(Collectors.toList());
        final SearchStage<String, Collection<String>, Collection<String>> addBar =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-bar").collect(Collectors.toList());

        final SearchStage<String, Collection<String>, Collection<String>> composed = addFoo.andThen(addBar);

        final Collection<String> results = composed.run("", Arrays.asList("str1", "str2"));

        assertThat(results).containsExactly("str1-foo-bar", "str2-foo-bar");
    }

    @Test
    public void testTypeSafeCompose() {
        final SearchStage<String, Collection<String>, Collection<String>> addFoo =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-foo").collect(Collectors.toList());
        final SearchStage<String, Collection<String>, Collection<Integer>> toIntAddOne =
                () -> (ctx, ss) -> ss.stream().map(s -> s.length() + 1).collect(Collectors.toList());
        final SearchStage<String, Collection<Integer>, Collection<String>> toString =
                () -> (ctx, is) -> is.stream().map(Object::toString).collect(Collectors.toList());

        final SearchStage<String, Collection<String>, Collection<String>> composed =
                addFoo.andThen(toIntAddOne).andThen(toString);

        final Collection<String> results = composed.run("", Arrays.asList("str1", "str2"));

        // 9 = "str1-foo".length + 1
        assertThat(results).containsExactly("9", "9");
    }

    @Test
    public void testFold() {
        final SearchStage<String, Collection<String>, Collection<String>> addFoo =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-foo").collect(Collectors.toList());
        final SearchStage<String, Collection<String>, Collection<String>> addBar =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-bar").collect(Collectors.toList());

        final SearchStage<String, Collection<String>, Collection<String>> composed =
                SearchStage.fold(Arrays.asList(addFoo, addBar));

        final Collection<String> results = composed.run("", Arrays.asList("str1", "str2"));

        assertThat(results).containsExactly("str1-foo-bar", "str2-foo-bar");
    }

    @Test
    public void testIdentity() {
        final SearchStage<String, Collection<String>, Collection<String>> identStage = SearchStage.identity();
        final SearchStage<String, Collection<String>, Collection<String>> addFoo =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-foo").collect(Collectors.toList());

        final SearchStage<String, Collection<String>, Collection<String>> composed = identStage.andThen(addFoo);

        final Collection<String> results = composed.run("", Arrays.asList("str1", "str2"));

        assertThat(results).containsExactly("str1-foo", "str2-foo");
    }

    @Test
    public void testFmapIdentity() {
        final SearchStage<String, Collection<String>, Collection<String>> addFoo =
                () -> (ctx, ss) -> ss.stream().map(s -> s + "-foo").collect(Collectors.toList());
        final SearchStage<String, Collection<String>, Collection<String>> identityLaw = addFoo.fmap(Function.identity());

        final Collection<String> results = identityLaw.run("", Arrays.asList("str1", "str2"));

        assertThat(results).containsExactly("str1-foo", "str2-foo");
    }

    @Test
    public void testPure() {
        final SearchStage<String, Collection<String>, List<String>> liftedValue =
                SearchStage.pure(Arrays.asList("value1", "value2"));
        final List<String> results = liftedValue.run("", Collections.emptyList());// input is ignored.

        assertThat(results).containsExactly("value1", "value2");
    }

    @Test
    public void testLift() {
        final Function<Collection<String>, Collection<Integer>> toLength =
                ss -> ss.stream().map(String::length).collect(Collectors.toList());

        final Function<SearchStage<String, Collection<String>, Collection<String>>,
                       SearchStage<String, Collection<String>, Collection<Integer>>> lifted = SearchStage.lift(toLength);

        final SearchStage<String, Collection<String>, Collection<Integer>> running =
                lifted.apply(SearchStage.pure(Arrays.asList("str1", "longer", "str2")));

        // The input (emptyList) is ignored because of pure().
        final Collection<Integer> results = running.run("", Collections.emptyList());

        assertThat(results).containsExactly(4, 6, 4);
    }

    @Test
    public void testFlapMap() {
        final SearchStage<String, Collection<String>, Collection<Integer>> flatMapped =
                SearchStage.<String, Collection<String>, Collection<String>>pure(Arrays.asList("val1", "value2"))
                           .flatMap(SearchStageTest::mapper);
        final Collection<Integer> results = flatMapped.run("", Collections.emptyList());

        assertThat(results).containsExactly(4, 6);
    }

    private static SearchStage<String, Collection<String>, Collection<Integer>> mapper(final Collection<String> ss) {
        return () -> (ctx, in) -> ss.stream().map(String::length).collect(Collectors.toList());
    }
}