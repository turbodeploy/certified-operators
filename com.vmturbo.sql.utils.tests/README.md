# Why does this module exist?

We have long had two related modules:

* `sql-utils` (in `com.vmturbo.sql.utils`) contains numerous classes that can be helpful in 
  developing code that accesses databases.
* `sql-test-utils` (in `com.vmturbo.sql.test.utils`) contains classes that support creation of 
  tests that make use of live database connections.

The `sql-test-utils` module includes a dependency on `sql-utils`, naturally enough. And some of 
the `sql-test-utils` class's dependencies on artifacts that normally would be in `test` scope 
are instead configured for `compile` scope, since its classes are in the module's `main` source 
tree, not its `test` tree. (This avoids the need to create and use a test-jar.)

So we have a bit of a problem if we want to create any live DB tests in `sql-utils`. To do so, 
we'd naturally want to make use of the `sql-test-util` classes, but for that we would need for 
the `sql-utils` module to have a dependency on `sql-test-utils`. That would be a circular 
dependency, which maven can't handle.

Hence, we have this module. It's intended to be a place where live DB test for `sql-utils` can be 
built. This module has a dependency on `sql-test-utils`, and hence it transitively has a dependency on 
`sql-utils` as well.

The recommended approach to implementing a live DB test for `sql-utils` module is to use 
whatever package and class name would make sense if the test class were in the `sql-utils` 
module itself, but just place it in `sql-utils-test` instead of `sql-utils`. This will preserve 
whatever package-private access may be needed.

## What about jOOQ?

Classes in this module will not normally have access ot the jOOQ models that we rely on in all our 
components. It is probably possible to arrange that access by adding a maven dependency from this
module to the desired component's main module. However, that is likely to run into difficulty
when more than one component are included. And besides, anything in the `sql-utils` module 
should be component-agnostic, so it makes more sense to create component-agnostic migrations and 
test using those. This approach has the added advantage that the component-agnostic migrations are
far less likely to grow over time, since they reflect functionality of the `sql-utils` module, 
rather than functionality of the Turbonomic product. So test execution should be faster than it 
would be if component migrations had to be executed.