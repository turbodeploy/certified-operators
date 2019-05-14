This script pulls images for the base XL components from the docker
registry. These are images that developers are not generally able to
recreate due to lack fo licenses for the underlying RHEL image.

The script can be run via maven, or manually from the command
line. The advantage of running from maven is that the version will
default to whatever snapshot versions is currently configured in the
project pom.xml files.

Runnig via maven:

  mvn [-f <build-dir>] -Prefresh-base-components [-Dversion=<version>]


* Specify -f <build-dir> if you're doing this from outside the build
  directory (e.g. -f build if executing from the git root).

* Specify -Dversion=<version> if you want to refresh for a specific
  version. This will override the default project version configured
  in build/pom.xml.


Running without maven:


  <build-dir>refresh-base-components/refresh-base-components.sh <version>


In this case, there is no default version; you must supply it directly.
