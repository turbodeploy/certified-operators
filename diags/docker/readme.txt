In order to use this as a diagnostic package:

$ docker network ls | grep default
e9ed38ae7bde        build_default       bridge              local

Choose the network that appears:
docker run -it --rm --pid=host --network=build_default --hostname=diags turbonomic/diags

# For example, the following command will run the diags container on the default network with the db volumes mounted:
docker run -it --rm --pid=host --network=$(docker network ls | grep default | awk 'NR==1{print $2; exit}') --hostname=diags --volumes-from build_db_1 turbonomic/diags

or:
docker run -it --rm --pid=host --network=$(docker network ls -f 'name=.*_default' --format "{{.Name}}" | head -n 1) --hostname=diags --volumes-from build_db_1 turbonomic/diags

or:
In order to strace process in a different container (Please replace network and the container as needed):

docker run -it --privileged --rm --pid=host --network=build_default --hostname=diags --volumes-from build_db_1 --cap-add=ALL turbonomic/diags

