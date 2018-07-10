#!/bin/bash -x

if [ "${WORKSPACE}" == "" ]; then
    echo 'Environment variable $WORKSPACE must be defined'
    exit 99
fi
mkdir -p ${WORKSPACE}/data
cd ${WORKSPACE}/data
rm -rf images >/dev/null 2>&1
mkdir -p ${WORKSPACE}/data/images
cd ${WORKSPACE}/data/images

# The following assumes that we have the script in the top-level directory and we have checked out the build project in the XL.
DOCKER_COMPOSE_YML_FILE_TGT=docker-compose.yml
DOCKER_COMPOSE_YML_FILE=prod-services.yml
images=$(egrep "^[\t ]+image[\t ]*\:.*" ${WORKSPACE}/build/${DOCKER_COMPOSE_YML_FILE} | sort | uniq | awk -F"image:" '{print $2}')

for img in ${images[@]}
do
    echo "Processing ${img}"
    # Use Bash wildcard matching. Must be used in [[ ]] brackets without double-quotes.
    if [[ $img != turbonomic* ]]; then
        # Generally available stuff
        # We assume that neither base image name, nor the version contain the ':'.
        # So the following happens for the file names:
        # arangodb:3.0.8 -> arangodb
        # grafana/grafana -> grafana
        # hopsoft/graphite-statsd -> graphite-statsd
        docker pull ${img} && docker save ${img} | xz -T0 -9 > $(echo $img | cut -d':' -f1 | cut -d'/' -f2).tgz
    else
        # Our stuff
		img_res=$(echo $img | cut -d'/' -f2 | awk -F'.component' '{print $1}')
		if [[ $img_res == com.vmturbo* ]]; then
			img_res=$(echo $img_res | awk -F'com.vmturbo.' '{print $2}')
		fi
		img_res=$(echo $img_res | tr '.' '_')
        docker save ${img}:latest | xz -T0 -9 > ${img_res}.tgz
    fi
done

# Copy the docker-compose.yml files for each size topology
cp ${WORKSPACE}/build/docker-compose.yml.15k ${WORKSPACE}/data/images/
cp ${WORKSPACE}/build/docker-compose.yml.25k ${WORKSPACE}/data/images/
cp ${WORKSPACE}/build/docker-compose.yml.50k ${WORKSPACE}/data/images/
cp ${WORKSPACE}/build/docker-compose.yml.100k ${WORKSPACE}/data/images/
cp ${WORKSPACE}/build/docker-compose.yml.200k ${WORKSPACE}/data/images/

# Copy the turboctl.py script to add into the iso
cp ${WORKSPACE}/build/turboctl.py ${WORKSPACE}/data/images/.

# Copy the upgrade script to add into the iso
cp ${WORKSPACE}/build/turboupgrade.py ${WORKSPACE}/data/images/.

# Copy vmtctl script to add into the iso
cp ${WORKSPACE}/build/vmtctl ${WORKSPACE}/data/images/.

# Copy the common definition .yml files with common values used by the above docker-compose.yml.nnk
cp ${WORKSPACE}/build/prod-services.yml ${WORKSPACE}/data/images/
cp ${WORKSPACE}/build/common-services.yml ${WORKSPACE}/data/images/

INFO="${WORKSPACE}/data/images/turbonomic_info.txt"
echo "Built on: $(date)" > ${INFO}
echo "Version: XL ${VER_SHORT}" >> ${INFO}
echo "Build #: ${RELEASE_REV}" >> ${INFO}

# Cleanup and create the ISO
cd ${WORKSPACE}/data
rm docker_images.iso
pushd images
sha256sum "${DOCKER_COMPOSE_YML_FILE_TGT}.15k" > turbonomic_sums.txt
sha256sum "${DOCKER_COMPOSE_YML_FILE_TGT}.25k" >> turbonomic_sums.txt
sha256sum "${DOCKER_COMPOSE_YML_FILE_TGT}.50k" >> turbonomic_sums.txt
sha256sum "${DOCKER_COMPOSE_YML_FILE_TGT}.100k" >> turbonomic_sums.txt
sha256sum "prod-services.yml" >> turbonomic_sums.txt
sha256sum "common-services.yml" >> turbonomic_sums.txt
sha256sum "turboctl.py" >> turbonomic_sums.txt
sha256sum "turboupgrade.py" >> turbonomic_sums.txt
sha256sum "vmtctl" >> turbonomic_sums.txt

# For customers who don't allow access to remote yum repository, we would have
# to ship the PyYAML package along with XL components.
# PyYAML is used by turboupgrade.py script for parsing yml files.
curl -O -ks https://10.10.150.66/repository/xl/PyYAML-3.10-11.el7.x86_64.rpm


for file in `ls *tgz`; do sha256sum $file >> turbonomic_sums.txt; done
popd
mkisofs -l -iso-level 4 -o docker_images_${RELEASE_REV}.iso images/
rm -f images/*
cd

#
# Diagnostics
#
cd ${WORKSPACE}/data/images
images=()
images+=('turbonomic/diags')
for img in ${images[@]}
do
    echo "Processing ${img}"
    # Our stuff
    img_res=$(echo $img | cut -d'/' -f2 | awk -F'.component' '{print $1}')
    if [[ $img_res == com.vmturbo* ]]; then
        img_res=$(echo $img_res | awk -F'com.vmturbo.' '{print $2}')
    fi
    img_res=$(echo $img_res | tr '.' '_')
    docker save ${img}:latest | xz -T0 -9 > ${img_res}.tgz
done
cd ${WORKSPACE}/data
rm docker_images.iso
pushd images
for file in `ls *tgz`; do sha256sum $file > turbonomic_sums.txt; done
popd
mkisofs -l -iso-level 4 -o docker_diags_${RELEASE_REV}.iso images/
rm -f images/*
cd
