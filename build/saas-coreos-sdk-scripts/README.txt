This README describes how to setup a CoreOS SDK, and the VMTurbo overlay for it.

1) create the CoreOS SDK, as described at: 
https://coreos.com/os/docs/latest/sdk-modifying-coreos.html

follow it until you are able to setup the board, but don't build the packages.

2) download the latest stable CoreOS img from:
http://stable.release.core-os.net/amd64-usr/current/

the format is
coreos_production_image.bin.bz2

and unpack it into the src/build/images/amd64-usr/latest folder.

3) apply the patch to the SDK environment running:
apply-vm-image-script-patch.sh

4) copy the oem-vmturbo dir into the src/third_party/coreos-overlay/coreos-base

5) run those scripts to create the requested image
create-hyperv-image.sh
create-openstack-image.sh
