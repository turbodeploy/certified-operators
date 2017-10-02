#!/bin/bash

cp ../build_library/vm_image_util.sh ../build_library/vm_image_util.sh.orig
patch ../build_library/vm_image_util.sh < vmturbo.patch
