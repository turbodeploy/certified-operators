#!/bin/bash -e

# VMTurbo Internal Docker Registry ca.crt
CA_CRT='H4sICPR7M1cCA2NhLmNydAB9lMnOq0YQRvc8RfboygYDxstuuoHGNKONwTvmwYAHbKanj/8oyiK5Si0/fVIdqY7q16/vQKwR6w8FeyeiEgWc8E/4i6GEIKNRFFBKJZgIBCUxgDNjwZ1gER4QBVtN8Z+aT5IdcjGE7hlQGM0qAj4srYCBgJ6UndFe+XbNdEM0L1ZFoRCiE+YpIovV4C1tXM4O7t/MXSlKZwv9k02M2uAzhUQD3BmDmZppF/RxaG3T/vaOeDyVJa7/TQG+FEAgEE1gYr6FI7h/0V3lw4NHWSHyURrvTo7dLk5rVHz07fPg9KTHY5fZuyLlcqV9cZWtkMvVihn88Vn0qO08lRFfLXJd57t8iHbAlu2PuEZvda6gXp9Mek8Niz+LbGfdHtZBQ0DBBzNhXMMFjXAzQxojJabK7uDNhU20nP8SPwpe3kvB0Npv7zUH+rFL1oaVuqxehPxVhLXoM2OrCnLeIAKLPh/HQRZPry1MSFGSTMjMKgM6FIplUObirBW1kKmN1od2RUPP1yOHMjdBUvrTsu8LfAsPmuMUV3kRk/Qas+wj6tuXuhwM8T7abo9oGbRVEJcLjgA8SonnxhvG0Jtr74NpTao9gHAx3fs01+XblG6QEz8cKCkEQGuyFXIUbn+ulaHSvUB4etvPUGd8Xhk4darGTZE8LzYQXd9/rFD7kUT3KXZWcCj/Lv+uyzwkHD9gOf2/D5qiuEj2bExByG3c9SvjzwKPYgaeAAKuvvmdsj+yABeyqfe0vfIhIiLVxw3VxIsx2sCv93X0ZoLhDSfuVr0STxZCKuTgEUhhYVygejxH0SyO3bY7slPC91BxzlOxHBc3lyTsqVKSLyLj0nsS7cEgZYPb0K6REFttnOvQvO6SfU/aI7A/ZiqgZ3Sdc7F9Bc4hdUVkiJQEt4G7MMjlzSE2MNs5A5sUIfZGQ+8aeAdbt51fyrGayFY2C5j2A463JL6th9s7Y2/rWW59DTOht0jG5pV9nLyalYYG0cjyqVbI1qTbT9Xa1pOeDqBJ/c4EOmAdBbQKZ5p+b8+wtD+MxpdyPbx0FdfzNVpHczTint0EVmybzr6je3KPtPIqJ+amqeVq2VscKy0Lx/z1TrCF/vti/gTxuiFQfwQAAA=='

# Directory that contains this script
BUILD_DIR="$(cd $(dirname -- "$0") >/dev/null; pwd)"

# Version in pom.xml
VERSION=$(grep -m 1 '<version>' "$BUILD_DIR/pom.xml" | egrep -o '[0-9]+\.[0-9]+(\.[0-9]+)?')

# SVN Revision
REVISION=$(svn info "$BUILD_DIR"/../ | egrep '^Revision: [0-9]+' | awk '{print $2}')

# Convert $CA_CRT into a file
base64 -d -w0 <<< "$CA_CRT" | gunzip > "$BUILD_DIR/ca.crt"

# Build the RPM
rpmbuild \
    -bb \
    -D "vers $VERSION" \
    -D "svn_revision $REVISION" \
    -D "_rpmdir "$BUILD_DIR"" \
    -D "_builddir "$BUILD_DIR"" \
    --quiet \
    VMTurbo.spec

# Remove the ca.crt file
rm ca.crt &>/dev/null

echo "VMTurbo RPM created here: $BUILD_DIR/noarch/VMTurbo-${VERSION}-${REVISION}.noarch.rpm"
