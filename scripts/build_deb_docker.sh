#!/bin/bash

VERSION="$1"
RELEASE="$2"

. ~/.cargo/env

cargo build --release

printf "Backup utility written in rust\n" > description-pak
checkinstall --pkgversion ${VERSION} --pkgrelease ${RELEASE} -y
