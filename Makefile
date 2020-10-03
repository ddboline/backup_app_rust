version = $(shell awk '/^version/' Cargo.toml | head -n1 | cut -d "=" -f 2 | sed 's: ::g')
release := "1"
uniq := $(shell head -c1000 /dev/urandom | sha512sum | head -c 12 ; echo ;)
cidfile := "/tmp/.tmp.docker.$(uniq)"
build_type := release

all:
	mkdir -p build/ && \
	cp Dockerfile.ubuntu20.04 build/Dockerfile && \
	cp -a Cargo.toml src scripts Makefile build/ && \
	cd build && \
	docker build -t backup_app_rust/build_rust:ubuntu20.04 . && \
	cd ../ && \
	rm -rf build/

cleanup:
	docker rmi `docker images | python -c "import sys; print('\n'.join(l.split()[2] for l in sys.stdin if '<none>' in l))"`
	rm -rf /tmp/.tmp.docker.backup_app_rust
	rm Dockerfile

package:
	docker run --cidfile $(cidfile) -v `pwd`/target:/backup_app_rust/target backup_app_rust/build_rust:ubuntu20.04 /backup_app_rust/scripts/build_deb_docker.sh $(version) $(release)
	docker cp `cat $(cidfile)`:/backup_app_rust/backup-app-rust_$(version)-$(release)_amd64.deb .
	docker rm `cat $(cidfile)`
	rm $(cidfile)

install:
	cp target/$(build_type)/backup-app-rust /usr/bin/backup-app-rust

pull:
	`aws ecr --region us-east-1 get-login --no-include-email`
	docker pull 281914939654.dkr.ecr.us-east-1.amazonaws.com/rust_stable:latest
	docker tag 281914939654.dkr.ecr.us-east-1.amazonaws.com/rust_stable:latest rust_stable:latest
	docker rmi 281914939654.dkr.ecr.us-east-1.amazonaws.com/rust_stable:latest
