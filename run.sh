#! /bin/bash
set -eu

# download mokuroku.csv
curl -L https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/mokuroku.csv.gz | gzip -d > mokuroku.csv

# build docker image
docker build -t rnet ghcr.io/azishio/rnet:latest

# run docker container

docker run rnet collect "$@"
docker run rnet delaunay -i ./river_node.csv



