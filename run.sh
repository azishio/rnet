#! /bin/bash
set -eu

# download mokuroku.csv
curl -L https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/mokuroku.csv.gz | gzip -d > mokuroku.csv

# run docker container

docker run --rm --name rnet -v "${PWD}:/data" -it ghcr.io/azishio/rnet:main collect "$@"
docker run --rm --name rnet -v "${PWD}:/data" -it ghcr.io/azishio/rnet:main delaunay -i ./river_node.csv



