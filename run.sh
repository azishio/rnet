#! /bin/bash
set -eu

# download mokuroku.csv
if [ ! -f mokuroku.csv ]; then
  curl -L https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/mokuroku.csv.gz | gzip -d > mokuroku.csv
fi

# run docker container

docker run --rm --name rnet -v "${PWD}:/data" -it ghcr.io/azishio/rnet:main collect "$@"
# -h や --help出会った場合は、ヘルプを表示して終了
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  exit 0
fi

docker run --rm --name rnet -v "${PWD}:/data" -it ghcr.io/azishio/rnet:main delaunay -i ./river_node.csv



