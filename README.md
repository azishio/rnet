国土地理院が公開する[mokurokuファイル](https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/mokuroku.csv.gz)
をもとに河川の情報を収集し、Neo4JにインポートするためのCSVファイルを作成します。

## 依存関係

Dockerがインストールされていることが前提です。

## 使い方

```bash
curl -o run_rnet.sh https://raw.githubusercontent.com/azishio/rnet/refs/heads/main/run.sh
bash ./run_rnet.sh
```

出力されるファイルは以下の通りです。

| ファイル名                        | 内容                    | 追加されるノードのラベル     | 追加されるリレーションシップのタイプ |
|------------------------------|-----------------------|------------------|--------------------|
| river_node.csv               | 河川の幾何学的特徴点            | RiverNode        |                    |
| river_link.csv               | 河川の幾何学的特徴点のつながり       |                  | RIVER_LINK         |
| tile.csv                     | river_nodeが存在するマップタイル | TileZ (Zはズームレベル) |                    |
| tile_family_relationship.csv | ズームレベルが異なるマップタイルの親子関係 |                  | CHILD              |
| tile_membership.csv          | 河川の幾何学的特徴点とタイルの関係     |                  | MEMBER             |
