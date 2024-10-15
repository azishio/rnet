国土地理院が公開する[mokurokuファイル](https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/mokuroku.csv.gz)
をもとに河川の情報を収集し、Neo4JにインポートするためのCSVファイルを作成します。

## 依存関係

Dockerがインストールされていることが前提です。

## 使い方

```bash
curl -o run_rnet.sh https://raw.githubusercontent.com/azishio/rnet/refs/heads/main/run.sh
bash ./run_rnet.sh
```
