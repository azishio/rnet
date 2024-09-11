use clap::{Parser, Subcommand};

use crate::collect::collect_river_data;

mod collect;
mod delaunay;

/// メインコマンドの構造体
#[derive(Parser, Debug)]
#[command(name = "MyApp")]
#[command(about = "CLI tool to manage river data operations")]
struct Cli {
    /// サブコマンドを指定する
    #[command(subcommand)]
    command: Commands,
}

/// サブコマンドを定義する構造体
#[derive(Subcommand, Debug)]
enum Commands {
    /// 河川データを収集し、書き出す
    Collect(CollectArgs),
    ///ドロネー三角分割を行った際のノード間のつながりを書き出す
    Delaunay {
        /// 河川データのriver_node.csvのパス
        #[arg(short, long)]
        input: String
    },
}

/// `collect` サブコマンドの引数を定義する構造体
#[derive(Parser, Debug)]
struct CollectArgs {
    /// 河川データの目録ファイルのパス
    #[arg(short, long, default_value = "./mokuroku.csv")]
    mokuroku: String,

    /// 処理のバッチサイズ
    #[arg(short, long, default_value_t = 100)]
    batch: usize,

    /// Line flag for the river data
    /// 収集する河川中心線の種別
    #[arg(short, long, default_value = "sn,sd,n,d,ao,w,o,u")]
    line: String,

    /// 収集する河川のカテゴリ
    #[arg(short, long, default_value = "all")]
    category: String,

    /// 河川データのベースURL
    #[arg(short, long, default_value = "https://cyberjapandata.gsi.go.jp/xyz/experimental_rvrcl/")]
    river_base_url: String,

    /// DEMデータのベースURL
    #[arg(short, long, default_value = "https://tiles.gsj.jp/tiles/elev/land/")]
    dem_base_url: String,

    /// 標高を検索する際に参照するDEMデータのズームレベル
    #[arg(short, long, default_value_t = 14)]
    zoom_lv: u8,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse(); // コマンドライン引数をパース

    match &cli.command {
        Commands::Collect(args) => collect_river_data(args).await, // collectサブコマンドが呼ばれた場合
        Commands::Delaunay { input } => delaunay::collect_delaunay(input), // delaunayサブコマンドが呼ばれた場合
    }
}

