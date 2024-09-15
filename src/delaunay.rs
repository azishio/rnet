use std::fs::{canonicalize, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use indicatif::ProgressBar;
use rayon::prelude::*;
use spade::{DelaunayTriangulation, HasPosition, Point2, Triangulation, validate_vertex};

struct RiverNode {
    pub id: u32,
    long: f64,
    lat: f64,
}

impl RiverNode {
    fn new(id: u32, long: f64, lat: f64) -> Self {
        Self { id, long, lat }
    }
}

impl HasPosition for RiverNode {
    type Scalar = f64;

    fn position(&self) -> Point2<Self::Scalar> {
        let point = Point2::new(self.long, self.lat);
        validate_vertex(&point).expect("Invalid vertex");
        point
    }
}

/// 河川データのノードを読み込む
fn read_nodes(nodes_path: PathBuf) -> Vec<RiverNode> {
    let file = OpenOptions::new()
        .read(true)
        .open(nodes_path)
        .unwrap();

    let reader = BufReader::new(file);

    // レコードの例
    // ex) 3412033,"{longitude:135.343717784783,latitude:35.1782983520012}",197.95,RiverNode

    reader
        .lines()
        // ヘッダーをスキップ
        .skip(1)
        .collect::<Vec<_>>()
        .into_par_iter()
        .filter_map(|line| {
            if let Ok(line) = line {
                // 空行を除外
                if line.is_empty() {
                    return None;
                }

                let mut iter = line.split(",");
                let hilbert = iter.next().unwrap().parse::<u32>().unwrap();


                let long = iter.next().unwrap().chars().filter(|&c| c.is_ascii_digit() || c == '.').collect::<String>().parse::<f64>().unwrap();
                let lat = iter.next().unwrap().chars().filter(|&c| c.is_ascii_digit() || c == '.').collect::<String>().parse::<f64>().unwrap();

                Some(RiverNode::new(hilbert, long, lat))
            } else {
                None
            }
        }).collect()
}

pub(crate) fn collect_delaunay(nodes_path: &String) {
    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));


    let nodes_path = canonicalize(nodes_path).expect("Failed to canonicalize the path");
    let result_path = nodes_path.with_file_name("delaunay.csv");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(result_path)
        .unwrap();


    spinner.set_message("Reading nodes...");
    let nodes = read_nodes(nodes_path);

    spinner.set_message("Calculating Delaunay triangulation...");
    let triangulation = DelaunayTriangulation::<RiverNode>::bulk_load(nodes).expect("Failed to create Delaunay triangulation");

    spinner.set_message("Writing result...");
    // ヘッダーを書き込む
    {
        let buf = [":START_ID", ":END_ID", ":TYPE"].join(",") + "\n";
        file.write_all(buf.as_bytes()).expect("Failed to write header");
        file.flush().expect("Failed to flush the file");
    }

    // 無向グラフのエッジを書き込む
    triangulation.undirected_edges().for_each(|edge| {
        let [n1, n2] = edge.vertices();
        let buf = [n1.data().id.to_string(), n2.data().id.to_string(), "DELAUNAY".to_string()].join(",") + "\n";

        file.write_all(buf.as_bytes()).expect("Failed to write edge");
    });

    file.flush().expect("Failed to flush the file");

    spinner.finish_with_message("Finished");
}
