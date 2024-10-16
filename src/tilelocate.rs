use coordinate_transformer::{ll2pixel, ZoomLv};
use indicatif::ProgressBar;
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use spade::{validate_vertex, DelaunayTriangulation, HasPosition, Point2, Triangulation};
use std::collections::{HashMap, HashSet};
use std::fs::{canonicalize, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
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

pub(crate) fn tile_locator(nodes_path: &String, max_zoomlv: ZoomLv) {
    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    let nodes_path = canonicalize(nodes_path).expect("Failed to canonicalize the path");
    let tile_list_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(nodes_path.with_file_name("tiles.csv"))
        .unwrap();
    let mut tiles_file = BufWriter::new(tile_list_file);
    let tile_family_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(nodes_path.with_file_name("tile_family_relationship.csv"))
        .unwrap();
    let mut tile_family_file = BufWriter::new(tile_family_file);
    let tile_membership_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(nodes_path.with_file_name("tile_membership.csv"))
        .unwrap();
    let mut tile_membership_file = BufWriter::new(tile_membership_file);

    spinner.set_message("Reading nodes...");
    let nodes = read_nodes(nodes_path);

    spinner.set_message("Calculating Delaunay triangulation...");
    let triangulation = DelaunayTriangulation::<RiverNode>::bulk_load(nodes).expect("Failed to create Delaunay triangulation");


    // HashMap<(タイルX, タイルY), Vec<ノードID>>を作成
    let mut tile_and_node = HashMap::<(u32, u32), Vec<u32>, FxBuildHasher>::with_hasher(FxBuildHasher::default());

    // タイルに三角形がかぶっていたら、その三角形のノードIDを記録
    triangulation.inner_faces().for_each(|face| {
        let tri_vertices = face.vertices().map(|v| {
            let ll_rad = (v.data().long.to_radians(), v.data().lat.to_radians());
            ll2pixel(ll_rad, max_zoomlv)
        });
        let ids = face.vertices().map(|v| v.data().id);
        // ピクセル座標のAABB
        let (max_x, min_x, max_y, min_y) = {
            let max_pixel = tri_vertices.iter().fold((0, 0), |acc, v| (acc.0.max(v.0), acc.1.max(v.1)));
            let min_pixel = tri_vertices.iter().fold((u32::MAX, u32::MAX), |acc, v| (acc.0.min(v.0), acc.1.min(v.1)));

            (max_pixel.0, min_pixel.0, max_pixel.1, min_pixel.1)
        };

        let check_tile_list = {
            // タイル座標のAABB
            let (max_tile_x, min_tile_x, max_tile_y, min_tile_y) = (max_x / 256, min_x / 256, max_y / 256, min_y / 256);

            (min_tile_x..=max_tile_x)
                .flat_map(move |x| (min_tile_y..=max_tile_y).map(move |y| (x, y)))
        };

        check_tile_list.for_each(|(tile_x, tile_y)| {
            // タイルの4頂点のピクセル座標のリスト
            let tile_aabb = [(tile_x, tile_y), (tile_x + 1, tile_y), (tile_x, tile_y + 1), (tile_x + 1, tile_y + 1)]
                .map(|(x, y)| (x * 256, y * 256));

            fn cross_product(p1: (u32, u32), p2: (u32, u32), p: (u32, u32)) -> i64 {
                (p2.0 as i64 - p1.0 as i64) * (p.1 as i64 - p1.1 as i64) - (p2.1 as i64 - p1.1 as i64) * (p.0 as i64 - p1.0 as i64)
            }

            // タイル4頂点のうち、一つでも三角形の中にあれば、その三角形はタイルに含まれる
            let is_contained = tile_aabb.iter().any(|p| {
                let cross1 = cross_product(tri_vertices[0], tri_vertices[1], *p);
                let cross2 = cross_product(tri_vertices[1], tri_vertices[2], *p);
                let cross3 = cross_product(tri_vertices[2], tri_vertices[0], *p);

                (cross1 >= 0 && cross2 >= 0 && cross3 >= 0) || (cross1 <= 0 && cross2 <= 0 && cross3 <= 0)
            });

            if is_contained {
                let entry = tile_and_node.entry((tile_x, tile_y)).or_default();
                let ids = face.vertices().map(|v| v.data().id);
                entry.extend(ids);
            }
        });
    });

    {
        // ヘッダーを書き込む
        let buf = [":START_ID", ":END_ID", ":TYPE"].join(",") + "\n";
        tile_membership_file.write_all(buf.as_bytes()).expect("Failed to write header");


        tile_and_node.iter().for_each(|(tile, nodes)| {
            nodes.iter().for_each(|node| {
                let tile_id = format!("{}-{}-{}", tile.0, tile.1, max_zoomlv as u32);
                let node_id = node.to_string();

                let buf = [tile_id, node_id, "MEMBER".to_string()].join(",") + "\n";
                tile_membership_file.write_all(buf.as_bytes()).expect("Failed to write edge");
            });
        });

        tile_membership_file.flush().expect("Failed to flush the file");
    }

    // 現在のズームレベルのタイルから、ズームレベルが1つ上のタイルを計算し、ズームレベルが0になるまで繰り返す
    {
        // ヘッダーを書き込む
        let buf = [":START_ID", ":END_ID", ":TYPE"].join(",") + "\n";
        tile_family_file.write_all(buf.as_bytes()).expect("Failed to write header");

        let buf = [":ID", ":LABEL", "x:int", "y:int"].join(",") + "\n";
        tiles_file.write_all(buf.as_bytes()).expect("Failed to write header");

        let mut tiles = HashSet::<(u32, u32), FxBuildHasher>::from_iter(tile_and_node.keys().map(|(x, y)| (*x, *y)));
        let mut parent_tiles = HashSet::<(u32, u32), FxBuildHasher>::with_hasher(FxBuildHasher);

        tiles.iter().for_each(|(x, y)| {
            let tile_id = format!("{}-{}-{}", x, y, max_zoomlv as u32);
            let label = format!("Tile{}", max_zoomlv as u32);
            let buf = [tile_id, label, x.to_string(), y.to_string()].join(",") + "\n";
            tiles_file.write_all(buf.as_bytes()).expect("Failed to write edge");
        });

        (1..=max_zoomlv as u32).rev().for_each(|zoom| {
            tiles.iter().for_each(|(x, y)| {
                let tile_id = format!("{}-{}-{}", x, y, zoom);

                let parent_tile = (x / 2, y / 2);
                let parent_tile_id = format!("{}-{}-{}", parent_tile.0, parent_tile.1, zoom - 1);
                parent_tiles.insert(parent_tile);

                let buf = [parent_tile_id, tile_id, "CHILD".to_string()].join(",") + "\n";
                tile_family_file.write_all(buf.as_bytes()).expect("Failed to write edge");
            });

            parent_tiles.iter().for_each(|(x, y)| {
                let tile_id = format!("{}-{}-{}", x, y, zoom - 1);
                let label = format!("Tile{}", zoom - 1);
                let buf = [tile_id, label, x.to_string(), y.to_string()].join(",") + "\n";
                tiles_file.write_all(buf.as_bytes()).expect("Failed to write edge");
            });

            tiles = parent_tiles.clone();
            parent_tiles.clear();
        })
    }
}
