use std::fs::canonicalize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use bitflags::{bitflags, Flags};
use coordinate_transformer::{ll2pixel, pixel2ll, ZoomLv};
use csv::Reader;
use futures::future;
use geojson::{FeatureCollection, JsonObject, Value};
use hilbert_index::ToHilbertIndex;
use image::ImageReader;
use indicatif::{ProgressBar, ProgressStyle};
use moka::future::Cache;
use polars::prelude::{CsvWriter, SerWriter, UniqueKeepStrategy};
use polars_lazy::prelude::{LazyCsvReader, LazyFileListReader};
use rayon::prelude::*;
use reqwest::Client;
use rustc_hash::FxBuildHasher;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::CollectArgs;

/// collectサブコマンド用の関数
pub async fn collect_river_data(args: &CollectArgs) {
    let spinner = ProgressBar::new_spinner();
    spinner.set_message("Initializing...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    // デフォルト値の設定
    let CollectArgs {
        mokuroku,
        batch: batch_size,
        line,
        category,
        river_base_url,
        dem_base_url,
        zoom_lv,
        aabb,
    } = args;
    let mokuroku = canonicalize(mokuroku).expect("Failed to canonicalize mokuroku file path");
    let rv_ctg_flags = Arc::new(parse_flag_list::<RvCtgFlags>(category));
    let rv_rcl_flags = Arc::new(parse_flag_list::<RvRclFlags>(line));
    let river_base_url = Arc::new(river_base_url.clone());
    let dem_base_url = Arc::new(dem_base_url.clone());
    let dem_zoom_lv = ZoomLv::parse(*zoom_lv).expect("Failed to parse ZoomLv");
    let aabb = aabb.clone().map(|s| s.parse::<AABB>().expect("Failed to parse AABB"));

    spinner.set_message("Reading mokuroku.csv...");
    let tiles = read_tile_list(&mokuroku, aabb);

    // 標高データのキャッシュ
    let altitude_cache = Cache::<(u32, u32), Arc<Vec<f32>>>::builder()
        .max_capacity(50)
        .build_with_hasher(FxBuildHasher);

    let nodes_path = mokuroku.with_file_name("river_node.csv");
    let links_path = mokuroku.with_file_name("river_link.csv");

    // ヘッダーの書き込み
    {
        spinner.set_message("Writing headers for nodes and links...");
        write_nodes_header(&nodes_path).await;
        write_link_header(&links_path).await;
        spinner.finish_and_clear();
    }

    let client = Client::new();

    // ProgressBarの設定
    let pb = ProgressBar::new(tiles.len() as u64);
    pb.set_message("Starting to process river center line tiles...");
    pb.set_style(
        ProgressStyle::with_template("{msg}\n[{elapsed_precise}] {wide_bar} {pos}/{len} ({eta_precise})")
            .unwrap(),
    );

    // バッチごとにタイルを処理
    for (i, batch) in tiles.chunks(*batch_size).enumerate() {
        let river_base_url = river_base_url.clone();
        let lines = fetch_ml(river_base_url, batch, *rv_rcl_flags, *rv_ctg_flags, &client).await;

        let links = collect_links(&lines);
        let nodes = collect_nodes(
            &lines,
            dem_base_url.clone(),
            dem_zoom_lv,
            altitude_cache.clone(),
            &client,
        )
            .await;

        write_nodes(&nodes_path, &nodes).await;
        write_links(&links_path, &links).await;

        pb.inc(batch.len() as u64);

        pb.set_message(format!(
            "Completed batch {} of {}",
            i + 1,
            tiles.len() / batch_size,
        ));
    }

    pb.finish_with_message("Finished processing all tiles!");

    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    // ノード情報の重複削除
    spinner.set_message("Deduplicating nodes...");
    deduplicate_nodes(&nodes_path);

    // 日本の緯度経度のAABBから4点を追記する
    spinner.set_message("Appending bounds...");
    append_bounds(nodes_path, aabb).await;
    spinner.finish_with_message("Process completed!");
}

bitflags! {
    /// 河川中心線の種別
    #[derive(Copy, Clone)]
    struct RvRclFlags: u16 {
        const SMALL_NORMAL = 0b0000000000000001;
        const SMALL_DRY = 0b0000000000000010;
        const NORMAL = 0b0000000000000100;
        const DRY = 0b0000000000001000;
        const ARTIFICIAL_OPEN = 0b0000000000010000;
        const ARTIFICIAL_UNDERGROUND = 0b0000000000100000;
        const WATERWAY = 0b0000000001000000;
        const OTHER = 0b0000000010000000;
        const UNKNOWN = 0b0000000100000000;
    }
}

impl FromStr for RvRclFlags {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // geojson内の表記
            "細河川（通常部）" => Ok(Self::SMALL_NORMAL),
            "細河川（枯れ川部）" => Ok(Self::SMALL_DRY),
            "河川中心線（通常部）" => Ok(Self::NORMAL),
            "河川中心線（枯れ川部）" => Ok(Self::DRY),
            "人工水路（空間）" => Ok(Self::ARTIFICIAL_OPEN),
            "人工水路（地下）" => Ok(Self::ARTIFICIAL_UNDERGROUND),
            "用水路" => Ok(Self::WATERWAY),
            "その他" => Ok(Self::OTHER),
            "不明" => Ok(Self::UNKNOWN),
            "" => Ok(Self::UNKNOWN),
            // コマンドラインオプションの短縮形
            "sn" => Ok(Self::SMALL_NORMAL),
            "sd" => Ok(Self::SMALL_DRY),
            "n" => Ok(Self::NORMAL),
            "d" => Ok(Self::DRY),
            "ao" => Ok(Self::ARTIFICIAL_OPEN),
            "au" => Ok(Self::ARTIFICIAL_UNDERGROUND),
            "w" => Ok(Self::WATERWAY),
            "o" => Ok(Self::OTHER),
            "u" => Ok(Self::UNKNOWN),
            "all" => Ok(Self::all()),
            _ => Err(anyhow!("Failed to parse RvRclType from string: {:?}", s)),
        }
    }
}

bitflags! {
    /// 河川のカテゴリ
    #[derive(Copy, Clone)]
    struct RvCtgFlags: u8 {
        const PRIMARY = 0b00000001;
        const SECONDARY = 0b00000010;
        const QUASI = 0b00000100;
        const REGULAR = 0b00001000;
        const OTHER = 0b00010000;
        const UNKNOWN = 0b00100000;
    }
}

impl FromStr for RvCtgFlags {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // geojson内の表記
            "一級河川" => Ok(Self::PRIMARY),
            "二級河川" => Ok(Self::SECONDARY),
            "準用河川" => Ok(Self::QUASI),
            "普通河川" => Ok(Self::REGULAR),
            "その他" => Ok(Self::OTHER),
            "不明" => Ok(Self::UNKNOWN),
            "" => Ok(Self::UNKNOWN),
            // コマンドラインオプションの短縮形
            "p" => Ok(Self::PRIMARY),
            "s" => Ok(Self::SECONDARY),
            "q" => Ok(Self::QUASI),
            "r" => Ok(Self::REGULAR),
            "o" => Ok(Self::OTHER),
            "u" => Ok(Self::UNKNOWN),
            "all" => Ok(Self::all()),
            _ => Err(anyhow!("Failed to parse RivCtg from string: {:?}", s)),
        }
    }
}

/// コンマで区切られた文字列からフラグをパース
fn parse_flag_list<T: FromStr + Flags>(s: &str) -> T {
    let list = s.split(",").map(|s| {
        s.parse::<T>()
            .unwrap_or_else(|_| panic!("Failed to parse flag from string: {:?}", s))
    });

    list.fold(T::empty(), |acc, x| acc.union(x))
}

/// タイルをフェッチする範囲を表す
#[derive(Debug, Clone, Copy)]
struct AABB {
    min_long: f64,
    max_long: f64,
    min_lat: f64,
    max_lat: f64,
}

impl FromStr for AABB {
    type Err = anyhow::Error;

    ///　コンマで区切られた文字列からAABBをパース(min_long,max_long,min_lat,max_lat)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split(",");

        let min_long = iter.next().unwrap().parse::<f64>()?;
        let max_long = iter.next().unwrap().parse::<f64>()?;
        let min_lat = iter.next().unwrap().parse::<f64>()?;
        let max_lat = iter.next().unwrap().parse::<f64>()?;

        if min_lat >= max_lat || min_long >= max_long {
            return Err(anyhow!("Invalid AABB: {:?}", s));
        }

        Ok(Self {
            min_long,
            max_long,
            min_lat,
            max_lat,
        })
    }
}

impl Default for AABB {
    /// 日本の緯度経度のAABB
    fn default() -> Self {
        Self {
            min_long: 153. + 59. / 60. + 19. / 3600.,
            max_long: 122. + 55. / 60. + 57. / 3600.,
            min_lat: 45. + 33. / 60. + 26. / 3600.,
            max_lat: 20. + 25. / 60. + 31. / 3600.,
        }
    }
}

/// CSVファイルからタイルリストを読み込む
/// タイルのURLの後半部分のみを格納したリストを返す
/// 例: https://example.com/{z}/{x}/{y}.geojson -> {z}/{x}/{y}.geojson
fn read_tile_list(path: &Path, aabb: Option<AABB>) -> Vec<String> {
    let tile_list = Reader::from_path(path)
        .unwrap_or_else(|_| panic!("Failed to read mokuroku CSV file at {:?}", path))
        .into_records()
        .filter_map(|record| {
            let record = record.ok()?;
            let url = record.get(0)?;
            if url.chars().next()?.is_ascii_digit() {
                Some(url.to_string())
            } else {
                None
            }
        });

    if let Some(aabb) = aabb {
        tile_list
            .filter(|url| {
                let url = url.as_str();
                let mut zxy = url.split(".").next().unwrap().split('/');

                let z = zxy.next().unwrap().parse::<ZoomLv>().unwrap();
                let tile_x = zxy.next().unwrap().parse::<u32>().unwrap();
                let tile_y = zxy.next().unwrap().parse::<u32>().unwrap();

                let (pixel_x, pixel_y) = (tile_x * 256, tile_y * 256);

                let (long, lat) = pixel2ll((pixel_x, pixel_y), z);
                let (long, lat) = (long.to_degrees(), lat.to_degrees());

                aabb.min_long <= long && long <= aabb.max_long && aabb.min_lat <= lat && lat <= aabb.max_lat
            })
            .collect()
    } else {
        tile_list.collect()
    }
}

/// geojsonのプロパティからRvRclTypeとRivCtgを読み込む
fn read_property(p: JsonObject) -> (RvRclFlags, RvCtgFlags) {
    let rv_rcl_type = p
        .get("type")
        .unwrap_or_else(|| panic!("Failed to get \"type\" property from JSON object: {:?}", p))
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "Failed to parse \"type\" property as string from JSON object: {:?}",
                p
            )
        })
        .parse::<RvRclFlags>()
        .unwrap_or_else(|_| {
            panic!(
                "Failed to parse \"type\" property as RvRclType from JSON object: {:?}",
                p
            )
        });
    let riv_ctg = p
        .get("rivCtg")
        .unwrap_or_else(|| {
            panic!(
                "Failed to get \"category\" property from JSON object: {:?}",
                p
            )
        })
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "Failed to parse \"category\" property as string from JSON object: {:?}",
                p
            )
        })
        .parse::<RvCtgFlags>()
        .unwrap_or_else(|_| {
            panic!(
                "Failed to parse \"category\" property as RivCtg from JSON object: {:?}",
                p
            )
        });

    (rv_rcl_type, riv_ctg)
}

/// ヒルベルトインデックスを計算
pub fn calc_hilbert_index(long: f64, lat: f64) -> u32 {
    let (x, y) = ll2pixel((long.to_radians(), lat.to_radians()), ZoomLv::Lv18);
    let h = [x as usize, y as usize].to_hilbert_index(26);
    h as u32
}

/// 2地点間のハヴァーサイン距離を計算
fn haversine_distance(long1: f64, lat1: f64, long2: f64, lat2: f64) -> f64 {
    let d_long = long2 - long1;
    let d_lat = lat2 - lat1;
    let a = (d_lat / 2.).sin().powi(2);
    let b = lat1.cos() * lat2.cos() * (d_long / 2.).sin().powi(2);
    let r = 6371.;
    2. * r * (a + b).sqrt().asin()
}

/// (ヒルベルト値, 経度, 緯度, 標高)
type RiverNode = (u32, f64, f64, f32);

/// Vec<(ヒルベルト値, 経度, 緯度)
type FetchedLine = Vec<(u32, f64, f64)>;

/// 主線のフェッチとフィルタリング
async fn fetch_ml(
    river_base_url: Arc<String>,
    url_part_list: &[String],
    rv_rcl_flags: RvRclFlags,
    river_flags: RvCtgFlags,
    client: &Client,
) -> Vec<FetchedLine> {
    let futures = url_part_list
        .iter()
        .map(|url_part| {
            let river_base_url = river_base_url.clone();
            async move {
                let url = format!("{river_base_url}{url_part}");
                let client = client.clone();

                let res = client.get(&url).send().await.unwrap_or_else(|e| {
                    panic!("Failed to fetch tile data from URL: {}: {:#?}.", url, e, )
                });

                let body = res.text().await.unwrap_or_else(|e| {
                    panic!(
                        "Failed to parse response body as text from URL: {}: {:#?}",
                        url, e
                    )
                });

                let geojson = body.parse::<geojson::GeoJson>().unwrap_or_else(|e| {
                    panic!(
                        "Failed to parse GeoJSON from response at URL: {}: {:#?}",
                        url, e
                    )
                });

                let fc = FeatureCollection::try_from(geojson).unwrap_or_else(|e| {
                    panic!(
                        "Failed to convert GeoJSON to FeatureCollection at URL: {}: {:#?}",
                        url, e
                    )
                });

                fc.features
                    .into_iter()
                    .filter_map(move |f| {
                        let p = f.properties.unwrap_or_else(|| {
                            panic!(
                                "Failed to get properties from GeoJSON feature at URL: {}",
                                url
                            )
                        });
                        let (rv_rcl_type, riv_ctg) = read_property(p);

                        if !rv_rcl_flags.contains(rv_rcl_type) || !river_flags.contains(riv_ctg) {
                            return None;
                        }

                        let line = match f.geometry.unwrap().value {
                            Value::LineString(v) => {
                                v
                                    .into_iter()
                                    .map(|p| {
                                        let long = p[0];
                                        let lat = p[1];

                                        let h =
                                            calc_hilbert_index(long, lat);

                                        (h, long, lat)
                                    })
                                    .collect::<Vec<_>>()
                            }
                            _ => unreachable!(),
                        };

                        Some(line)
                    })
                    .collect::<Vec<_>>()
            }
        })
        .collect::<Vec<_>>();

    let result = future::join_all(futures).await;

    result.into_iter().flatten().collect()
}

/// (StartID, EndID, Distance)
type Link = (u32, u32, f64);

/// フェッチした中心線情報から繋がりを収集
fn collect_links(lines: &Vec<FetchedLine>) -> Vec<Link> {
    lines
        .into_par_iter()
        .flat_map(|line| {
            line.windows(2)
                .map(|link| {
                    let (id1, long1, lat1) = link[0];
                    let (id2, long2, lat2) = link[1];

                    let dist = haversine_distance(
                        long1.to_radians(),
                        lat1.to_radians(),
                        long2.to_radians(),
                        lat2.to_radians(),
                    );

                    (id1, id2, dist)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

/// フェッチした中心線情報からノード情報を収集
async fn collect_nodes(
    lines: &Vec<FetchedLine>,
    dem_base_url: Arc<String>,
    dem_zoom_lv: ZoomLv,
    cache: Cache<(u32, u32), Arc<Vec<f32>>, FxBuildHasher>,
    client: &Client,
) -> Vec<RiverNode> {
    let futures = lines
        .into_par_iter()
        .flat_map(|line| {
            line.into_par_iter().map(|n| async {
                let (h, long, lat) = n;
                let pixel_coord = ll2pixel((long.to_radians(), lat.to_radians()), dem_zoom_lv);
                let tile_coord = (pixel_coord.0 / 256, pixel_coord.1 / 256);

                let altitude_map = cache
                    .entry(tile_coord)
                    .or_insert_with(async {
                        let z = dem_zoom_lv as u8;
                        let (tile_x, tile_y) = tile_coord;
                        // 産総研のシームレス標高タイルの仕様に合わせる
                        let url = format!("{dem_base_url}{z}/{tile_y}/{tile_x}.png");

                        let res = client.get(&url).send().await.unwrap_or_else(|e| {
                            panic!("Failed to fetch DEM tile data from URL: {}: {:#?}", url, e)
                        });

                        let bytes = res.bytes().await.unwrap_or_else(|e| {
                            panic!(
                                "Failed to parse response body as bytes from URL: {}: {:#?}",
                                url, e
                            )
                        });

                        let altitudes = ImageReader::new(std::io::Cursor::new(bytes))
                            .with_guessed_format()
                            .unwrap_or_else(|e| {
                                panic!(
                                    "Failed to guess image format from bytes at URL: {}: {:#?}",
                                    url, e
                                )
                            })
                            .decode()
                            .map(|image| {
                                image
                                    .into_rgb8()
                                    .pixels()
                                    .map(|color| {
                                        let r = color[0] as f64;
                                        let g = color[1] as f64;
                                        let b = color[2] as f64;

                                        let x = 2_f64.powi(16) * r + 2_f64.powi(8) * g + b;
                                        let u = 0.01;

                                        (if x < 2_f64.powi(23) {
                                            x * u
                                        } else if x > 2_f64.powi(23) {
                                            (x - 2_f64.powi(24)) * u
                                        } else {
                                            0.
                                        }) as f32
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_else(|_| vec![0.; 256 * 256]);

                        Arc::new(altitudes)
                    })
                    .await;
                let altitude_map = altitude_map.value();

                let (local_x, local_y) = (pixel_coord.0 % 256, pixel_coord.1 % 256);
                let altitude = altitude_map[(local_y * 256 + local_x) as usize];

                let node: RiverNode = (*h, *long, *lat, altitude);
                node
            })
        })
        .collect::<Vec<_>>();

    future::join_all(futures).await
}

/// ヘッダーの書き込み
async fn write_nodes_header(path: &Path) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .expect("Failed to create river_node.csv");

    let header = [
        "hilbert18:ID",
        "location:point{crs:WGS-84}",
        "altitude",
        ":LABEL",
    ]
        .join(",")
        + "\n";

    file.write_all(header.as_ref())
        .await
        .expect("Failed to write header to river_node.csv");
    file.flush().await.expect("Failed to flush river_node.csv");
}

/// ノード情報の書き込み
async fn write_nodes(path: &Path, lines: &[RiverNode]) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .expect("Failed to create river_node.csv");

    let buf = lines
        .iter()
        .map(|(id, long, lat, altitude)| {
            let location = format!("\"{{longitude:{long},latitude:{lat}}}\"");
            [
                id.to_string(),
                location,
                altitude.to_string(),
                "RiverNode".to_string(),
            ]
                .join(",")
                + "\n"
        })
        .collect::<Vec<_>>()
        .concat();

    file.write_all(buf.as_ref())
        .await
        .expect("Failed to write river_node.csv");
    file.flush().await.expect("Failed to flush river_node.csv");
}

/// ヘッダーの書き込み
async fn write_link_header(path: &Path) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .expect("Failed to create river_link.csv");

    let header = [":START_ID", ":END_ID", ":TYPE", "length"].join(",") + "\n";

    file.write_all(header.as_ref())
        .await
        .expect("Failed to write header to river_link.csv");
    file.flush()
        .await
        .expect("Failed to flush river_link.csv");
}

/// リレーション情報の書き込み
async fn write_links(path: &Path, lines: &[Link]) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)
        .await
        .expect("Failed to create river_link.csv");

    let buf = lines
        .iter()
        .map(|(id1, id2, dist)| {
            [
                id1.to_string(),
                id2.to_string(),
                "RIVER_LINK".to_string(),
                dist.to_string(),
            ]
                .join(",")
                + "\n"
        })
        .collect::<Vec<_>>()
        .concat();

    file.write_all(buf.as_ref())
        .await
        .expect("Failed to write river_link.csv");
    file.flush()
        .await
        .expect("Failed to flush river_link.csv");
}

/// ノード情報の重複削除
fn deduplicate_nodes(nodes_path: &Path) {
    let mut df_deduplicated = LazyCsvReader::new(nodes_path)
        .with_has_header(true)
        .finish()
        .expect("Failed to read river_node.csv")
        .unique(
            Some(vec!["hilbert18:ID".to_string()]),
            UniqueKeepStrategy::Any,
        )
        .collect()
        .expect("Failed to deduplicate river_node.csv");

    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(nodes_path)
        .expect("Failed to create river_node.csv");
    let buf = std::io::BufWriter::new(file);

    CsvWriter::new(buf)
        .finish(&mut df_deduplicated)
        .expect("Failed to write river_node.csv");
}

// AABBから4点を追記する
async fn append_bounds(path: PathBuf, aabb: Option<AABB>) {
    let AABB {
        min_long,
        max_long,
        min_lat,
        max_lat,
    } = aabb.unwrap_or_default();

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .expect("Failed to create river_node.csv");
    let buf =
        [
            (calc_hilbert_index(max_long, max_lat), min_long, min_lat, 0.),
            (calc_hilbert_index(min_long, max_lat), max_long, min_lat, 0.),
            (calc_hilbert_index(max_long, min_lat), min_long, max_lat, 0.),
            (calc_hilbert_index(min_long, min_lat), max_long, max_lat, 0.),
        ]
            .iter()
            .map(|(id, long, lat, altitude)| {
                let location = format!("\"{{longitude:{long},latitude:{lat}}}\"");
                [
                    id.to_string(),
                    location,
                    altitude.to_string(),
                    "BoundNode".to_string(),
                ]
                    .join(",")
                    + "\n"
            })
            .collect::<Vec<_>>()
            .concat();

    file.write_all(buf.as_ref())
        .await
        .expect("Failed to write river_node.csv");
    file.flush().await.expect("Failed to flush river_node.csv");
}
