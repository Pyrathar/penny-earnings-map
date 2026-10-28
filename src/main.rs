//! penny-earnings-map: sub-dollar US stocks with their latest two quarters, grouped by industry.
//!
//!   ingest [--limit N] [--min-sleep-ms 1100]   crawl the Finnhub universe into SQLite
//!   serve  [--port 8200]                       serve the JSON API and the static frontend
//!
//! Finnhub's free tier allows 60 requests a minute, so ingest is resumable: it prices symbols
//! that have no price yet and exits when the budget or the limit is reached. Running it from
//! cron fills the map over time instead of needing one long crawl to survive.

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::env;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

const KEY: &str = "dbgkorpr01qj6jgoiht0dbgkorpr01qj6jgoihtg";
const API: &str = "https://finnhub.io/api/v1";
const UA: &str = "penny-earnings-map/0.1 (research; razdiel@localhost)";

#[derive(Serialize)]
struct Stock {
    symbol: String,
    name: String,
    industry: String,
    price: f64,
    currency: String,
    q1_revenue: Option<f64>,
    q2_revenue: Option<f64>,
    q1_income: Option<f64>,
    q2_income: Option<f64>,
    revenue_growth: Option<f64>,
    income_growth: Option<f64>,
    q1_label: String,
    q2_label: String,
}

fn db_path() -> PathBuf {
    let base = env::var("PENNY_DB").unwrap_or_else(|_| "data/penny.db".to_string());
    PathBuf::from(base)
}

fn open_db() -> rusqlite::Result<Connection> {
    if let Some(dir) = db_path().parent() {
        let _ = fs::create_dir_all(dir);
    }
    let c = Connection::open(db_path())?;
    c.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS companies (
           symbol TEXT PRIMARY KEY, name TEXT, industry TEXT, price REAL, currency TEXT,
           priced_at TEXT, detailed_at TEXT);
         CREATE TABLE IF NOT EXISTS quarters (
           symbol TEXT, fiscal_year INTEGER, quarter INTEGER, revenue REAL, net_income REAL,
           form TEXT, filed TEXT, PRIMARY KEY (symbol, fiscal_year, quarter));
         CREATE INDEX IF NOT EXISTS idx_quarters_symbol ON quarters(symbol);",
    )?;
    Ok(c)
}

fn get_json(url: &str) -> Option<serde_json::Value> {
    match ureq::get(url)
        .set("User-Agent", UA)
        .set("Accept", "application/json")
        .timeout(Duration::from_secs(20))
        .call()
    {
        Ok(resp) => resp.into_json::<serde_json::Value>().ok(),
        Err(e) => {
            eprintln!("  ! {url}: {e}");
            None
        }
    }
}

fn api(path: &str) -> String {
    format!("{API}/{path}{}token={KEY}", if path.contains('?') { "&" } else { "?" })
}

fn get_text(url: &str) -> Option<String> {
    match ureq::get(url)
        .set("User-Agent", UA)
        .timeout(Duration::from_secs(30))
        .call()
    {
        Ok(resp) => resp.into_string().ok(),
        Err(e) => {
            eprintln!("  ! {url}: {e}");
            None
        }
    }
}

/// The free symbol list answers with a small HTML page that links to the real JSON file, so the
/// link is followed rather than assuming the endpoint returns JSON.
fn fetch_symbols() -> Option<serde_json::Value> {
    let body = get_text(&api("stock/symbol?exchange=US"))?;
    let trimmed = body.trim();
    if trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).ok();
    }
    let start = trimmed.find("href=\"")? + 6;
    let end = start + trimmed[start..].find('"')?;
    let url = trimmed[start..end].replace("&amp;", "&");
    let text = get_text(&url)?;
    serde_json::from_str(&text).ok()
}

// ------------------------------------------------------------------ ingest

fn ingest(limit: usize, sleep_ms: u64) {
    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("db: {e}");
            return;
        }
    };

    // 1. the symbol universe
    println!("fetching US symbol list");
    let symbols = match fetch_symbols() {
        Some(v) => v,
        None => {
            eprintln!("could not fetch the symbol list");
            return;
        }
    };
    let arr = symbols.as_array().cloned().unwrap_or_default();
    let mut common = 0;
    {
        let tx = conn.transaction().expect("tx");
        for s in &arr {
            let symbol = s["symbol"].as_str().unwrap_or("");
            let kind = s["type"].as_str().unwrap_or("");
            if symbol.is_empty() || !kind.eq_ignore_ascii_case("common stock") {
                continue;
            }
            let name = s["description"].as_str().unwrap_or(symbol);
            tx.execute(
                "INSERT OR IGNORE INTO companies (symbol, name) VALUES (?1, ?2)",
                params![symbol, name],
            )
            .ok();
            common += 1;
        }
        tx.commit().ok();
    }
    println!("common stocks in the list: {common}");

    // 2. price whatever has no price yet, oldest first
    let pending: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare("SELECT symbol, name FROM companies WHERE price IS NULL ORDER BY rowid")
            .expect("stmt");
        stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .expect("query")
            .filter_map(Result::ok)
            .collect()
    };
    println!("unpriced symbols: {}", pending.len());

    let mut done = 0usize;
    for (symbol, name) in pending.iter().take(limit) {
        let q = match get_json(&api(&format!("quote?symbol={symbol}"))) {
            Some(v) => v,
            None => {
                thread::sleep(Duration::from_millis(sleep_ms));
                continue;
            }
        };
        let price = q["c"].as_f64().unwrap_or(0.0);
        if price <= 0.0 {
            // no quote (delisted, halted, or empty); record it as zero so it is not retried
            conn.execute(
                "UPDATE companies SET price=0, priced_at=datetime('now') WHERE symbol=?1",
                params![symbol],
            )
            .ok();
            thread::sleep(Duration::from_millis(sleep_ms));
            continue;
        }
        conn.execute(
            "UPDATE companies SET price=?1, priced_at=datetime('now') WHERE symbol=?2",
            params![price, symbol],
        )
        .ok();
        done += 1;
        if price < 1.0 {
            println!("  under a dollar: {symbol} {name} at {price}");
        }
        thread::sleep(Duration::from_millis(sleep_ms));
    }
    println!("priced {done} symbol(s) this pass");

    // 3. detail the sub-dollar ones
    let sub: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare("SELECT symbol, name FROM companies WHERE price > 0 AND price < 1.0 AND detailed_at IS NULL")
            .expect("stmt");
        stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .expect("query")
            .filter_map(Result::ok)
            .collect()
    };
    println!("sub-dollar symbols needing detail: {}", sub.len());
    for (symbol, name) in sub {
        let profile = get_json(&api(&format!("profile2?symbol={symbol}"))).unwrap_or_default();
        let industry = profile["finnhubIndustry"].as_str().unwrap_or("Unclassified").to_string();
        let currency = profile["currency"].as_str().unwrap_or("USD").to_string();
        thread::sleep(Duration::from_millis(sleep_ms));

        let reported = get_json(&api(&format!("financials-reported?symbol={symbol}&freq=quarterly")))
            .unwrap_or_default();
        if let Some(rows) = reported["data"].as_array() {
            for row in rows {
                let year = row["year"].as_i64().unwrap_or(0);
                let quarter = row["quarter"].as_i64().unwrap_or(0);
                let form = row["form"].as_str().unwrap_or("");
                if form != "10-Q" || !(1..=4).contains(&quarter) {
                    continue;
                }
                let report = &row["report"];
                let mut revenue = None;
                let mut income = None;
                if let Some(items) = report["ic"].as_array() {
                    for item in items {
                        let label = item["label"].as_str().unwrap_or("");
                        let value = item["value"].as_f64();
                        if revenue.is_none()
                            && (label.contains("Revenue") || label.contains("Revenues")
                                || label.contains("Sales"))
                        {
                            revenue = value;
                        }
                        if income.is_none()
                            && (label.contains("Net Income") || label.contains("Net loss"))
                        {
                            income = value;
                        }
                    }
                }
                conn.execute(
                    "INSERT OR REPLACE INTO quarters
                       (symbol, fiscal_year, quarter, revenue, net_income, form, filed)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![symbol, year, quarter, revenue, income, form,
                            row["filedDate"].as_str().unwrap_or("")],
                )
                .ok();
            }
        }
        conn.execute(
            "UPDATE companies SET industry=?1, currency=?2, detailed_at=datetime('now') WHERE symbol=?3",
            params![industry, currency, symbol],
        )
        .ok();
        println!("  detailed {symbol} {name} [{industry}]");
        thread::sleep(Duration::from_millis(sleep_ms));
    }
}

// ------------------------------------------------------------------ read side

fn latest_two_quarters(conn: &Connection, symbol: &str) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>, String, String) {
    let mut rows: Vec<(i64, i64, Option<f64>, Option<f64>)> = {
        let mut stmt = conn
            .prepare(
                "SELECT fiscal_year, quarter, revenue, net_income FROM quarters
                 WHERE symbol=?1 ORDER BY fiscal_year DESC, quarter DESC LIMIT 2",
            )
            .expect("stmt");
        stmt.query_map(params![symbol], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .expect("q")
        .filter_map(Result::ok)
        .collect()
    };
    rows.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let first = rows.first().cloned();
    let second = rows.get(1).cloned();
    let label = |r: &Option<(i64, i64, Option<f64>, Option<f64>)>| match r {
        Some((y, q, _, _)) => format!("Q{q} {y}"),
        None => "".to_string(),
    };
    (
        first.and_then(|r| r.2),
        second.and_then(|r| r.2),
        first.and_then(|r| r.3),
        second.and_then(|r| r.3),
        label(&first),
        label(&second),
    )
}

fn api_stocks(conn: &Connection) -> Vec<Stock> {
    let mut out = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT symbol, name, COALESCE(industry,'Unclassified'), price, COALESCE(currency,'USD')
             FROM companies WHERE price > 0 AND price < 1.0 ORDER BY symbol",
        )
        .expect("stmt");
    let rows: Vec<(String, String, String, f64, String)> = stmt
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .expect("q")
        .filter_map(Result::ok)
        .collect();
    for (symbol, name, industry, price, currency) in rows {
        let (r1, r2, i1, i2, l1, l2) = latest_two_quarters(conn, &symbol);
        let growth = |a: Option<f64>, b: Option<f64>| -> Option<f64> {
            match (a, b) {
                (Some(x), Some(y)) if y.abs() > f64::EPSILON => Some((x - y) / y.abs() * 100.0),
                _ => None,
            }
        };
        out.push(Stock {
            symbol,
            name,
            industry,
            price,
            currency,
            q1_revenue: r1,
            q2_revenue: r2,
            q1_income: i1,
            q2_income: i2,
            revenue_growth: growth(r1, r2),
            income_growth: growth(i1, i2),
            q1_label: l1,
            q2_label: l2,
        });
    }
    out
}

// ------------------------------------------------------------------ server

fn serve(port: u16) {
    let conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("db: {e}");
            return;
        }
    };
    let root = PathBuf::from(env::var("PENNY_WEB").unwrap_or_else(|_| "web".to_string()));
    let addr = format!("0.0.0.0:{port}");
    let server = match tiny_http::Server::http(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("bind {addr}: {e}");
            return;
        }
    };
    println!("serving on http://{addr}");

    for request in server.incoming_requests() {
        let url = request.url().to_string();
        let path = url.split('?').next().unwrap_or("/").to_string();
        let (status, ctype, body): (u16, &str, Vec<u8>) = match path.as_str() {
            "/api/stocks" => {
                let stocks = api_stocks(&conn);
                (200, "application/json", serde_json::to_vec(&stocks).unwrap_or_default())
            }
            "/api/summary" => {
                let stocks = api_stocks(&conn);
                let mut by_industry: std::collections::BTreeMap<String, usize> = Default::default();
                let mut grown = 0;
                let mut shrunk = 0;
                for s in &stocks {
                    *by_industry.entry(s.industry.clone()).or_default() += 1;
                    match s.revenue_growth {
                        Some(g) if g > 0.0 => grown += 1,
                        Some(g) if g < 0.0 => shrunk += 1,
                        _ => {}
                    }
                }
                let priced: i64 = conn
                    .query_row("SELECT COUNT(*) FROM companies WHERE price IS NOT NULL", [], |r| r.get(0))
                    .unwrap_or(0);
                let total: i64 = conn
                    .query_row("SELECT COUNT(*) FROM companies", [], |r| r.get(0))
                    .unwrap_or(0);
                let payload = serde_json::json!({
                    "stocks": stocks.len(), "grown": grown, "shrunk": shrunk,
                    "priced": priced, "universe": total, "by_industry": by_industry,
                });
                (200, "application/json", serde_json::to_vec(&payload).unwrap_or_default())
            }
            _ => {
                let rel = if path == "/" { "index.html" } else { path.trim_start_matches('/') };
                match fs::read(root.join(rel)) {
                    Ok(bytes) => {
                        let ctype = if rel.ends_with(".html") {
                            "text/html; charset=utf-8"
                        } else if rel.ends_with(".js") {
                            "application/javascript"
                        } else if rel.ends_with(".css") {
                            "text/css"
                        } else {
                            "application/octet-stream"
                        };
                        (200, ctype, bytes)
                    }
                    Err(_) => (404, "text/plain", b"not found".to_vec()),
                }
            }
        };
        let _ = request.respond(tiny_http::Response::from_data(body).with_status_code(status)
            .with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap()));
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let get = |flag: &str, default: &str| -> String {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };
    match args.first().map(String::as_str) {
        Some("ingest") => {
            let limit: usize = get("--limit", "40").parse().unwrap_or(40);
            let sleep_ms: u64 = get("--sleep-ms", "1100").parse().unwrap_or(1100);
            ingest(limit, sleep_ms);
        }
        Some("serve") => {
            let port: u16 = get("--port", "8200").parse().unwrap_or(8200);
            serve(port);
        }
        _ => eprintln!("usage: penny-earnings-map ingest|serve [--limit N] [--port P]"),
    }
}

// keep Cursor used on all code paths for future streaming clients
#[allow(dead_code)]
fn _unused(_c: Cursor<Vec<u8>>) {}
