use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;

#[derive(Debug)]
pub enum Mode {
    Help,
    Version,
    Server,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum DataFormat {
    CSV,
    JSON,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct DataOptions {
    futures_contracts: Option<String>,
    futures_calendar: Option<String>,
    futures_tick: Option<String>,
    futures_minute: Option<String>,
    futures_daily_rank: Option<String>,
    options_list: Option<String>,
    options_tick: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    #[serde(skip)]
    pub mode: Option<Mode>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub data_format: Option<DataFormat>,
    #[serde(default)]
    pub data_file_options: DataOptions,
    #[serde(default)]
    pub data_api_options: DataOptions,
}

fn parse_setting(exp: &String) -> Result<(String, String), String> {
    let sep: Vec<&str> = exp.split('=').collect();
    if sep.len() != 2 {
        return Err(format!("Invalid setting: {}", exp));
    }
    return Ok((sep[0].to_string(), sep[1].to_string()));
}

fn has_source(file_opt: &Option<String>, api_opt: &Option<String>) -> bool {
    match (file_opt, api_opt) {
        (Some(f), _) if !f.is_empty() => true,
        (_, Some(a)) if !a.is_empty() => true,
        _ => false,
    }
}

fn check_config(config: &mut Config) {
    if config.mode.is_none() {
        return;
    }

    if config.data_format.is_none() {
        eprintln!("Data format is not specified.");
        config.mode = None;
        return;
    }

    let mut missing: Vec<&str> = Vec::new();

    let checks: [(&str, &Option<String>, &Option<String>); 7] = [
        (
            "futures_contracts",
            &config.data_file_options.futures_contracts,
            &config.data_api_options.futures_contracts,
        ),
        (
            "futures_calendar",
            &config.data_file_options.futures_calendar,
            &config.data_api_options.futures_calendar,
        ),
        (
            "futures_tick",
            &config.data_file_options.futures_tick,
            &config.data_api_options.futures_tick,
        ),
        (
            "futures_minute",
            &config.data_file_options.futures_minute,
            &config.data_api_options.futures_minute,
        ),
        (
            "futures_daily_rank",
            &config.data_file_options.futures_daily_rank,
            &config.data_api_options.futures_daily_rank,
        ),
        (
            "options_list",
            &config.data_file_options.options_list,
            &config.data_api_options.options_list,
        ),
        (
            "options_tick",
            &config.data_file_options.options_tick,
            &config.data_api_options.options_tick,
        ),
    ];

    for (name, file_opt, api_opt) in checks.iter() {
        if !has_source(file_opt, api_opt) {
            missing.push(name);
        }
    }

    if !missing.is_empty() {
        eprintln!("Missing data sources for: {}", missing.join(", "));
        eprintln!("Provide at least one source (file or API) for each data key.");
        config.mode = None;
    }
}

fn merge_data_options(base: &mut DataOptions, incoming: &DataOptions) {
    if let Some(v) = &incoming.futures_contracts {
        base.futures_contracts = Some(v.clone());
    }
    if let Some(v) = &incoming.futures_calendar {
        base.futures_calendar = Some(v.clone());
    }
    if let Some(v) = &incoming.futures_tick {
        base.futures_tick = Some(v.clone());
    }
    if let Some(v) = &incoming.futures_minute {
        base.futures_minute = Some(v.clone());
    }
    if let Some(v) = &incoming.futures_daily_rank {
        base.futures_daily_rank = Some(v.clone());
    }
    if let Some(v) = &incoming.options_list {
        base.options_list = Some(v.clone());
    }
    if let Some(v) = &incoming.options_tick {
        base.options_tick = Some(v.clone());
    }
}

fn merge_config(base: &mut Config, incoming: Config) {
    if let Some(v) = incoming.host {
        base.host = Some(v);
    }
    if let Some(v) = incoming.port {
        base.port = Some(v);
    }
    if let Some(v) = incoming.data_format {
        base.data_format = Some(v);
    }

    merge_data_options(&mut base.data_file_options, &incoming.data_file_options);
    merge_data_options(&mut base.data_api_options, &incoming.data_api_options);
}

fn read_config_file(file: &String, config: &mut Config) {
    match fs::exists(file) {
        Ok(true) => {
            let config_file: fs::File;
            match fs::OpenOptions::new().read(true).open(file) {
                Ok(f) => {
                    config_file = f;
                }
                Err(e) => {
                    eprintln!("Error opening config file {}: {}", file, e);
                    config.mode = None;
                    return;
                }
            }
            match serde_json::from_reader(config_file) {
                Ok(de) => {
                    merge_config(config, de);
                    config.mode = Some(Mode::Server);
                }
                Err(e) => {
                    eprintln!("Error parsing config file {}: {}", file, e);
                    config.mode = None;
                    return;
                }
            }
        }
        Ok(false) => {
            eprintln!("Config file {} does not exist.", file);
            return;
        }
        Err(e) => {
            eprintln!("Error stating config file {}: {}", file, e);
            return;
        }
    }
}

impl Config {
    pub fn parse(args: &mut std::env::Args) -> Config {
        let mut ret: Config = Config {
            mode: Some(Mode::Server),
            host: Some(String::from("localhost")),
            port: Some(8888),
            data_format: Some(DataFormat::CSV),
            data_file_options: DataOptions {
                futures_contracts: None,
                futures_calendar: None,
                futures_tick: None,
                futures_minute: None,
                futures_daily_rank: None,
                options_list: None,
                options_tick: None,
            },
            data_api_options: DataOptions {
                futures_contracts: None,
                futures_calendar: None,
                futures_tick: None,
                futures_minute: None,
                futures_daily_rank: None,
                options_list: None,
                options_tick: None,
            },
        };
        let args = args.skip(1);
        let mut need_value_for: String = String::new();
        for arg in args {
            if arg == "--help" || arg == "-h" {
                ret.mode = Some(Mode::Help);
                break;
            } else if arg == "--version" || arg == "-v" {
                ret.mode = Some(Mode::Version);
                break;
            } else if arg == "--host" || arg == "-H" {
                need_value_for = String::from("host");
                continue;
            } else if arg == "--port" || arg == "-p" {
                need_value_for = String::from("port");
                continue;
            } else if arg == "--config" || arg == "-c" {
                need_value_for = String::from("config");
                continue;
            } else if arg == "--data-format" || arg == "-F" {
                need_value_for = String::from("data_format");
                continue;
            } else if arg == "--data-file" {
                need_value_for = String::from("data_file");
                continue;
            } else if arg == "--data-api" {
                need_value_for = String::from("data_api");
                continue;
            } else if need_value_for != "" {
                if need_value_for == "host" {
                    ret.host = Some(arg);
                } else if need_value_for == "port" {
                    match arg.parse::<u16>() {
                        Ok(port) if port > 0 && port < 65535 => {
                            ret.port = Some(port);
                        }
                        _ => {
                            ret.mode = None;
                            eprintln!("Invalid port: {}", arg);
                            eprintln!("Run `qd-engine --help` for usage.");
                            break;
                        }
                    }
                } else if need_value_for == "config" {
                    read_config_file(&arg, &mut ret);
                } else if need_value_for == "data_format" {
                    let upper = arg.to_ascii_uppercase();
                    if upper == "CSV" {
                        ret.data_format = Some(DataFormat::CSV);
                    } else if upper == "JSON" {
                        ret.data_format = Some(DataFormat::JSON);
                    } else {
                        ret.mode = None;
                        eprintln!("Invalid data format: {}", arg);
                        eprintln!("Run `qd-engine --help` for usage.");
                        break;
                    }
                } else if need_value_for == "data_file" {
                    let (key, value) = match parse_setting(&arg) {
                        Ok(kv) => kv,
                        Err(e) => {
                            ret.mode = None;
                            eprintln!("{}", e);
                            eprintln!("Run `qd-engine --help` for usage.");
                            break;
                        }
                    };
                    match key.as_str() {
                        "futures_contracts" => {
                            ret.data_file_options.futures_contracts = Some(value);
                        }
                        "futures_calendar" => {
                            ret.data_file_options.futures_calendar = Some(value);
                        }
                        "futures_tick" => {
                            ret.data_file_options.futures_tick = Some(value);
                        }
                        "futures_minute" => {
                            ret.data_file_options.futures_minute = Some(value);
                        }
                        "futures_daily_rank" => {
                            ret.data_file_options.futures_daily_rank = Some(value);
                        }
                        "options_list" => {
                            ret.data_file_options.options_list = Some(value);
                        }
                        "options_tick" => {
                            ret.data_file_options.options_tick = Some(value);
                        }
                        _ => {
                            ret.mode = None;
                            eprintln!("Unknown data file key: {}", key);
                            eprintln!("Run `qd-engine --help` for usage.");
                            break;
                        }
                    }
                } else if need_value_for == "data_api" {
                    let (key, value) = match parse_setting(&arg) {
                        Ok(kv) => kv,
                        Err(e) => {
                            ret.mode = None;
                            eprintln!("{}", e);
                            eprintln!("Run `qd-engine --help` for usage.");
                            break;
                        }
                    };
                    match key.as_str() {
                        "futures_contracts" => {
                            ret.data_api_options.futures_contracts = Some(value);
                        }
                        "futures_calendar" => {
                            ret.data_api_options.futures_calendar = Some(value);
                        }
                        "futures_tick" => {
                            ret.data_api_options.futures_tick = Some(value);
                        }
                        "futures_minute" => {
                            ret.data_api_options.futures_minute = Some(value);
                        }
                        "futures_daily_rank" => {
                            ret.data_api_options.futures_daily_rank = Some(value);
                        }
                        "options_list" => {
                            ret.data_api_options.options_list = Some(value);
                        }
                        "options_tick" => {
                            ret.data_api_options.options_tick = Some(value);
                        }
                        _ => {
                            ret.mode = None;
                            eprintln!("Unknown data API key: {}", key);
                            eprintln!("Run `qd-engine --help` for usage.");
                            break;
                        }
                    }
                }
                need_value_for.clear();
            } else {
                ret.mode = None;
                eprintln!("Unknown argument: {}", arg);
                eprintln!("Run `qd-engine --help` for usage.");
                break;
            }
        }

        if !need_value_for.is_empty() {
            ret.mode = None;
            eprintln!("Missing value for option: --{}", need_value_for);
            eprintln!("Run `qd-engine --help` for usage.");
        }

        check_config(&mut ret);

        ret
    }
}

pub fn print_help() {
    println!("Usage: qd-engine [OPTIONS]");
    println!("Options:");
    println!("  -h, --help                 Display help information");
    println!("  -v, --version              Display version information");
    println!("  -H, --host <HOST>          Specify the host name to listen (default: localhost)");
    println!("  -p, --port <PORT>          Specify the port to listen (1-65535, default: 8888)");
    println!("  -c, --config <FILE>        Specify the config file (.json)");
    println!("The following options can be set in the config file:");
    println!("  -F, --data-format <FMT>    Specify the data format (CSV or JSON, default: CSV)");
    println!(
        "      --data-file <DATA=PATH>  Specify directory of data files (e.g., futures_tick=./ticks)"
    );
    println!(
        "      --data-api <DATA=URL>    Specify the data API (e.g., futures_tick=https://tabxx.net/api)"
    );
}

pub fn print_version() {
    println!("QDEngine version 0.0.1;");
}
