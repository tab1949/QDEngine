# Usage of QDEngine
This document describes the usage of QDEngine.

## Foreword
QDEngine exposes WebSocket interface that enables users to access this engine from either local processes or network hosts.  
The WebSocket interface **provides**:   
1. Quantitative testing and trading interface, for instructions and reports;
2. A unique data interface, no matter what data source were specified;  

This design: 
* Enhanced the extensibility of computing power. ;
* Integrated different data sources into one unique interface for clients; 
* Lowered the delay of communicating with trading server;  


*NOTE: By default, QuantDirect runs a QDEngine process locally, but you can deploy it on other computers you can access and configure QuantDirect to use remote quant engine(s).*  

To get fully use of this tool, we need to know the available options and configurations. So, this document is for that. (*You may need to get fully documentation for configurations in [config.md](./config.md)*)

## Command Line Startup
Configurations can be written in configuration files or specified when running QDEngine through command line, but we suggest you to save your configurations in files, so that you can run QDEngine with same options without typing massive words every time. *(However, you can run a QDEngine process from scripts.)*  
This section introduces command line options. For details about configuration files, see [config.md](./config.md).  
The chart below displays available command line options.  
|Argument|Alias|Value Needed?|Default Value|Description|Comment|
|:-------|:----|:-----------:|:------------:|:----------|:------|
|`--help`|`-h`|N||Display help information||
|`--version`|`-v`|N||Display version information||
|`--host`|`-H`|Y|`localhost`|Specify the host name to listen||
|`--port`|`-p`|Y|`8888`|Specify the port to listen|1~65535|
|`--config`|`-c`|Y|None|Specify the config file|.json|
|`--data-format`|`-F`|Y|`CSV`|Specify the format of source data.|`CSV` or `JSON`|
|`--data-file`||Y||Specify the directory of data files|Format: `<data>=<path>`, like `--data-file futures_tick=./ticks`. Available `<data>` options listed in [Fields and Values](#fields-and-values) section.|
|`--data-api`||Y||Specify the data API|Format: `<data>=<URL>`, like `--data-api futures_tick=https://tabxx.net/api`. Available `<data>` options listed in [Fields and Values](#fields-and-values)|

You can mix local and remote data: use `--data-file` for datasets you keep on disk and `--data-api` for datasets you fetch over HTTP. Provide whichever keys you need; missing keys fall back to defaults or stay unused.
For each data key, supply at least one source (file or API). Providing both is acceptable (redundant) if you want an alternate, but omitting a key means that dataset will be unavailable.

## Fields and Values
- `--data-file`
  |Field|Default|Description|
  |:----|:-----:|:----------|
  |futures_contracts||Metadata table `Contracts` (futures contract info)|
  |futures_calendar||Trading calendar table `TradingCalendar`|
  |futures_tick||0.5s snapshot table `HistoryTick`|
  |futures_minute||1-minute bars table `HistoryMinute`|
  |futures_daily_rank||Broker ranking table `DailyRank`|
  |options_list||Options metadata table `OptionsList`|
  |options_tick||0.5s snapshot table `OptionsHistoryTick`|
- `--data-api`
  |Field|Default|Description|
  |:----|:-----:|:----------|
  |futures_contracts||Remote API for `Contracts`|
  |futures_calendar||Remote API for `TradingCalendar`|
  |futures_tick||Remote API for `HistoryTick`|
  |futures_minute||Remote API for `HistoryMinute`|
  |futures_daily_rank||Remote API for `DailyRank`|
  |options_list||Remote API for `OptionsList`|
  |options_tick||Remote API for `OptionsHistoryTick`|