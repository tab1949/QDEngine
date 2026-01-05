## QDEngine Configuration

You can start QDEngine with command-line flags or with a JSON config file passed via `--config`. This document mirrors the options in `usage.md` and shows how to express them in a file.

### Supported fields

| Field | Type | Default | Description |
| :---- | :--- | :------ | :---------- |
| `host` | string | `"localhost"` | Hostname to listen on. |
| `port` | number | `8888` | Port to listen on (1–65535). |
| `data_format` | string | `"CSV"` | Source data format: `"CSV"` or `"JSON"`. |
| `data_file_options` | object | `{}` | Directory mapping for file data. Keys from the list below, values are local paths. |
| `data_api_options` | object | `{}` | API mapping for remote data. Keys from the list below, values are URLs. |

#### Fields for `data_file` / `data_api`
Use `<data>=<path>` for `data_file_options` and `<data>=<URL>` for `data_api_options`. Valid `<data>` values:

| Data key | Description |
| :------- | :---------- |
| `futures_contracts` | Metadata table `Contracts` (futures contract info) |
| `futures_calendar` | Trading calendar table `TradingCalendar` |
| `futures_tick` | 0.5s snapshot table `HistoryTick` |
| `futures_minute` | 1-minute bars table `HistoryMinute` |
| `futures_daily_rank` | Broker ranking table `DailyRank` |
| `options_list` | Options metadata table `OptionsList` |
| `options_tick` | 0.5s snapshot table `OptionsHistoryTick` |

### Example config file

```json
{
	"host": "0.0.0.0",
	"port": 9000,
	"data_format": "CSV",
	"data_file_options": {
		"futures_contracts": "./data/contracts",
		"futures_calendar": "./data/calendar",
		"futures_tick": "./data/ticks",
		"futures_minute": "./data/minute",
		"futures_daily_rank": "./data/daily_rank",
		"options_list": "./data/options_list",
		"options_tick": "./data/options_ticks"
	},
	"data_api_options": {
		"futures_contracts": "https://example.com/contracts",
		"futures_calendar": "https://example.com/calendar",
		"futures_tick": "",
		"futures_minute": "",
		"futures_daily_rank": "",
		"options_list": "",
		"options_tick": "https://example.com/options/ticks"
	}
}
```

### Notes
- Command-line flags override these values when both are provided.
- If a field is omitted, QDEngine uses the default shown above.
- You can mix local files and remote APIs: set `data_file_options` for data you store on disk and `data_api_options` for data you fetch over HTTP.
- For each data key, provide at least one source (file or API). Supplying both is allowed (redundant) if you want a fallback, but leaving a key absent means that dataset is unavailable.
