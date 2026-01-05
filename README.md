# QDEngine 
Back-testing engine of QuantDirect  
Usage: [doc/usage.md](./doc/usage.md)

## Features
This project is designed to provide a high performance back-testing engine for markets like futures, options, stocks, etc.  

Main Functions:
- **Back-test**
- **Live deciding**
- **Market Replay**

Main features:
- **Easy-to-use**: Edit strategies easily with built-in library;
- **Reusable Strategy**: Compatible strategy code with QuantDirect App Indicators; 
- **Standardized I/O**: Formatted JSON I/O for instructions (optional CSV output);
- **Efficiency**: High performance computing;
- **Live Support**: Realtime deciding (if connected with realtime data server);
- **Replay**: Connect with QuantDirect App to replay history market;
- **Configurable**: Diverse configurable parameters like:
    - Transaction rules;
    - Slippage;
    - ... (See [doc/config.md](./doc/config.md))

## Data source
QuantDirect is designed to build a history market data server, which provides data access through HTTP RESTful API.  
QDEngine can be set to use this data server, or use other data sources (like local CSV files). You just need to simply configure the engine. (See [doc/config.md](./doc/config.md))  
*(By the way, When you are using local files as data source, the QDEngine itself is a WebSocket data server actually. See [doc/usage.md](./doc/usage.md))*
