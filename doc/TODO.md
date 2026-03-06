# TODO List for QDEngine
This file lists the tasks to do.

## Main Program
- [x] Configuration 
  - [x] Command Line
  - [x] JSON File

## Data Source Adaptation
- [ ] QuantDirect Data Server
- [ ] Local Files
  - [ ] Compressed (ZIP/TGZ/...)
  - [ ] CSV
  - [ ] JSON

## WebSocket Service
- [ ] Test Instruction Handling
  - [ ] Message Structure
  - [ ] Error Handling
  - [ ] Core Process
- [ ] Test & Report
  - [ ] Message Structure
  - [ ] Error Handling
  - [ ] Testing Process
- [ ] Data Fetching Agent
  - [ ] Abstraction of Local File
  - [ ] Abstraction of Network Data Services
- [x] WebCTP Market Data Relaying
  - [x] Data Receiving
  - [x] Data Sending
- [ ] History Replaying
- [ ] Trading Service
  - [ ] Human-Controlled Trading Instruction
  - [ ] Realtime Market Analysis

## Quantitative Researching
- [ ] Strategy Module Adaptor 
- [ ] Event-Driven Trading Simulation Engine
  - [ ] Parameter Configuration
  - [ ] Data Fetching
  - [ ] Data Replay & Simulated Execution
  - [ ] Feedback 

## Trading
- [ ] Common
  - [ ] Order Submit/Cancel/Modify/Notify/Query
  - [ ] Conditional Order
  - [ ] Account Information Access
  - [ ] Instrument Information Access
  - [ ] History Data Interface
  - [ ] Basic Utility
    - [ ] Candle Stick Aggregating
      - [ ] Volume Interval
      - [ ] Tick Interval
      - [ ] Time Interval
    - [ ] Mathematical Functions
- [ ] Basic Account Access
  - [ ] Account Information
  - [ ] Trading Entry
  - [ ] Profit/Loss Feedback
  - [ ] Order Info
  - [ ] Transaction Records
  - [ ] Position
- [ ] Market Chart
- [ ] Strategy Performing
- [ ] Risk-Controlling
  - [ ] Capital Pool
  - [ ] Variable Monitoring 
    - [ ] Assets
    - [ ] Price
    - [ ] Customized
  - [ ] Strategies with Priority