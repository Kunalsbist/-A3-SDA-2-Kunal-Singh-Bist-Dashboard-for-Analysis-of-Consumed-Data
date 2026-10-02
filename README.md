# Indian Railway Brake-Stress Streaming Analytics — V3

## Project objective
Build a streaming analytics pipeline that replays scheduled train trips from 1 January 2025 onward. Each completed scheduled trip is one streaming event. The event combines route distance, scheduled duration, average scheduled speed, number of intermediate stops, terrain elevation gain/loss, and cumulative trip exposure.

The project creates a **0–100 brake-stress proxy**. A higher score means the trip has more of the project-defined operational/terrain factors associated with braking demand. It is NOT actual brake pressure, brake temperature, pad wear, or a prediction of failure.

## Data sources
- Indian Railway Data open dataset: trains.json + stations.json, containing train routes, running days, distances, schedules, station coordinates and related fields.
- Open-Meteo elevation API: terrain elevation from coordinates. No API key is required for normal non-commercial use.

## Score model
Trip score = weighted normalized sum of:
- Distance: 20%
- Intermediate stops: 15%
- Elevation gain: 15%
- Elevation descent: 30%
- Average scheduled speed: 20%

Descent receives the highest weight because the project's primary hypothesis is braking demand during downhill operation. These weights are assumptions and should be validated/sensitivity-tested in the final report.

## Historical replay
The producer creates one event for every scheduled operating day between START_DATE and END_DATE. This means a daily train contributes approximately one trip per operating day; a train running only on selected weekdays contributes only on those weekdays.

This is a **historical replay stream**, not a claim that we have GPS-verified actual train movement on every date.

## VS Code setup
1. Open this folder in VS Code.
2. Open Terminal -> New Terminal.
3. Create/activate a virtual environment if desired:
   `python -m venv .venv`
   `.venv\Scripts\activate`
4. Run:
   `python -u .\producer.py | python -u .\consumer.py`

Start with TRAIN_LIMIT=10 in config.py. After it works, increase to 50, 100, and eventually None for all trains.

## Later stages
1. Producer -> consumer (current stage)
2. Add MongoDB persistence
3. Build MongoDB Compass queries
4. Add Grafana dashboards
5. Add train-level cumulative exposure rankings
6. Add sensitivity testing for score weights
7. If suitable operational data becomes available, compare the proxy against real maintenance/brake data
