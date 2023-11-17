# Signal Emulator

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/release)

A python object model for working with TfL traffic signal data

## Features

- Import signal data from the following sources;
    - Digital timing sheets (.csv)
    - UTC Signal timing data (M37 and M16 messages)
    - UTC Plans (.pln)
    - UTC Plan Timetables (PJA UTC query)
- Postgres database integration
- Calculate phase based signal timings
- Export timings to VISUM Signal Controllers and Signal Groups format
- Export signal configuration and timings to Linsig version 2.3.6 format

## Getting Started

Python 3.11 or later required.

Postgres 14 or 16 required for database support.

### Install with pip
```commandline
pip install git+https://github.com/fradge26/signal_emulator.git
```
### Install with git
```commandline
git clone https://github.com/fradge26/signal_emulator.git
cd signal_emulator
pip install -r requirements.txt
```
Sample database for 10 junctions can be restored from pg_dump using psql
```psql
cd signal_emulator/signal_emulator/resources/pg_dump
psql -h your_host -p your_port -U your_username -d your_database < tfl_signal_timings_10_sites.sql
```
## Run the sample script
```commandline
python -m signal_emulator.scripts.run_signal_emulator
```

## Run the tests
```bash
pytest
```
