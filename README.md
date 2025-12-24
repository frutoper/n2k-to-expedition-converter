# NMEA 2000 to Expedition Log Converter

Convert NMEA 2000 (N2K) decoded CSV files to Expedition sailing software format with intelligent data aggregation and UTC time synchronization.

## Features

- **Automatic UTC Time Conversion**: Uses PGN 126992 (System Time) to convert relative timestamps to absolute UTC time
- **Smart Data Aggregation**: Averages sensor readings within configurable time buckets
- **Circular Averaging**: Properly handles angular data (headings, wind angles, COG) using circular mean
- **Timestamp Rounding**: Reduces output file size by consolidating data to configurable precision (default: 5 decimal places = 0.01ms)
- **Sparse Matrix Format**: Outputs data in Expedition's efficient sparse format (only non-null values)
- **Multi-File Processing**: Combines multiple N2K files based on time proximity
- **Automatic File Splitting**: Detects and splits files with internal time gaps

## Scripts

### 1. n2k_to_expedition_converter.py

Converts a single NMEA 2000 decoded CSV file to Expedition format.

#### Usage

```bash
python n2k_to_expedition_converter.py <input_file.csv> [output_file.csv] [--time-resolution SECONDS] [--round-decimals PLACES]
```

**Arguments:**
- `input_file.csv`: Path to N2K decoded CSV file (required)
- `output_file.csv`: Output path (optional, defaults to input name + "_expedition.csv")
- `--time-resolution`: Time bucket size in seconds for aggregation (default: 0.1s)
- `--round-decimals`: Decimal places for timestamp rounding (default: 5)

**Examples:**

```bash
# Basic conversion with defaults
python n2k_to_expedition_converter.py myrace.n2kdecoded.csv

# Custom output file and 3 decimal places
python n2k_to_expedition_converter.py myrace.n2kdecoded.csv myrace_exp.csv --round-decimals 3

# Aggregate to 0.5 second buckets
python n2k_to_expedition_converter.py myrace.n2kdecoded.csv --time-resolution 0.5
```

### 2. combine_n2k_logs.py

Processes multiple N2K files in a directory, combining files with overlapping time ranges and splitting files with gaps.

#### Usage

```bash
python combine_n2k_logs.py <input_directory> [output_directory] [--time-gap-hours HOURS] [--round-decimals PLACES]
```

**Arguments:**
- `input_directory`: Directory containing N2K decoded CSV files (required)
- `output_directory`: Where to save combined files (optional, defaults to input_directory/combined_output)
- `--time-gap-hours`: Maximum time gap to group files (default: 1 hour)
- `--round-decimals`: Decimal places for timestamp rounding (default: 5)

**Examples:**

```bash
# Process all files in a directory
python combine_n2k_logs.py ./my_race_logs

# Use 2-hour window for combining files
python combine_n2k_logs.py ./logs ./output --time-gap-hours 2

# Higher precision timestamps
python combine_n2k_logs.py ./logs --round-decimals 6
```

#### How It Works

1. Scans all `.n2kdecoded.csv` files in the input directory
2. Detects time gaps within each file (> time-gap-hours)
3. Splits files with gaps into separate segments
4. Groups segments from different files if their times are within time-gap-hours
5. Outputs combined Expedition files named by start timestamp

**Example scenario:**
- `race1.n2kdecoded.csv`: 10:00-11:00, then gap, then 15:00-16:00
- `race2.n2kdecoded.csv`: 10:30-11:30
- **Result:**
  - `combined_100000_expedition.csv` (contains race1 10:00-11:00 + race2 10:30-11:30)
  - `combined_150000_expedition.csv` (contains race1 15:00-16:00)

## Supported PGN Messages

The converter extracts data from the following NMEA 2000 PGNs:

| PGN | Name | Expedition Fields |
|-----|------|-------------------|
| 126992 | System Time | UTC time reference |
| 127250 | Vessel Heading | HDG (heading) |
| 127257 | Attitude | Heel, Trim |
| 128259 | Speed | BSP (boat speed) |
| 128267 | Water Depth | Depth |
| 129025 | Position, Rapid Update | Lat, Lon |
| 129026 | COG & SOG, Rapid Update | COG, SOG |
| 129029 | GNSS Position Data | Lat, Lon |
| 129033 | Time & Date | (future support) |
| 129283 | Cross Track Error | XTE |
| 130306 | Wind Data | AWA, AWS, TWA, TWS, TWD |
| 130310 | Environmental Parameters | SeaTemp |
| 130311 | Environmental Parameters | SeaTemp |
| 130316 | Temperature, Extended Range | SeaTemp |
| 127258 | Magnetic Variation | Variation |
| 129539 | GNSS DOPs | PDOP |

## Requirements

- Python 3.6 or higher
- No external dependencies (uses only standard library)

## Input File Format

The scripts expect NMEA 2000 data that has already been decoded to CSV format. This is typically produced by tools like:
- Actisense N2K Reader
- CANboat analyzer
- Other N2K decoding software

**Required columns:**
- Column 1: Line number
- Column 2: Timestamp (relative time in seconds)
- Column 3: Type (e.g., "CANPACKET")
- Column 4: PGN number
- Column 5: PGN name
- Additional columns: PGN-specific data fields

**Important:** The script requires PGN 126992 (System Time) to be present in the input file for proper UTC time conversion. If not found, it will output relative timestamps instead.

## Output Format

The output follows Expedition's sparse matrix CSV format:

**Header rows:**
```
!Boat,Utc,BSP,AWA,AWS,TWA,TWS,TWD,Set,Drift,HDG,SeaTemp,Depth,Heel,Trim,ROT,PDOP,Lat,Lon,COG,SOG,Heave
!boat,0,1,2,3,4,5,6,11,12,13,15,17,18,19,32,41,48,49,50,51,268
!v12.5.1
```

**Data rows (sparse format):**
```
0,45859.15310,1,6.81,2,31.35,3,13.67,48,44.3431296,49,-86.5743779,50,356.15
```

Each data row contains:
- `0` = Boat identifier
- `45859.15310` = UTC timestamp (Excel epoch: days since 1899-12-30)
- Pairs of `column_index,value` for non-null data only

## Time Handling

### Time Resolution vs Round Decimals

- **time_resolution** (default: 0.1s): Initial bucketing for aggregating raw data
  - Example: 0.1s groups data into 100ms buckets before averaging
  - Larger values = more aggressive aggregation, smaller files

- **round_decimals** (default: 5): Final UTC timestamp precision
  - Example: 5 decimal places = 0.00001 days ≈ 0.864 seconds precision
  - Higher values = more precise timestamps, larger files

### UTC Time Conversion

1. Script scans for all PGN 126992 (System Time) messages
2. Builds a map of relative timestamps → absolute UTC times
3. Converts all relative timestamps to UTC using base reference
4. Handles both MM/DD/YYYY and DD/MM/YYYY date formats automatically
5. Outputs UTC as fractional days since Excel epoch (1899-12-30)

## Data Averaging

When multiple sensor readings fall within the same time bucket:

**Linear data** (speed, temperature, depth, etc.):
- Uses arithmetic mean

**Angular data** (HDG, COG, TWD, AWA, TWA):
- Uses circular mean to handle 359°→1° wraparound correctly
- Converts to unit vectors, averages, converts back to angle

## Troubleshooting

### "No PGN 126992 (System Time) found"

Your input file doesn't contain system time messages. The converter will still work but UTC times will be relative timestamps instead of absolute times.

**Solutions:**
- Ensure your N2K decoder is configured to output PGN 126992
- Check if your N2K device is broadcasting system time
- Use a GPS source that provides PGN 126992

### Incorrect Date/Time Values

If UTC times look wrong, check:
- System time on your N2K GPS device
- Timezone settings (PGN 126992 should be UTC)
- Date format in your decoded CSV (MM/DD/YYYY vs DD/MM/YYYY)

### Large Output Files

Reduce file size by:
- Increasing `--time-resolution` (e.g., 0.5 for half-second buckets)
- Decreasing `--round-decimals` (e.g., 3 for millisecond precision)

## Contributing

Contributions welcome! Please open an issue or pull request on GitHub.

## License

MIT License - feel free to use and modify for your needs.

## Author

Created for the sailing community to simplify N2K data analysis in Expedition.

## Acknowledgments

- NMEA 2000 specification by NMEA
- Expedition sailing software by Nick White
- Actisense for N2K hardware and decoding tools
