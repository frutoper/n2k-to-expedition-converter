#!/usr/bin/env python3
"""
NMEA 2000 to Expedition Log Converter

Converts decoded NMEA 2000 CAN packet data to Expedition log format.
Aggregates multiple CANPACKET messages by timestamp into single rows.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta


def parse_lat_lon(coord_str):
    """
    Convert NMEA coordinate format to decimal degrees.
    Example: "42° 07.650444' N" -> 42.1275074
    """
    if not coord_str or coord_str == "Data not available":
        return None

    try:
        # Remove extra spaces and split
        coord_str = coord_str.strip()

        # Parse format like "42° 07.650444' N"
        parts = coord_str.replace('°', ' ').replace("'", ' ').split()
        degrees = float(parts[0])
        minutes = float(parts[1])
        direction = parts[2] if len(parts) > 2 else ''

        # Convert to decimal degrees
        decimal = degrees + (minutes / 60.0)

        # Apply direction
        if direction in ['S', 'W']:
            decimal = -decimal

        return decimal
    except:
        return None


def parse_angle(angle_str):
    """Convert angle string to float, handling radians if needed."""
    if not angle_str or angle_str == "Data not available":
        return None
    try:
        angle = float(angle_str)
        # Angles in N2K are typically in radians, convert to degrees
        # But some are already in degrees, check if value is > 2*pi
        if angle > 6.2832:  # Greater than 2*pi, likely degrees
            return angle
        else:
            # Convert radians to degrees
            return angle * 57.2958  # 180/pi
    except:
        return None


def parse_speed(speed_str):
    """Convert speed from m/s to knots."""
    if not speed_str or speed_str == "Data not available":
        return None
    try:
        # N2K speeds are in m/s, convert to knots
        return float(speed_str) * 1.94384
    except:
        return None


def extract_value(header, row, column_name):
    """Extract value from row by column name using header mapping."""
    try:
        # Find the column index from header
        idx = header.index(column_name)
        if idx < len(row):
            value = row[idx].strip()
            if value and value != "Data not available":
                return value
    except (ValueError, IndexError):
        pass
    return None


class N2KToExpeditionConverter:
    def __init__(self, time_resolution=0.1, round_decimals=5):
        """
        Initialize converter.

        Args:
            time_resolution: Time bucket size in seconds for aggregating data (default 0.1 = 100ms)
            round_decimals: Number of decimal places to round timestamps to (e.g., 5 for 0.00001s precision)
                          If set, this will be used for final output rounding and averaging
        """
        self.data_by_time = {}  # Store lists of values for averaging
        self.time_resolution = time_resolution
        self.round_decimals = round_decimals
        self.base_datetime = None  # Will store the first System Time we encounter
        self.base_relative_time = None  # The relative time when we got base_datetime
        self.system_time_map = {}  # Map of relative_time -> absolute datetime from PGN 126992

    def parse_n2k_file(self, n2k_file):
        """Parse the N2K decoded CSV file."""
        print(f"Reading N2K file: {n2k_file}")

        # First pass: Extract all PGN 126992 (System Time) messages to build time reference map
        print("  Pass 1: Extracting system time references...")
        with open(n2k_file, 'r', encoding='latin-1') as f:
            reader = csv.reader(f)
            self.header = next(reader)  # Save header row for column lookup

            for row in reader:
                if len(row) < 5:
                    continue

                timestamp_str = row[1]
                pgn = row[3]

                if pgn == "126992":  # System Time
                    self._extract_system_time(timestamp_str, row)

        if self.system_time_map:
            print(f"  Found {len(self.system_time_map)} system time reference(s)")
            # Set base time from first system time entry
            first_rel_time = min(self.system_time_map.keys())
            self.base_relative_time = first_rel_time
            self.base_datetime = self.system_time_map[first_rel_time]
            print(f"  Base time established: {self.base_datetime} (UTC) at relative time {self.base_relative_time}")
        else:
            print("  WARNING: No PGN 126992 (System Time) found - UTC conversion will not be available")

        # Second pass: Extract all PGN data
        print("  Pass 2: Extracting sensor data...")
        with open(n2k_file, 'r', encoding='latin-1') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header

            line_count = 0
            for row in reader:
                line_count += 1
                if line_count % 10000 == 0:
                    print(f"    Processed {line_count} lines...")

                if len(row) < 5:
                    continue

                # Parse basic fields
                timestamp_str = row[1]  # Time column
                pgn = row[3]  # PGN
                name = row[4].strip('"')  # Name

                # Round timestamp to nearest time bucket for aggregation
                try:
                    timestamp_raw = float(timestamp_str)
                    # Round to nearest time_resolution interval
                    timestamp = round(timestamp_raw / self.time_resolution) * self.time_resolution
                    timestamp_key = f"{timestamp:.3f}"
                except:
                    continue

                # Create data structure for this timestamp if not exists
                if timestamp_key not in self.data_by_time:
                    self.data_by_time[timestamp_key] = {}

                # Extract data based on PGN type (skip 126992 since we already processed it)
                if pgn != "126992":
                    self.extract_pgn_data(timestamp_key, pgn, name, row)

            print(f"  Total lines processed: {line_count}")
            print(f"  Unique timestamps: {len(self.data_by_time)}")

    def _extract_system_time(self, timestamp_str, row):
        """Extract PGN 126992 System Time and store in system_time_map."""
        try:
            # Get relative timestamp
            relative_time = float(timestamp_str.replace(',', '.'))

            # Try to find "Date" field in the CSV
            date_idx = self.header.index("Date") if "Date" in self.header else None
            time_indices = [i for i, col in enumerate(self.header) if col == "Time"]
            time_idx = [i for i in time_indices if i > (date_idx or 0)][0] if time_indices else None

            if date_idx is not None and time_idx is not None and date_idx < len(row) and time_idx < len(row):
                date_str = row[date_idx].strip()
                time_str = row[time_idx].strip()

                if date_str and time_str and date_str != "Data not available" and time_str != "Data not available":
                    dt = None
                    # Try multiple date/time format combinations
                    for date_fmt in ["%m/%d/%Y", "%d/%m/%Y"]:
                        for time_fmt in ["%H:%M:%S", "%H:%M:%S.%f"]:
                            try:
                                dt = datetime.strptime(f"{date_str} {time_str}", f"{date_fmt} {time_fmt}")
                                break
                            except ValueError:
                                continue
                        if dt:
                            break

                    # If still no match, try NMEA 2000 raw format
                    if not dt:
                        try:
                            nmea_epoch = datetime(1990, 5, 1)
                            date_days = float(date_str)
                            time_seconds = float(time_str)
                            dt = nmea_epoch + timedelta(days=date_days, seconds=time_seconds)
                        except:
                            return

                    # Store this system time reference
                    if dt:
                        self.system_time_map[relative_time] = dt
        except Exception:
            pass

    def _append_value(self, data, field, value):
        """Helper method to append a value to a field's list."""
        if field not in data:
            data[field] = []
        data[field].append(value)

    def extract_pgn_data(self, timestamp, pgn, name, row):
        """Extract relevant data from a PGN and append to lists for later averaging."""
        # Ensure timestamp exists in data_by_time
        if timestamp not in self.data_by_time:
            self.data_by_time[timestamp] = {}
        data = self.data_by_time[timestamp]

        if pgn == "127250":  # Vessel Heading
            heading = extract_value(self.header, row, "Heading Sensor Reading")
            if heading:
                parsed = parse_angle(heading)
                if parsed is not None:
                    self._append_value(data, 'HDG', parsed)

        elif pgn == "127251":  # Rate of Turn
            rot = extract_value(self.header, row, "Rate of Turn")
            if rot:
                try:
                    self._append_value(data, 'ROT', float(rot))
                except:
                    pass

        elif pgn == "127257":  # Attitude (Pitch/Roll)
            pitch = extract_value(self.header, row, "Pitch")
            roll = extract_value(self.header, row, "Roll")
            if pitch:
                parsed = parse_angle(pitch)
                if parsed is not None:
                    self._append_value(data, 'Trim', parsed)
            if roll:
                parsed = parse_angle(roll)
                if parsed is not None:
                    self._append_value(data, 'Heel', parsed)

        elif pgn == "127252":  # Heave
            heave = extract_value(self.header, row, "Heave")
            if heave:
                try:
                    self._append_value(data, 'Heave', float(heave))
                except:
                    pass

        elif pgn == "129025":  # Position (Rapid Update)
            lat_str = extract_value(self.header, row, "Latitude")
            lon_str = extract_value(self.header, row, "Longitude")
            if lat_str:
                parsed = parse_lat_lon(lat_str)
                if parsed is not None:
                    self._append_value(data, 'Lat', parsed)
            if lon_str:
                parsed = parse_lat_lon(lon_str)
                if parsed is not None:
                    self._append_value(data, 'Lon', parsed)

        elif pgn == "129026":  # COG & SOG (Rapid Update)
            cog = extract_value(self.header, row, "Course Over Ground")
            sog = extract_value(self.header, row, "Speed Over Ground")
            if cog:
                parsed = parse_angle(cog)
                if parsed is not None:
                    self._append_value(data, 'COG', parsed)
            if sog:
                parsed = parse_speed(sog)
                if parsed is not None:
                    self._append_value(data, 'SOG', parsed)

        elif pgn == "130306":  # Wind Data
            wind_ref = extract_value(self.header, row, "Wind Reference")
            wind_speed = extract_value(self.header, row, "Wind Speed")
            wind_dir = extract_value(self.header, row, "Wind Direction")

            if wind_ref and "2 (Apparent Wind" in wind_ref:
                # Apparent Wind
                if wind_speed:
                    parsed = parse_speed(wind_speed)
                    if parsed is not None:
                        self._append_value(data, 'AWS', parsed)
                if wind_dir:
                    parsed = parse_angle(wind_dir)
                    if parsed is not None:
                        self._append_value(data, 'AWA', parsed)

            elif wind_ref and "4 (Theoretical" in wind_ref:
                # True Wind
                if wind_speed:
                    parsed = parse_speed(wind_speed)
                    if parsed is not None:
                        self._append_value(data, 'TWS', parsed)
                if wind_dir:
                    parsed = parse_angle(wind_dir)
                    if parsed is not None:
                        self._append_value(data, 'TWA', parsed)

        elif pgn == "128259":  # Speed, Water Referenced
            bsp = extract_value(self.header, row, "Speed Water Referenced")
            if bsp:
                parsed = parse_speed(bsp)
                if parsed is not None:
                    self._append_value(data, 'BSP', parsed)

        elif pgn == "128267":  # Water Depth
            depth = extract_value(self.header, row, "Water Depth Transducer")
            if depth:
                try:
                    self._append_value(data, 'Depth', float(depth))
                except:
                    pass

        elif pgn == "130316":  # Temperature, Extended Range
            temp_source = extract_value(self.header, row, "Temperature Source")
            temp = extract_value(self.header, row, "Actual Temperature")
            if temp and temp_source:
                if "Sea Temperature" in temp_source:
                    # Convert Kelvin to Celsius
                    try:
                        self._append_value(data, 'SeaTemp', float(temp) - 273.15)
                    except:
                        pass

        elif pgn == "127258":  # Magnetic Variation
            variation = extract_value(self.header, row, "Variation")
            if variation:
                try:
                    self._append_value(data, 'Variation', float(variation))
                except:
                    pass

        elif pgn == "129539":  # GNSS DOPs
            hdop = extract_value(self.header, row, "HDOP")
            vdop = extract_value(self.header, row, "VDOP")
            if hdop:
                try:
                    self._append_value(data, 'PDOP', float(hdop))
                except:
                    pass

        # Note: PGN 126992 (System Time) is now processed separately in the first pass
        # and doesn't need to be handled here

    def relative_to_utc(self, relative_time_str):
        """Convert relative time to UTC timestamp."""
        if self.base_datetime is None or self.base_relative_time is None:
            # No system time available, return relative time
            return relative_time_str

        try:
            relative_time = float(relative_time_str)
            # Calculate offset from base time
            offset_seconds = relative_time - self.base_relative_time
            # Add offset to base datetime
            absolute_time = self.base_datetime + timedelta(seconds=offset_seconds)
            # Return as Excel/Expedition UTC format (fractional days since 1899-12-30)
            # Excel epoch: 1899-12-30 00:00:00
            excel_epoch = datetime(1899, 12, 30)
            delta = absolute_time - excel_epoch
            utc_value = delta.total_seconds() / 86400.0  # Convert to fractional days
            return utc_value
        except:
            return relative_time_str

    def consolidate_data(self):
        """
        Consolidate data by averaging values in lists and optionally rounding timestamps.
        This reduces the number of output rows when round_decimals is set.
        """
        if self.round_decimals is None or self.round_decimals == 0:
            # No rounding requested, just average the lists
            consolidated = {}
            for timestamp_key, data in self.data_by_time.items():
                consolidated[timestamp_key] = self._average_data_dict(data)
            return consolidated

        # Round timestamps and group data
        rounded_data = {}

        for timestamp_key, data in self.data_by_time.items():
            # Round the timestamp
            timestamp_float = float(timestamp_key)
            rounded_timestamp = round(timestamp_float, self.round_decimals)
            rounded_key = f"{rounded_timestamp:.{self.round_decimals}f}"

            # Initialize rounded timestamp bucket if needed
            if rounded_key not in rounded_data:
                rounded_data[rounded_key] = {}

            # Merge data lists into the rounded timestamp bucket
            for field, values in data.items():
                if field not in rounded_data[rounded_key]:
                    rounded_data[rounded_key][field] = []
                rounded_data[rounded_key][field].extend(values)

        # Average the consolidated data
        consolidated = {}
        for timestamp_key, data in rounded_data.items():
            consolidated[timestamp_key] = self._average_data_dict(data)

        print(f"  Consolidated from {len(self.data_by_time)} to {len(consolidated)} rows")
        return consolidated

    def _average_data_dict(self, data_dict):
        """Average all lists in a data dictionary."""
        averaged = {}
        for field, values in data_dict.items():
            if isinstance(values, list) and len(values) > 0:
                try:
                    # Use circular mean for angular data (headings, angles)
                    if field in ['HDG', 'COG', 'TWD', 'AWA', 'TWA']:
                        averaged[field] = self._circular_mean(values)
                    else:
                        # Regular mean for other values
                        averaged[field] = sum(values) / len(values)
                except Exception as e:
                    # Skip fields that can't be averaged
                    print(f"Warning: Could not average field '{field}': {e}")
                    pass
        return averaged

    def _circular_mean(self, angles):
        """
        Calculate mean of angles using circular statistics.
        Handles wrap-around at 0/360 degrees properly.
        """
        import math
        if not angles:
            return None

        try:
            # Filter out None values
            valid_angles = [a for a in angles if a is not None]
            if not valid_angles:
                return None

            # Convert to radians and compute sin/cos components
            sin_sum = sum(math.sin(math.radians(a)) for a in valid_angles)
            cos_sum = sum(math.cos(math.radians(a)) for a in valid_angles)

            # Calculate mean angle
            mean_angle = math.degrees(math.atan2(sin_sum, cos_sum))

            # Normalize to 0-360
            if mean_angle < 0:
                mean_angle += 360

            return mean_angle
        except Exception as e:
            print(f"Warning: Error in circular mean calculation: {e}, angles: {angles}")
            return None

    def write_expedition_file(self, output_file):
        """Write data to Expedition format CSV."""
        print(f"\nWriting Expedition format file: {output_file}")

        # Consolidate and average data
        consolidated_data = self.consolidate_data()

        # Calculate derived values
        for timestamp_key in consolidated_data:
            data = consolidated_data[timestamp_key]

            # Calculate TWD from TWA + HDG if available
            if 'TWA' in data and 'HDG' in data and data['TWA'] is not None and data['HDG'] is not None:
                twd = (data['HDG'] + data['TWA']) % 360
                data['TWD'] = twd

        # Define Expedition column headers (minimal set of common fields)
        exp_columns = [
            'Boat', 'Utc', 'BSP', 'AWA', 'AWS', 'TWA', 'TWS', 'TWD',
            'Set', 'Drift', 'HDG', 'SeaTemp', 'Depth', 'Heel', 'Trim',
            'ROT', 'PDOP', 'Lat', 'Lon', 'COG', 'SOG', 'Heave'
        ]

        # Column indices for the !boat line
        column_indices = {
            'Boat': 0, 'Utc': 0, 'BSP': 1, 'AWA': 2, 'AWS': 3, 'TWA': 4,
            'TWS': 5, 'TWD': 6, 'Set': 11, 'Drift': 12, 'HDG': 13,
            'SeaTemp': 15, 'Depth': 17, 'Heel': 18, 'Trim': 19,
            'ROT': 32, 'PDOP': 41, 'Lat': 48, 'Lon': 49, 'COG': 50,
            'SOG': 51, 'Heave': 268
        }

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Write header - only Boat column gets the ! prefix
            header_row = ['!Boat'] + exp_columns[1:]
            writer.writerow(header_row)

            # Write column index mapping
            index_row = ['!boat'] + [str(column_indices.get(col, 0)) for col in exp_columns[1:]]
            writer.writerow(index_row)

            # Write version
            writer.writerow(['!v12.5.1'])

            # Sort timestamps
            sorted_times = sorted(consolidated_data.keys(), key=lambda x: float(x))

            # Deduplicate by UTC timestamp - merge rows with same UTC after formatting
            utc_deduplicated = {}
            for timestamp in sorted_times:
                data = consolidated_data[timestamp]

                # Convert relative time to UTC
                utc_timestamp = self.relative_to_utc(timestamp)

                # Format UTC timestamp with appropriate precision
                if isinstance(utc_timestamp, float):
                    if self.round_decimals is not None and self.round_decimals > 0:
                        # Round to the specified decimal places
                        utc_formatted = f'{utc_timestamp:.{self.round_decimals}f}'
                    else:
                        # Use default precision
                        utc_formatted = str(utc_timestamp)
                else:
                    utc_formatted = timestamp

                # Check if this UTC timestamp already exists
                if utc_formatted in utc_deduplicated:
                    # Merge data by averaging - convert both to lists and re-average
                    existing_data = utc_deduplicated[utc_formatted]

                    # For each field, combine values and re-average
                    for field in set(list(existing_data.keys()) + list(data.keys())):
                        if field in existing_data and field in data:
                            # Both have this field - average them
                            if field in ['HDG', 'COG', 'TWD', 'AWA', 'TWA']:
                                # Use circular mean for angles
                                existing_data[field] = self._circular_mean([existing_data[field], data[field]])
                            else:
                                # Regular mean
                                existing_data[field] = (existing_data[field] + data[field]) / 2.0
                        elif field in data:
                            # Only new data has this field
                            existing_data[field] = data[field]
                        # If only existing has it, keep existing value (already in dict)
                else:
                    # New UTC timestamp
                    utc_deduplicated[utc_formatted] = data.copy()

            if len(utc_deduplicated) < len(sorted_times):
                print(f"  Merged duplicate UTC timestamps: {len(sorted_times)} -> {len(utc_deduplicated)} rows")

            # Write data rows in sparse format (column_index, value pairs)
            row_count = 0
            for utc_formatted, data in sorted(utc_deduplicated.items(), key=lambda x: float(x[0])):
                # Start with boat number and UTC timestamp
                row = ['0', utc_formatted]

                # Add data as column_index,value pairs (only for non-empty values)
                for col in exp_columns[2:]:
                    value = data.get(col, None)

                    # Only include if we have a value
                    if value is not None:
                        col_index = column_indices.get(col, 0)

                        # Format value based on type
                        if isinstance(value, float):
                            if col in ['Lat', 'Lon']:
                                formatted_value = f'{value:.7f}'
                            elif col in ['BSP', 'AWS', 'TWS', 'SOG', 'TWD', 'AWA', 'TWA', 'COG', 'HDG']:
                                formatted_value = f'{value:.2f}'
                            elif col in ['Depth', 'SeaTemp']:
                                formatted_value = f'{value:.2f}'
                            else:
                                formatted_value = f'{value:.3f}'
                        else:
                            formatted_value = str(value)

                        # Add column_index, value pair
                        row.append(str(col_index))
                        row.append(formatted_value)

                writer.writerow(row)
                row_count += 1

                if row_count % 1000 == 0:
                    print(f"  Written {row_count} rows...")

            print(f"  Total rows written: {row_count}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python n2k_to_expedition_converter.py <input_n2k_file> [output_file] [--round-decimals N]")
        print("\nOptions:")
        print("  --round-decimals N    Round timestamps to N decimal places (default: 5 for 0.00001s precision)")
        print("                        This reduces row count by averaging data within rounded time buckets")
        print("                        Use --round-decimals 0 to disable rounding")
        print("\nExamples:")
        print('  python n2k_to_expedition_converter.py "log.n2kdecoded.csv"')
        print('  python n2k_to_expedition_converter.py "log.n2kdecoded.csv" --round-decimals 3')
        print('  python n2k_to_expedition_converter.py "log.n2kdecoded.csv" "output.csv" --round-decimals 0')
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = None
    round_decimals = 5

    # Parse arguments
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--round-decimals':
            if i + 1 < len(sys.argv):
                try:
                    round_decimals = int(sys.argv[i + 1])
                    i += 2
                except ValueError:
                    print(f"Error: Invalid value for --round-decimals: {sys.argv[i + 1]}")
                    sys.exit(1)
            else:
                print("Error: --round-decimals requires a numeric argument")
                sys.exit(1)
        else:
            # Assume it's the output file if not specified yet
            if output_file is None:
                output_file = arg
            i += 1

    # Determine output file if not specified
    if output_file is None:
        input_path = Path(input_file)
        output_file = str(input_path.parent / f"{input_path.stem}_expedition.csv")

    # Verify input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    print("=" * 70)
    print("NMEA 2000 to Expedition Log Converter")
    print("=" * 70)
    if round_decimals is not None and round_decimals > 0:
        print(f"Timestamp rounding: {round_decimals} decimal places")
        print("Data will be averaged within rounded time buckets")
    elif round_decimals == 0:
        print("Timestamp rounding: DISABLED")
        print("Each original timestamp will be preserved")
    print("=" * 70)

    # Create converter and process
    converter = N2KToExpeditionConverter(round_decimals=round_decimals)
    converter.parse_n2k_file(input_file)
    converter.write_expedition_file(output_file)

    print("\n" + "=" * 70)
    print("Conversion complete!")
    print(f"Output file: {output_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
