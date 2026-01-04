#!/usr/bin/env python3
"""
NMEA 2000 Log Combiner

Processes multiple N2K decoded CSV files in a directory and combines them
into Expedition format files based on UTC time proximity. Files with timestamps
within 1 hour of each other are merged into a single output file.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from n2k_to_expedition_converter import N2KToExpeditionConverter


def find_time_segments_in_file(n2k_file, time_gap_hours=1):
    """
    Analyze an N2K file and find continuous time segments (no gaps > time_gap_hours).
    Returns a list of tuples: [(start_time, end_time, filepath, segment_index), ...]
    Each tuple represents a continuous segment within the file.
    """
    print(f"  Analyzing {n2k_file.name}...")

    converter = N2KToExpeditionConverter()
    converter.parse_n2k_file(str(n2k_file))

    if converter.base_datetime is None:
        print(f"    Warning: No system time found in {n2k_file.name}")
        return []

    # Get all timestamps
    if not converter.data_by_time:
        print(f"    Warning: No data found in {n2k_file.name}")
        return []

    timestamps = sorted([float(ts) for ts in converter.data_by_time.keys()])

    # Convert relative timestamps to UTC datetime objects
    utc_times = []
    for relative_ts in timestamps:
        offset = relative_ts - converter.base_relative_time
        utc_time = converter.base_datetime + timedelta(seconds=offset)
        utc_times.append(utc_time)

    # Find segments where there are no gaps > time_gap_hours
    segments = []
    segment_start = utc_times[0]
    segment_end = utc_times[0]

    for i in range(1, len(utc_times)):
        time_gap = utc_times[i] - utc_times[i-1]

        if time_gap > timedelta(hours=time_gap_hours):
            # Gap detected - end current segment and start new one
            segments.append((segment_start, segment_end, str(n2k_file), len(segments)))
            segment_start = utc_times[i]
            segment_end = utc_times[i]
        else:
            # Continue current segment
            segment_end = utc_times[i]

    # Don't forget the last segment
    segments.append((segment_start, segment_end, str(n2k_file), len(segments)))

    if len(segments) > 1:
        print(f"    Found {len(segments)} time segments (gaps > {time_gap_hours}h detected)")
        for i, (start, end, _, _) in enumerate(segments):
            print(f"      Segment {i+1}: {start} to {end}")
    else:
        print(f"    Time range: {segments[0][0]} to {segments[0][1]}")

    return segments


def group_files_by_time(file_time_ranges, time_gap_hours=1):
    """
    Group files into clusters based on time proximity.
    Files are grouped if their times are within time_gap_hours of each other.

    Returns a list of file groups, where each group is a list of file paths.
    """
    if not file_time_ranges:
        return []

    # Sort by start time
    sorted_files = sorted(file_time_ranges, key=lambda x: x[0])

    groups = []
    current_group = [sorted_files[0]]
    current_end = sorted_files[0][1]

    for i in range(1, len(sorted_files)):
        start_time, end_time, filepath, segment_index = sorted_files[i]

        # Check if this file is within time_gap_hours of the current group
        time_diff = start_time - current_end

        if time_diff <= timedelta(hours=time_gap_hours):
            # Add to current group
            current_group.append(sorted_files[i])
            # Update group end time to the latest end time
            current_end = max(current_end, end_time)
        else:
            # Start a new group
            groups.append(current_group)
            current_group = [sorted_files[i]]
            current_end = end_time

    # Don't forget the last group
    if current_group:
        groups.append(current_group)

    return groups


def combine_files_in_group(file_group, output_file, round_decimals=5):
    """
    Combine multiple N2K file segments into a single Expedition output file.
    file_group is a list of tuples: (start_time, end_time, filepath, segment_index)
    """
    print(f"\nCombining {len(file_group)} segment(s) into {output_file}...")

    # Create a single converter to merge all data
    combined_converter = N2KToExpeditionConverter(round_decimals=round_decimals)

    # Process each file segment and merge into combined converter
    for start_time, end_time, filepath, segment_index in file_group:
        print(f"  Processing: {Path(filepath).name} (segment {segment_index + 1})")
        converter = N2KToExpeditionConverter(round_decimals=round_decimals)
        converter.parse_n2k_file(filepath)

        # Merge data into combined converter
        if combined_converter.base_datetime is None and converter.base_datetime is not None:
            # Use first file's base time
            combined_converter.base_datetime = converter.base_datetime
            combined_converter.base_relative_time = converter.base_relative_time

        # Convert segment boundaries to relative timestamps
        if converter.base_datetime is None:
            continue

        segment_start_relative = (start_time - converter.base_datetime).total_seconds() + converter.base_relative_time
        segment_end_relative = (end_time - converter.base_datetime).total_seconds() + converter.base_relative_time

        # Merge only timestamps within this segment's time range
        for timestamp_key, data in converter.data_by_time.items():
            timestamp_float = float(timestamp_key)

            # Filter to only include data within this segment
            if segment_start_relative <= timestamp_float <= segment_end_relative:
                if timestamp_key not in combined_converter.data_by_time:
                    combined_converter.data_by_time[timestamp_key] = {}

                # Merge data for this timestamp
                for field, values in data.items():
                    if field not in combined_converter.data_by_time[timestamp_key]:
                        combined_converter.data_by_time[timestamp_key][field] = []
                    combined_converter.data_by_time[timestamp_key][field].extend(values)

    # Write combined output
    combined_converter.write_expedition_file(output_file)
    print(f"  Combined file written: {output_file}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python combine_n2k_logs.py <input_directory> [output_directory] [--time-gap HOURS] [--round-decimals N]")
        print("\nOptions:")
        print("  --time-gap HOURS      Max time gap in hours to combine files (default: 1)")
        print("  --round-decimals N    Round timestamps to N decimal places (default: 5)")
        print("\nExample:")
        print('  python combine_n2k_logs.py "C:\\logs" "C:\\output" --time-gap 2 --round-decimals 5')
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = None
    time_gap_hours = 1
    round_decimals = 5

    # Parse arguments
    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--time-gap':
            if i + 1 < len(sys.argv):
                try:
                    time_gap_hours = float(sys.argv[i + 1])
                    i += 2
                except ValueError:
                    print(f"Error: Invalid value for --time-gap: {sys.argv[i + 1]}")
                    sys.exit(1)
            else:
                print("Error: --time-gap requires a numeric argument")
                sys.exit(1)
        elif arg == '--round-decimals':
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
            # Assume it's the output directory if not specified yet
            if output_dir is None:
                output_dir = Path(arg)
            i += 1

    # Verify input directory exists
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Error: Input directory not found or not a directory: {input_dir}")
        sys.exit(1)

    # Set default output directory if not specified
    if output_dir is None:
        output_dir = input_dir / "combined_output"

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("NMEA 2000 Log Combiner")
    print("=" * 70)
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Time gap threshold: {time_gap_hours} hour(s)")
    print(f"Timestamp rounding: {round_decimals} decimal places")
    print("=" * 70)

    # Find all N2K decoded files
    n2k_files = list(input_dir.glob("*.n2kdecoded.csv"))

    if not n2k_files:
        print(f"\nNo *.n2kdecoded.csv files found in {input_dir}")
        sys.exit(0)

    print(f"\nFound {len(n2k_files)} N2K decoded files")

    # Extract time segments from all files
    print("\nAnalyzing file time ranges and detecting gaps...")
    file_segments = []
    for n2k_file in n2k_files:
        try:
            segments = find_time_segments_in_file(n2k_file, time_gap_hours)
            if segments:
                file_segments.extend(segments)
        except Exception as e:
            print(f"  Error processing {n2k_file.name}: {e}")

    if not file_segments:
        print("\nNo valid time segments found in any files!")
        sys.exit(1)

    # Group segments by time proximity
    print(f"\nGrouping segments by time proximity (max gap: {time_gap_hours} hour(s))...")
    segment_groups = group_files_by_time(file_segments, time_gap_hours)

    print(f"\nCreated {len(segment_groups)} group(s):")
    for i, group in enumerate(segment_groups, 1):
        start_time = group[0][0]
        end_time = max(item[1] for item in group)
        print(f"  Group {i}: {len(group)} segment(s) from {start_time} to {end_time}")
        for item in group:
            segment_index = item[3]
            print(f"    - {Path(item[2]).name} (segment {segment_index + 1})")

    # Combine each group into a separate output file
    print("\n" + "=" * 70)
    print("Combining segment groups...")
    print("=" * 70)

    for i, group in enumerate(segment_groups, 1):
        # Generate output filename based on first segment's start time
        start_time = group[0][0]
        output_filename = f"combined_{start_time.strftime('%Y%m%d_%H%M%S')}_expedition.csv"
        output_path = output_dir / output_filename

        try:
            combine_files_in_group(group, str(output_path), round_decimals)
        except Exception as e:
            print(f"  Error combining group {i}: {e}")

    print("\n" + "=" * 70)
    print("Processing complete!")
    print(f"Output files written to: {output_dir}")
    print("=" * 70)


if __name__ == "__main__":
    main()
