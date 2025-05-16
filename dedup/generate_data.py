import csv
import os
import random # Still used for some minor variations if desired, or could be removed
import string # Still used for some minor variations if desired, or could be removed
import time
import io # For creating in-memory text block

# --- Configuration ---
TARGET_SIZE_GB = 10  # Target file size in Gigabytes
# For quick testing:
# TARGET_SIZE_GB = 0.01 # 10MB
# TARGET_SIZE_GB = 0.1  # 100MB

FILENAME = "large_filler_data_fast.csv"
# ROWS_PER_CHUNK defines how many rows are in our repeatable data block.
# A larger chunk means fewer write calls, but more memory for the block.
ROWS_PER_CHUNK = 20000  # Number of rows in the pre-generated repeatable block
REPORT_INTERVAL_SECONDS = 2 # How often to print progress updates

# --- Column Definitions (Names Only) ---
# These names will be used for the header.
COLUMN_NAMES = [
    "ID", "FirstName", "LastName", "Email", "PhoneNumber", "Address", "City", "Country",
    "JobTitle", "CompanyName", "CreditCardNumber", "DateOfBirth", "RegistrationDate",
    "RandomInteger", "RandomFloat", "RandomString_Short", "RandomString_Long",
    "BooleanFlag", "ProductCode", "Latitude", "Longitude"
]

# --- Fixed Data for Extreme Speed (Content will be largely identical for all data rows) ---
# We create one "template" row. This row will be repeated to form the data block.
# You can make these values more varied if you make FIXED_ROW_VALUES a function call,
# but for maximum speed, truly static values are best.
FIXED_ROW_VALUES = [
    "static_id_" + "a" * 22 + "1",
    "FirstName" + "X" * 5,
    "LastName" + "Y" * 7,
    "user" + "Z" * 10 + "@example.com",
    "000-000-0000",
    "123 Main Street, Apt 4B, " + "Anytown" + "Q" * 5 + ", ST " + "12345" + "P" * 10,
    "CityName" + "R" * 8,
    "CountryName" + "S" * 10,
    "JobTitle" + "T" * 15,
    "CompanyName" + "U" * 12 + " LLC",
    "0000000000000000",
    "2000-01-01",
    "2023-01-01T12:00:00Z",
    str(random.randint(1, 100000)), # Minor variation, negligible impact
    f"{random.uniform(0.0, 1000.0):.4f}", # Minor variation
    "short_str_" + ''.join(random.choices(string.ascii_lowercase, k=15)),
    "long_text_block_" + ("long_filler_text_element_" * 5) + ''.join(random.choices(string.ascii_lowercase, k=50)),
    str(random.choice([True, False])),
    f"PROD-{random.randint(1000,9999)}-{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}",
    f"{random.uniform(-90.0, 90.0):.6f}",
    f"{random.uniform(-180.0, 180.0):.6f}"
]

# Ensure the fixed row has the correct number of columns
if len(COLUMN_NAMES) != len(FIXED_ROW_VALUES):
    raise ValueError(
        f"Mismatch in column definition lengths: "
        f"COLUMN_NAMES has {len(COLUMN_NAMES)} elements, "
        f"FIXED_ROW_VALUES has {len(FIXED_ROW_VALUES)} elements."
    )


def create_data_block_string(num_rows):
    """
    Creates a large string containing multiple CSV rows.
    Uses FIXED_ROW_VALUES for each row for speed.
    """
    string_io = io.StringIO()
    # Using lineterminator='\n' for consistent newlines in the block.
    # The 'open' function with newline='' will handle OS-specific newlines.
    temp_csv_writer = csv.writer(string_io, lineterminator='\n')
    
    # For ultra-fast, truly identical rows:
    # for _ in range(num_rows):
    #     temp_csv_writer.writerow(FIXED_ROW_VALUES)

    # For very slight variation per row within the block (slower than above but still fast):
    # This generates new "random" values for each row in the block.
    # If FIXED_ROW_VALUES itself contains lambda functions, this would call them.
    # As FIXED_ROW_VALUES are mostly static strings with a few direct random calls,
    # this creates one block where rows have some minimal variation based on those direct calls.
    # If you want the *exact same block* written every time, pre-calculate FIXED_ROW_VALUES
    # fully, or call this function only once.
    # For the current setup, this will make each block internally consistent, but different
    # from other blocks if create_data_block_string was called multiple times (it's not).
    #
    # The code below will use the globally defined FIXED_ROW_VALUES, which has its random
    # parts evaluated *once* when the script starts. So the block will be identical.
    for _ in range(num_rows):
         # If you want *some* variation within the block, you'd regenerate row values here:
         # row = [eval(v) if "random" in v else v for v in FIXED_ROW_VALUES_AS_LAMBDAS_OR_STRINGS]
         # For max speed, use the pre-computed FIXED_ROW_VALUES:
        temp_csv_writer.writerow(FIXED_ROW_VALUES)

    block_content = string_io.getvalue()
    string_io.close()
    return block_content

def main():
    target_size_bytes = TARGET_SIZE_GB * 1024 * 1024 * 1024
    current_size_bytes = 0
    rows_written = 0 # This will be an estimate based on blocks
    start_time = time.time()
    last_report_time = start_time

    print(f"Starting generation of '{FILENAME}' aiming for ~{TARGET_SIZE_GB} GB.")
    print(f"Generating one data block of {ROWS_PER_CHUNK} rows...")

    # --- Pre-generate the repeatable data block ---
    # This is the key optimization: create the CSV content for a chunk of rows ONCE.
    data_block_string = create_data_block_string(ROWS_PER_CHUNK)
    # Estimate size of one block (excluding newlines that file.write might add differently than len())
    # This is a rough guide; os.path.getsize() is the source of truth.
    estimated_block_size = len(data_block_string.encode('utf-8'))
    print(f"Data block created. Estimated block size: ~{estimated_block_size / (1024*1024):.2f} MB.")
    print(f"Writing data block repeatedly to '{FILENAME}'.")

    if os.path.exists(FILENAME):
        print(f"Removing existing file: {FILENAME}")
        try:
            os.remove(FILENAME)
        except OSError as e:
            print(f"Warning: Could not remove existing file '{FILENAME}': {e}")


    try:
        with open(FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
            # Write header using csv.writer for proper CSV formatting
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(COLUMN_NAMES)
            csvfile.flush() # Ensure header is written

            # Get initial size (header)
            if os.path.exists(FILENAME):
                 current_size_bytes = os.path.getsize(FILENAME)
            else: # Should not happen if open was successful
                 print("Error: File could not be created for size check after header write.")
                 return


            while current_size_bytes < target_size_bytes:
                csvfile.write(data_block_string) # Write the pre-formatted block of rows
                # csvfile.flush() # Flushing frequently can slow down; OS buffering is often better for bulk writes
                                # Flush only if intermediate size checks are critical or before reporting.

                # Update rows written (this is an approximation of unique rows if blocks were unique)
                rows_written += ROWS_PER_CHUNK
                
                # Check size and report periodically
                # To avoid calling getsize too often if blocks are small or writes are very fast:
                current_time = time.time()
                if current_time - last_report_time >= REPORT_INTERVAL_SECONDS:
                    csvfile.flush() # Flush before getting size for accurate report
                    current_size_bytes = os.path.getsize(FILENAME)
                    elapsed_time = current_time - start_time
                    gb_written = current_size_bytes / (1024**3)
                    speed_mb_s = 0
                    if elapsed_time > 0:
                        speed_mb_s = (current_size_bytes / (1024**2)) / elapsed_time
                    
                    print(f"\rWritten: {rows_written // ROWS_PER_CHUNK:>8,} blocks ({rows_written:,} pseudo-rows) | Size: {gb_written:>6.2f} GB / {TARGET_SIZE_GB} GB "
                          f"| Speed: {speed_mb_s:>6.2f} MB/s | Elapsed: {elapsed_time:>5.0f}s", end="")
                    last_report_time = current_time
            
            # Ensure final data is flushed before final size check
            csvfile.flush()
            current_size_bytes = os.path.getsize(FILENAME) # Final accurate size

            # Final progress update
            terminal_width = 80
            try: terminal_width = os.get_terminal_size().columns
            except OSError: pass
            print(f"\r{' ' * (terminal_width -1)}\r", end="") 

            elapsed_time = time.time() - start_time
            gb_written = current_size_bytes / (1024**3)
            avg_speed_mb_s = 0
            if elapsed_time > 0 :
                avg_speed_mb_s = (current_size_bytes / (1024**2)) / elapsed_time
            
            print(f"Finished generation!")
            print(f"Total pseudo-rows written (blocks * rows_per_block): {rows_written:,}")
            print(f"Final file size: {gb_written:.3f} GB (Target: {TARGET_SIZE_GB} GB)")
            print(f"Total time taken: {elapsed_time:.2f} seconds")
            print(f"Average speed: {avg_speed_mb_s:.2f} MB/s")

    except KeyboardInterrupt:
        print("\nGeneration interrupted by user.")
        if os.path.exists(FILENAME):
            current_size_bytes = os.path.getsize(FILENAME)
            gb_written = current_size_bytes / (1024**3)
            print(f"Current file size: {gb_written:.3f} GB with ~{rows_written:,} pseudo-rows.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()