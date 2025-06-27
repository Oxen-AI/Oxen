import csv
import os
import random 
import string
import time
import io

TARGET_SIZE_GB = 10

FILENAME = "large_filler.csv"

ROWS_PER_CHUNK = 20000 
REPORT_INTERVAL_SECONDS = 2

COLUMN_NAMES = [
    "ID", "FirstName", "LastName", "Email", "PhoneNumber", "Address", "City", "Country",
    "JobTitle", "CompanyName", "CreditCardNumber", "DateOfBirth", "RegistrationDate",
    "RandomInteger", "RandomFloat", "RandomString_Short", "RandomString_Long",
    "BooleanFlag", "ProductCode", "Latitude", "Longitude"
]

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
    str(random.randint(1, 100000)),
    f"{random.uniform(0.0, 1000.0):.4f}",
    "short_str_" + ''.join(random.choices(string.ascii_lowercase, k=15)),
    "long_text_block_" + ("long_filler_text_element_" * 5) + ''.join(random.choices(string.ascii_lowercase, k=50)),
    str(random.choice([True, False])),
    f"PROD-{random.randint(1000,9999)}-{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}",
    f"{random.uniform(-90.0, 90.0):.6f}",
    f"{random.uniform(-180.0, 180.0):.6f}"
]

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
    temp_csv_writer = csv.writer(string_io, lineterminator='\n')
    
    for _ in range(num_rows):
        temp_csv_writer.writerow(FIXED_ROW_VALUES)

    block_content = string_io.getvalue()
    string_io.close()
    return block_content

def main():
    target_size_bytes = TARGET_SIZE_GB * 1024 * 1024 * 1024
    current_size_bytes = 0
    rows_written = 0
    start_time = time.time()
    last_report_time = start_time

    print(f"Starting generation of '{FILENAME}' aiming for ~{TARGET_SIZE_GB} GB.")
    print(f"Generating one data block of {ROWS_PER_CHUNK} rows...")

    data_block_string = create_data_block_string(ROWS_PER_CHUNK)

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
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(COLUMN_NAMES)
            csvfile.flush()

            if os.path.exists(FILENAME):
                 current_size_bytes = os.path.getsize(FILENAME)
            else:
                 print("Error: File could not be created for size check after header write.")
                 return


            while current_size_bytes < target_size_bytes:
                csvfile.write(data_block_string)
                rows_written += ROWS_PER_CHUNK
                current_time = time.time()
                if current_time - last_report_time >= REPORT_INTERVAL_SECONDS:
                    csvfile.flush() 
                    current_size_bytes = os.path.getsize(FILENAME)
                    elapsed_time = current_time - start_time
                    gb_written = current_size_bytes / (1024**3)
                    speed_mb_s = 0
                    if elapsed_time > 0:
                        speed_mb_s = (current_size_bytes / (1024**2)) / elapsed_time
                    
                    print(f"\rWritten: {rows_written // ROWS_PER_CHUNK:>8,} blocks ({rows_written:,} pseudo-rows) | Size: {gb_written:>6.2f} GB / {TARGET_SIZE_GB} GB "
                          f"| Speed: {speed_mb_s:>6.2f} MB/s | Elapsed: {elapsed_time:>5.0f}s", end="")
                    last_report_time = current_time
            
            csvfile.flush()
            current_size_bytes = os.path.getsize(FILENAME) 


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