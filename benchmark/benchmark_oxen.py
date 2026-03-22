import subprocess
import time
import statistics

def run_command(command):
    start_time = time.time()
    subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.time() - start_time

def main():
    commands = [
        {"name":"add", "command":"oxen add ."},
        {"name":"commit", "command":"oxen commit -m \"benchmark commit\""},
        # {"name":"push", "command":"oxen push origin main"},
        # {"name":"clone", "command":"oxen clone https://github.com/example/repo.git"}
    ]


    for command in commands:
        duration = run_command(command["command"])
        print(f"{command['name']}: {duration:.2f} seconds")

if __name__ == "__main__":
    main()