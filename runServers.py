import subprocess
import time

# List of servers/endpoints to run in sequence
servers = [
    {"name": "clientLogServer.py", "wait": 2},  # Client log server
    {"name": "serverLogServer.py", "wait": 2},  # Server log server
    {"name": "cpubLogServer.py", "wait": 2},  # Pub log server
    {"name": "server.py", "wait": 2},  # Main server
]

processes = []

try:
    for server in servers:
        print(f"Starting {server['name']}...")
        process = subprocess.Popen(["python3", server["name"]])
        processes.append(process)

        # Wait for the server to initialize
        if server["wait"] > 0:
            print(
                f"Waiting {server['wait']} seconds for {server['name']} to initialize..."
            )
            time.sleep(server["wait"])

    print("All servers are running. Press Ctrl+C to stop.")

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping all servers...")
    for process in processes:
        process.terminate()

finally:
    print("All servers stopped.")
