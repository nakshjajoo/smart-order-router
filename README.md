# Smart Order Router Backtest

This project implements and backtests a Smart Order Router (SOR) based on the Cont & Kukanov cost model. The system uses Kafka to simulate a real-time stream of market data, which is consumed by a backtesting script that applies the SOR logic, tunes its parameters, and benchmarks it against standard execution strategies.

## Project Structure

-   `l1_day.csv`: The raw Level 1 market data snapshots. (not on github)
-   `kafka_producer.py`: Reads the `l1_day.csv` file, filters data for the required simulation window, and streams venue snapshots to a Kafka topic.
-   `backtest.py`: The main application. It consumes the market data stream from Kafka, implements the SOR allocation logic, runs a parameter search, and benchmarks the results.
-   `allocator_pseudocode.txt`: The provided pseudocode describing the static allocation logic. (not on github)
-   `README.md`

## Approach and Logic

### 1. Data Simulation (`kafka_producer.py`)

The producer simulates a live market data feed.
- It reads the source `l1_day.csv` file into a pandas DataFrame.
- It converts the `ts_event` column to datetime objects to allow for time-based filtering.
- It filters the data to a specific time window as required: **13:36:32 to 13:45:14 UTC**.
- To ensure data quality for the simulation, it only considers `rtype=10` events, which represent order book snapshots, and drops duplicates to use only the latest update for each venue at any given timestamp.
- It iterates through the data, grouped by timestamp, and sends a JSON object containing a list of all venue snapshots for that timestamp to the `mock_l1_stream` Kafka topic.
- To mimic a real-time feed, it calculates the time delta between consecutive events and uses `time.sleep()` to pause execution accordingly.

### 2. Backtesting and SOR Logic (`backtest.py`)

The backtester is the core of the project and performs several key tasks:
- **Consume Data**: It subscribes to the `mock_l1_stream` topic and ingests all snapshots for the simulation run into memory.
- **Implement Allocator**: It translates the `allocate` and `compute_cost` functions from the provided pseudocode into Python. The `compute_cost` function calculates the explicit and implicit costs of an allocation based on the Cont & Kukanov model, factoring in penalties for over-execution (`lambda_over`), under-execution (`lambda_under`), and general execution risk (`theta_queue`). The `allocate` function searches for the share allocation across venues that minimizes this cost for a given order size.
- **Parameter Tuning**: The effectiveness of the SOR depends heavily on the penalty parameters (`lambda_over`, `lambda_under`, `theta_queue`). The script performs a grid search over a predefined range of these values. For each combination, it runs the entire simulation and calculates the total cost. The combination that results in the lowest total cost is selected as the `best_parameters`.
- **Benchmarking**: To prove the SOR's value, its performance is compared against three standard, simpler strategies:
    - **Naïve Best Ask**: At every opportunity, this strategy sends the entire remaining order to the single venue with the lowest ask price.
    - **TWAP (Time-Weighted Average Price)**: The total order is split into equal chunks to be executed over 60-second intervals throughout the simulation window.
    - **VWAP (Volume-Weighted Average Price)**: The total order is split into chunks proportional to the total displayed volume in each 60-second interval.
- **Final Output**: The script calculates the total cash spent and average fill price for the optimized SOR and all three baselines. It also calculates the cost savings of the SOR in basis points (bps) relative to each baseline. All results are then printed to standard output in a clean, machine-readable JSON format.

## EC2 Deployment Instructions

These steps will be able to guide you through setting up and running the full pipeline on an AWS EC2 instance.

**1. Launch EC2 Instance**
- **Instance Type**: `t2.micro` (used because of free tier)
- **AMI**: Ubuntu Server 22.04 LTS
- **Storage**: 12 GB EBS volume (to accommodate the OS, Kafka, and Python libraries).
- **Security Group**: Ensure you have an inbound rule that allows SSH traffic (Port 22) from your IP address.

**2. SSH into EC2 and Install Dependencies**
```bash
# SSH into your instance
ssh -i /path/to/your-key.pem ubuntu@<your-ec2-public-ip>

# Update package lists
sudo apt update && sudo apt upgrade -y

# Install Java (required for Kafka)
sudo apt install default-jre -y

# Install Python and Pip
sudo apt install python3 python3-pip -y

# Install required Python libraries
pip3 install pandas kafka-python numpy
```

**3. Download and Set Up Kafka**
```bash
# Download Kafka (version 3.7.0 for Scala 2.13)
wget [https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz](https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz)

# Extract the archive
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0
```

**4. Upload Project Files**

Use `scp` to upload your project files (`kafka_producer.py`, `backtest.py`, and `l1_day.csv`) from your local machine to the EC2 instance's home directory (`/home/ubuntu`).
```bash
# Run this from your local machine's terminal
scp -i /path/to/your-key.pem kafka_producer.py backtest.py l1_day.csv ubuntu@<your-ec2-public-ip>:~
```

**5. Run the Pipeline**

You will need two separate terminal sessions connected to your EC2 instance.

**Terminal 1: Start Zookeeper and Kafka Server**
```bash
# Navigate to your Kafka directory
cd ~/kafka_2.13-3.7.0

# Start Zookeeper (in the background)
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait a few seconds for Zookeeper to start, then start Kafka Server
bin/kafka-server-start.sh config/server.properties
# Keep this terminal window open!
```

**Terminal 2: Run the Producer and Backtester**
```bash
# In a new terminal, SSH into your EC2 instance again
ssh -i /path/to/your-key.pem ubuntu@<your-ec2-public-ip>

# Start the Kafka producer. It will start streaming data and then exit.
python3 kafka_producer.py

# Once the producer is running, or has finished, start the backtester.
# The backtester will wait for messages from the producer.
# It's best to start the backtester shortly after the producer.
python3 backtest.py
```
> You can run the producer and then immediately run the backtester in the same terminal using:
`python3 kafka_producer.py && python3 backtest.py`

\
**6. View the Results**
The `backtest.py` script will run for a few minutes while it performs the parameter search and benchmarking. Once complete, it will print the final JSON output directly to the console.

### Sample Screenshot of Expected Output

Below is a conceptual example of what the running system and final output might look like.

**Terminal 1 (Kafka Server Running):**
```
[2025-06-19 18:44:25,016] INFO Registered kafka:type=kafka.Log4jController MBean (kafka.utils.Log4jControllerRegistration$)
...
```

**Terminal 2 (Running scripts and seeing JSON output):**
```
$ ubuntu@ip-172-31-6-28:~$ python3 kafka_producer.py && python3 backtest.py
Kafka Producer created successfully.
Streaming 54327 snapshots from 2024-08-01 13:36:32.491911683+00:00 to 2024-08-01 13:45:13.949170107+00:00
Sent snapshot for timestamp: 2024-08-01 13:36:32.491911683+00:00
...
Finished streaming data.
Waiting 10 seconds for Kafka producer to start streaming...
Consumer connected. Waiting for data...
Received 63711 snapshots from Kafka.
Starting parameter search over 27 combinations...
Parameter search complete. Best cost: 1114117.28, Best params: {'lambda_over': 0.1, 'lambda_under': 0.1, 'theta_queue': 0.1}
{
  "best_parameters": {
    "lambda_over": 0.1,
    "lambda_under": 0.1,
    "theta_queue": 0.1
  },
  "optimized": {
    "total_cash": 1114117.28,
    "avg_fill_px": 222.8235
  },
  "baselines": {
    "best_ask": {
      "total_cash": 1114117.28,
      "avg_fill_px": 222.8235
    },
    "twap": {
      "total_cash": 1115265.03,
      "avg_fill_px": 223.053
    },
    "vwap": {
      "total_cash": 1116307.4,
      "avg_fill_px": 223.2615
    }
  },
  "savings_vs_baselines_bps": {
    "best_ask": 0.0,
    "twap": 10.29,
    "vwap": 19.62
  }
}
```

**System Info (`uname -a`):**
```
Linux ip-172-31-6-28 6.8.0-1029-aws #31-Ubuntu SMP Wed Apr 23 18:42:41 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux
```
