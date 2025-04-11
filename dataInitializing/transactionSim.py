import json
import random
import time
import uuid
import datetime
import math
import boto3 # Optional: Only needed if sending directly to Kinesis

# --- Configuration ---
NUM_CARDS = 50
NUM_TRANSACTIONS = 1000
FRAUD_INJECTION_PROBABILITY = 0.03 # Probability a transaction is flagged for potential fraud *if* card is compromised
COMPROMISE_CARD_PROBABILITY = 0.10 # Probability a card gets marked as compromised initially
AVG_TRANSACTION_DELAY_SECONDS = 0.5 # Average time between transactions
LOCATION_VARIATION_KM_NORMAL = 50  # Max distance (km) from home for normal transactions
LOCATION_VARIATION_KM_FRAUD = 5000 # Max distance (km) from home for fraud transactions
NORMAL_AMOUNT_RANGE = (5.00, 250.00)
FRAUD_AMOUNT_RANGE = (100.00, 2000.00)
HIGH_VELOCITY_THRESHOLD_KMH = 800 # Speed (km/h) considered suspicious

# Optional: Kinesis Configuration
KINESIS_STREAM_NAME = 'your-fraud-detection-stream' # Replace with your Kinesis stream name
SEND_TO_KINESIS = False # Set to True to send data to Kinesis

# --- Helper Functions ---

def generate_location_around(lat, lon, max_distance_km):
    """Generates a random coordinate within a certain distance (approximation)."""
    # Approximate conversions: 1 deg lat ~= 111km, 1 deg lon ~= 111km * cos(lat)
    radius_deg_lat = max_distance_km / 111.1
    # Use latitude in radians for cosine calculation
    radius_deg_lon = max_distance_km / (111.1 * math.cos(math.radians(lat)))

    delta_lat = random.uniform(-radius_deg_lat, radius_deg_lat)
    delta_lon = random.uniform(-radius_deg_lon, radius_deg_lon)

    new_lat = max(-90.0, min(90.0, lat + delta_lat))   # Clamp latitude
    new_lon = ((lon + delta_lon + 180) % 360) - 180  # Wrap longitude
    return round(new_lat, 6), round(new_lon, 6)

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points on the earth."""
    R = 6371 # Earth radius in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    a = math.sin(dLat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dLon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def generate_timestamp(start_time, increment_seconds):
    """Generates an increasing timestamp."""
    return start_time + datetime.timedelta(seconds=increment_seconds)

def send_to_kinesis(data_record, stream_name, partition_key):
    """Sends a single data record to Kinesis."""
    try:
        kinesis_client = boto3.client('kinesis')
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data_record),
            PartitionKey=partition_key # Use CardID for good distribution
        )
        # print(f"Sent to Kinesis: {response['SequenceNumber']}")
        return True
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")
        return False

# --- Main Simulation ---

print("Initializing card data...")
cards_data = {}
for i in range(NUM_CARDS):
    card_id = f"CARD_{i:04d}"
    # Generate somewhat realistic home locations (e.g., across US/Europe)
    home_lat = random.uniform(25.0, 65.0) # Latitude range
    home_lon = random.uniform(-125.0, 40.0) # Longitude range (covers US & Europe)
    cards_data[card_id] = {
        "HomeLatitude": round(home_lat, 6),
        "HomeLongitude": round(home_lon, 6),
        "IsCompromised": random.random() < COMPROMISE_CARD_PROBABILITY,
        "LastTxTimestamp": None,
        "LastTxLatitude": None,
        "LastTxLongitude": None,
    }

print(f"Generated {len(cards_data)} cards. Starting transaction simulation...")

current_time = datetime.datetime.now(datetime.timezone.utc)

for i in range(NUM_TRANSACTIONS):
    card_id = random.choice(list(cards_data.keys()))
    card_info = cards_data[card_id]

    is_fraud = False
    amount = 0.0
    merchant_lat = 0.0
    merchant_lon = 0.0

    # Determine if this transaction is fraudulent
    if card_info["IsCompromised"] and random.random() < FRAUD_INJECTION_PROBABILITY:
        is_fraud = True
        amount = round(random.uniform(*FRAUD_AMOUNT_RANGE), 2)
        # Generate location far from home
        merchant_lat, merchant_lon = generate_location_around(
            card_info["HomeLatitude"],
            card_info["HomeLongitude"],
            random.uniform(LOCATION_VARIATION_KM_NORMAL * 2, LOCATION_VARIATION_KM_FRAUD) # Ensure it's far
        )

        # Check for velocity anomaly opportunity
        if card_info["LastTxTimestamp"] and card_info["LastTxLatitude"]:
             time_delta_seconds = (current_time - card_info["LastTxTimestamp"]).total_seconds()
             if time_delta_seconds > 0: # Avoid division by zero
                 distance_km = haversine(
                     card_info["LastTxLatitude"], card_info["LastTxLongitude"],
                     merchant_lat, merchant_lon
                 )
                 velocity_kmh = (distance_km / time_delta_seconds) * 3600
                 # If velocity isn't naturally high, potentially reduce time delta to force it
                 if velocity_kmh < HIGH_VELOCITY_THRESHOLD_KMH and random.random() < 0.5:
                      # Simulate shorter time gap - NOTE: This breaks strict chronological order
                      # for realism, but useful for testing velocity rules.
                      # For strict order, just let natural high velocity occur.
                      required_time_seconds = (distance_km / HIGH_VELOCITY_THRESHOLD_KMH) * 3600
                      simulated_short_delay = max(1, random.uniform(required_time_seconds * 0.5, required_time_seconds * 0.9))
                      current_time = card_info["LastTxTimestamp"] + datetime.timedelta(seconds=simulated_short_delay)
                      # print(f"DEBUG: Forced shorter time delta for {card_id} to trigger velocity.")
             else:
                 # Increment time normally even for fraud if velocity isn't forced
                 time_increment = random.expovariate(1.0 / AVG_TRANSACTION_DELAY_SECONDS)
                 current_time = generate_timestamp(current_time, time_increment)

    else: # Normal transaction
        is_fraud = False
        amount = round(random.uniform(*NORMAL_AMOUNT_RANGE), 2)
        # Generate location near home
        merchant_lat, merchant_lon = generate_location_around(
            card_info["HomeLatitude"],
            card_info["HomeLongitude"],
            random.uniform(0, LOCATION_VARIATION_KM_NORMAL)
        )
        # Increment time normally
        time_increment = random.expovariate(1.0 / AVG_TRANSACTION_DELAY_SECONDS)
        current_time = generate_timestamp(current_time, time_increment)


    transaction_id = str(uuid.uuid4())
    merchant_id = f"MERCHANT_{random.randint(1000, 9999)}"

    # Create the transaction record
    transaction_data = {
        "TransactionID": transaction_id,
        "CardID": card_id,
        # Use ISO 8601 format with Z for UTC - common and easily parsed
        "Timestamp": current_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "Amount": amount,
        "MerchantID": merchant_id,
        "MerchantLatitude": merchant_lat,
        "MerchantLongitude": merchant_lon,
        "IsFraud": is_fraud,
         # Include home location for potential direct use or verification
        "HomeLatitude": card_info["HomeLatitude"],
        "HomeLongitude": card_info["HomeLongitude"]
    }

    # --- Output ---
    json_output = json.dumps(transaction_data)
    print(json_output) # Print JSON to console

    # Optional: Send to Kinesis
    if SEND_TO_KINESIS:
        if not send_to_kinesis(transaction_data, KINESIS_STREAM_NAME, card_id):
             print(f"Failed to send TXN {transaction_id} to Kinesis. Stopping.")
             # break # Optional: Stop if Kinesis fails

    # --- Update card state ---
    card_info["LastTxTimestamp"] = current_time
    card_info["LastTxLatitude"] = merchant_lat
    card_info["LastTxLongitude"] = merchant_lon

    # --- Simulate Delay ---
    # Add slight randomness to delay
    delay = max(0.05, random.gauss(AVG_TRANSACTION_DELAY_SECONDS, AVG_TRANSACTION_DELAY_SECONDS / 3))
    # time.sleep(delay) # Uncomment to run in near real-time

print(f"\nFinished generating {NUM_TRANSACTIONS} transactions.")