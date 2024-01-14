import json
from datetime import datetime
from scipy.stats import percentileofscore

class UserManager:
    def __init__(self):
        self.users = {}

    def create_user(self, username, age, gender):
        if username in self.users:
            return {"status": "error", "message": f"User {username} already exists."}
        
        self.users[username] = {"age": age, "gender": gender, "vitals": {}}
        return {"status": "success", "message": f"User {username} created successfully."}

    def insert_vital(self, username, vital_id, value, timestamp):
        if username not in self.users:
            return {"status": "error", "message": f"User {username} does not exist."}

        vitals = self.users[username].get("vitals", {})
        
        if vital_id not in vitals:
            vitals[vital_id] = {"values": [], "timestamps": []}

        vitals[vital_id]["values"].append(value)
        vitals[vital_id]["timestamps"].append(timestamp)

        self.users[username]["vitals"] = vitals
        return {"status": "success", "message": f"Vital {vital_id} for {username} inserted successfully."}

    def aggregate_vitals(self, username, vital_ids, start_timestamp, end_timestamp):
        if username not in self.users:
            return {"status": "error", "message": f"User {username} does not exist."}

        aggregates = {}
        user_vitals = self.users[username].get("vitals", {})

        for vital_id in vital_ids:
            vital_data = user_vitals.get(vital_id, {})
            values = vital_data.get("values", [])

            if values:
                mean_value = sum(values) / len(values)
                aggregates[vital_id] = mean_value

        return {
            "status": "success",
            "message": "Aggregate fetched successfully.",
            "data": {
                "username": username,
                "aggregates": aggregates,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp
            }
        }
    
    def population_insight(self, username, vital_id, start_timestamp, end_timestamp):
        if username not in self.users:
            return {"status": "error", "message": f"User {username} does not exist."}

        user_aggregates = self.aggregate_vitals(username, [vital_id], start_timestamp, end_timestamp)
        user_value = user_aggregates["data"]["aggregates"].get(vital_id, 0)

        population_values = []
        for other_username, other_user_data in self.users.items():
            user_aggregates = self.aggregate_vitals(other_username, [vital_id], start_timestamp, end_timestamp)
            population_values.append(user_aggregates["data"]["aggregates"].get(vital_id, 0))

        percentile_rank = percentileofscore(population_values, user_value)

        insight_message = f"Your {vital_id} is in the {round(percentile_rank)}th percentile."

        return {
            "status": "success",
            "message": "Population insight fetched successfully.",
            "data": {
                "username": username,
                "vital_id": vital_id,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "insight": insight_message
            }
        }

    def process_command(self, command):
        if command["command"] == "create_user":
            return self.create_user(command["username"], command["age"], command["gender"])
        elif command["command"] == "insert_vital":
            return self.insert_vital(
                command["username"],
                command["vital_id"],
                command["value"],
                command["timestamp"]
            )
        elif command["command"] == "aggregate":
            return self.aggregate_vitals(
                command["username"],
                command["vital_ids"],
                command["start_timestamp"],
                command["end_timestamp"]
            )
        elif command["command"] == "population_insight":
            return self.population_insight(
                command["username"],
                command["vital_id"],
                command["start_timestamp"],
                command["end_timestamp"]
            )
        else:
            return {"status": "error", "message": f"Unknown command: {command['command']}"}


def main():
    # Read input from the JSON file
    with open("input.json") as file:
        commands = json.load(file)

    # Initialize user manager
    user_manager = UserManager()

    # Process each command
    output = []
    for command in commands:
        result = user_manager.process_command(command)
        output.append(result)

    # Print the output
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
