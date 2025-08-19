"""
This script extracts the tasks approved or rejected per annotator their respective reviewer.

Documentation on label logs:
https://docs.encord.com/sdk-documentation/sdk-labels/sdk-activity-logs#log-actions

Label Actions:
* 11: Submit a task
* 33: Approve a task
* 34: Reject a task
"""

from encord import EncordUserClient
import datetime
import pandas as pd
from datetime import date

SSH_PATH = "secrets/encord-alejandra-accelerate-private-key.ed25519"
# PROJECT_ID = "fb5c3af6-f023-4f70-87da-29bc7d4ac658"  # Yutori - Aug 11 Delivery
# PROJECT_ID = "ca2111d8-c641-4f89-8a48-4184b4a88328"  # Yutori - Aug 18 Delivery
PROJECT_ID = "1551a512-11d4-4051-bb0c-27d893bda57b"  # Yutori - Aug 25 delivery

# Connect to project
user_client = EncordUserClient.create_with_ssh_private_key(
    ssh_private_key_path=SSH_PATH
)
project = user_client.get_project(PROJECT_ID)


class LabelLog:
    def __init__(self, log_line):
        self.log_hash = log_line.log_hash
        self.user_hash = log_line.user_hash
        self.user_email = log_line.user_email
        self.data_hash = log_line.data_hash
        self.action = log_line.action
        self.created_at = log_line.created_at
        self.identifier = log_line.identifier
        self.feature_hash = log_line.feature_hash
        self.label_name = log_line.label_name
        self.time_taken = log_line.time_taken
        self.frame = log_line.frame

    def to_dict(self):
        return self.__dict__


# Fetch logs from last 7 days
label_logs = project.get_label_logs(
    after=datetime.datetime.now() - datetime.timedelta(weeks=2)
)
log_objects = [LabelLog(log) for log in label_logs]
df = pd.DataFrame([log.to_dict() for log in log_objects])

# Sort by time
df = df.sort_values("created_at")

# Separate submit and review logs
submits = df[df["action"] == 11].copy()
reviews = df[df["action"].isin([34])].copy()

submits = submits.rename(columns={"created_at": "submitted_at"})
reviews = reviews.rename(columns={"created_at": "reviewed_at"})

# Merge: get first review after each submit
merged = pd.merge_asof(
    submits.sort_values("submitted_at"),
    reviews.sort_values("reviewed_at"),
    by="data_hash",
    left_on="submitted_at",
    right_on="reviewed_at",
    direction="forward",
    suffixes=("_submitter", "_reviewer")
)

# Select and rename columns
result = merged.rename(columns={
    "user_email_submitter": "annotator",
    "user_email_reviewer": "reviewer",
    "action_reviewer": "action",
})[["data_hash", "annotator", "submitted_at", "reviewer", "action", "reviewed_at"]]

result = result.dropna(subset=["reviewer", "action"])

result["action"] = result["action"].map({33: "approve", 34: "reject"})
result = result.sort_values("annotator")

# Save to CSV
today = date.today().isoformat()
result.to_csv(f"task_outcome_{today}.csv", index = False)
