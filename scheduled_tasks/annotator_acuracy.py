"""
This script extracts the label logs for a project and calculates label rejection rates per annotator.

Rajection rate = Rejections / ( Approvals + Rejections )

Documentation on label logs:
https://docs.encord.com/sdk-documentation/sdk-labels/sdk-activity-logs#log-actions

Label Actions:
* 12: Approve a label
* 13: Reject a label
* 28: Submit an object or a classification label for review
"""

from encord import EncordUserClient
import datetime
import pandas as pd
from datetime import date

# Constants
SSH_PATH = "secrets/encord-alejandra-accelerate-private-key.ed25519"
PROJECT_ID = "fb5c3af6-f023-4f70-87da-29bc7d4ac658"  # Yutori - Aug 11 Delivery
# PROJECT_ID = "85317f8b-dc97-4866-9bdd-2ccc0dc9285f"  # Yutori [TEST]

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

# Fetch label logs
user_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_PATH)
project = user_client.get_project(PROJECT_ID)

label_logs = project.get_label_logs(after=datetime.datetime.now() - datetime.timedelta(weeks=1))
log_objects = [LabelLog(log) for log in label_logs]
all_labels = pd.DataFrame([log.to_dict() for log in log_objects])

# Filter by action type
reviews = all_labels[all_labels['action'].isin([12, 13])]
annotations = all_labels[all_labels['action'] == 28]

# Find the latest submission per annotation id and label name
annotations = (
    annotations
    .sort_values('created_at')
    .drop_duplicates(subset=['identifier', 'label_name'], keep='last')
    .rename(columns={'user_email': 'annotator'})[['identifier', 'label_name', 'annotator']]
)

# Merge annotations with reviews
df = reviews.merge(annotations, on=['identifier', 'label_name'], how='inner')

# Counts approvals and rejections
review_outcome = df.groupby(['annotator', 'label_name', 'action']).size().unstack(fill_value=0)
review_outcome[12] = review_outcome.get(12, 0)
review_outcome[13] = review_outcome.get(13, 0)

# Calculate accuracy
review_outcome['accuracy'] = review_outcome[13] / (review_outcome[12] + review_outcome[13])

# Format output
accuracy_table = review_outcome['accuracy'].unstack(fill_value=0)
accuracy_table.index.name = 'user_email'
accuracy_table.columns.name = None
accuracy_table = accuracy_table.round(3)

today = date.today().isoformat()
accuracy_table.to_csv(f"annotator_accuracy_{today}.csv")
