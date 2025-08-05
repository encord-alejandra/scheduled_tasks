# label_log_analysis.py

from encord import EncordUserClient
import datetime
import pandas as pd

# Constants
SSH_PATH = "../secrets/encord-alejandra-accelerate-private-key.ed25519"
PROJECT_ID = "85317f8b-dc97-4866-9bdd-2ccc0dc9285f"  # Yutori [TEST]

# Log actions reference:
# 3: Start labeling
# 4: End labeling
# 13: Reject a label
# 29: Submit an attribute for review
# 28: Submit an object or a classification label for review
# 31: A bitrate warning was shown
# 12: Approve a label
# 11: Submit a task
# 30: Buffering icon appeared
# 33: Approve a task

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

def main():
    # Initialize client
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_PATH
    )

    project = user_client.get_project(PROJECT_ID)

    # Fetch label logs from the last week
    label_logs = project.get_label_logs(
        after=datetime.datetime.now() - datetime.timedelta(weeks=1)
    )

    # Convert logs to DataFrame
    log_objects = [LabelLog(log_line) for log_line in label_logs]
    all_labels = pd.DataFrame([log.to_dict() for log in log_objects])

    # Filter logs by action type
    rejections = all_labels[all_labels['action'] == 13]
    submissions = all_labels[all_labels['action'] == 28]

    # Count submissions per user and task type
    submitted_counts = (
        submissions
        .groupby(['user_email', 'label_name'])
        .size()
        .reset_index(name='submitted')
    )

    # Match rejections with corresponding submissions
    merged = pd.merge(
        rejections,
        submissions,
        on='identifier',
        suffixes=('_rejection', '_submission')
    )

    # Keep only submissions before the rejection
    merged_timed = merged[merged['created_at_submission'] < merged['created_at_rejection']]

    # Get latest submission before each rejection
    merged_timed_ordered = (
        merged_timed
        .sort_values('created_at_submission')
        .groupby(['identifier', 'created_at_rejection'])
        .tail(1)
    )

    # Count rejections per user and task type
    rejected_counts = (
        merged_timed_ordered
        .groupby(['user_email_submission', 'label_name_submission'])
        .size()
        .reset_index(name='rejected')
        .rename(columns={
            'user_email_submission': 'user_email',
            'label_name_submission': 'label_name'
        })
    )

    # Merge with submission counts and calculate rejection rates
    stats = pd.merge(submitted_counts, rejected_counts, on=['user_email', 'label_name'], how='left')
    stats['rejected'] = stats['rejected'].fillna(0)
    stats['rejection_rate'] = stats['rejected'] / stats['submitted']

    # Pivot result to have rejection rates per annotator and task type
    result = stats.pivot(index='user_email', columns='label_name', values='rejection_rate')

    # Output result
    print(result)

if __name__ == "__main__":
    main()
