from encord import EncordUserClient
import datetime
import pandas as pd
from datetime import date
import json

# Config
SSH_PATH = "../secrets/encord-alejandra-accelerate-private-key.ed25519"
PROJECT_ID = "ca2111d8-c641-4f89-8a48-4184b4a88328"

# Labels of interest
target_labels = {
    "UI grounding": "1. UI Grounding: incorrect mouse action compared to model's intended action",
    "Thought verification": "10. Thought Verification Error: model does not acknowledge or notice error in previous step",
    "UI hallucination": "12. UI / Visual Hallucination: hallucinates UI elements that don’t exist or misinterprets visual information",
    "Early stopping": "4. Early Stopping (Premature Task Satisfaction): assuming task is done even when task not complete (Default)"
}

# Label action IDs
APPROVE = 12
REJECT = 13
SUBMIT = 28

# Fetch data
user_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=SSH_PATH)
project = user_client.get_project(PROJECT_ID)
label_logs = project.get_label_logs(after=datetime.datetime.now() - datetime.timedelta(weeks=2))

# Convert to DataFrame
df = pd.DataFrame([vars(log) for log in label_logs])

# Filter relevant actions
reviews = df[df['action'].isin([APPROVE, REJECT])]
annotations = (
    df[df['action'] == SUBMIT]
    .sort_values('created_at')
    .drop_duplicates(subset=['identifier', 'label_name'], keep='last')
    .rename(columns={'user_email': 'annotator'})[['identifier', 'label_name', 'annotator']]
)

# Merge
merged = reviews.merge(annotations, on=['identifier', 'label_name'], how='inner')

# Aggregate counts
counts = merged.groupby(['annotator', 'label_name', 'action']).size().unstack(fill_value=0)
counts[APPROVE] = counts.get(APPROVE, 0)
counts[REJECT] = counts.get(REJECT, 0)
counts['rejection_rate'] = counts[REJECT] / (counts[APPROVE] + counts[REJECT])

# Filter for target labels
counts = counts.reset_index()
results = {}

for short_name, full_label in target_labels.items():
    label_data = counts[counts['label_name'] == full_label]
    label_data = label_data[label_data['rejection_rate'] > 0]
    worst_five = (
        label_data.sort_values('rejection_rate', ascending=False)
        .head(5)[['annotator', 'rejection_rate']]
        .round(3)
    )
    results[short_name] = worst_five

slack_blocks = []
for label, table in results.items():
    slack_blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*{label}*"}})
    for _, row in table.iterrows():
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"- {row['annotator']} | Review rate: {row['rejection_rate']:.1%}"
            }
        })
    slack_blocks.append({"type": "divider"})

print(json.dumps({"blocks": slack_blocks}))
