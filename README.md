# Setup

## [Create a default Pub/Sub Topic](https://console.cloud.google.com/cloudpubsub/topic/) (1)

## [Create a Cloud Scheduler](https://console.cloud.google.com/cloudscheduler)
Frequency:
0 1 * * * - (example)

Target Type:
Pub/Sub
Topic (1)
Body message (Template example):
```json
{
  "PROJECT_ID": "your-project-id",
  "DATASET_ID": "your-dataset-id",
  "intraday": 1,
  "event_params": {
    "page_location": {"type": "string", "description": "URL of the page"},
    "page_title": {"type": "string", "description": "Title of the page"},
    "ga_session_id": {"type": "integer", "description": "Session ID"}
  },
  "user_properties": {
    "source": {"type": "string", "description": "User Source"}
  }
}
```
## Create a service account and grant the necessary role (2)
```
gcloud iam service-accounts create connect-to-bigquery
gcloud projects add-iam-policy-binding your-project --member="serviceAccount:connect-to-bigquery@your-project.iam.gserviceaccount.com" --role="roles/owner"
```

## [Create a Cloud Function](https://console.cloud.google.com/functions/list)
Trigger type:
Pub/Sub
Topic (1)

In the drop-down execution time settings set the service account you created (2)

You can use the code in this repository as the Cloud Function (Python 3.10 Enviroment)

## Test
You can force run the [Job Scheduled](https://console.cloud.google.com/cloudscheduler) to check if it is working
