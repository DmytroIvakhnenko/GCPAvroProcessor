Create new notification for Cloud Storage object creation event

`gsutil notification create -t TOPIC_NAME -f json -e OBJECT_FINALIZE gs://BUCKET_NAME`

Expected result:

`Created notification config projects/_/buckets/BUCKET_NAME/notificationConfigs/1`

https://cloud.google.com/run/docs/triggering/pubsub-push#run_pubsub_handler-java