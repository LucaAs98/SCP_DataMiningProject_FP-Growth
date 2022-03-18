gsutil -m rm -r -f gs://bucket-dataset-scp/output/
gcloud dataproc jobs submit spark     --cluster=cluster-1     --class=mainClass.MainClassGCP     --jars=gs://bucket-dataset-scp/scp_dataminingproject_fp-growth_2.12-0.1.jar     --region=europe-central2     -- gs://bucket-dataset-scp/input/ gs://bucket-dataset-scp/output/ NonordFPRDD 0 30 10
