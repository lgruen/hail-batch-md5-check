import os
import sys
import hailtop.batch as hb

backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)

batch = hb.Batch(backend=backend, name='md5 check')

with open(sys.argv[1]) as f:
    for line in f:
        filename = line.strip()
        job = batch.new_job(name=filename)
        job.image(
            'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:a0dfa8b526a156379b2864bcf88fd008c4f2913a-hail-0.2.70.dev408ec3c82449'
        )
        job.command(
            'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
        )
        job.command(f'gsutil cat "{filename[:-4]}" | md5sum > /tmp/checksum.md5')
        job.command(
            f'diff <(cat /tmp/checksum.md5 | cut -d " " -f1 ) <(gsutil cat "{filename}" | cut -d " " -f1 )'
        )

batch.run()
