COPY annuity_policies
FROM 's3://annuity-bucketbroooooooo/data/annuity_policies.csv'
IAM_ROLE 'arn:aws:iam::648618456228:role/redshif-annuity-s3'
CSV
IGNOREHEADER 1
REGION 'us-east-2'
DATEFORMAT 'YYYY-MM-DD';