import boto3


def create_bucket(bucket_name='bdi-aircraft', region='us-east-1'):
    try:
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Successfully created bucket {bucket_name} in Virginia (us-east-1)")
        return True
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return False

# Create the bucket and list buckets
create_bucket('bdi-aircraft')

# List buckets
s3 = boto3.client('s3', region_name='us-east-1')
response = s3.list_buckets()
print("\nExisting buckets:")
for bucket in response['Buckets']:
    print(f"- {bucket['Name']}")

def main():
    # Create the bucket
    create_bucket('bdi-aircraft')

if __name__ == '__main__':
    main()


