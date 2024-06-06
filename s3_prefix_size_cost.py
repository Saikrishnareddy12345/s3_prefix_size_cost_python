# import boto3
# from collections import defaultdict
# import pandas as pd

# def list_all_prefixes(bucket_name):
#     s3 = boto3.client('s3')
#     prefixes = set()
#     continuation_token = None
    
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/', ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        
#         for prefix in response.get('CommonPrefixes', []):
#             prefixes.add(prefix['Prefix'])
        
#         if response.get('IsTruncated'):
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
    
#     return prefixes

# def get_objects_data(bucket_name, prefix):
#     s3 = boto3.client('s3')
#     objects = []
#     continuation_token = None
    
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
#         objects.extend(response.get('Contents', []))
        
#         if response.get('IsTruncated'):
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
    
#     return objects

# def calculate_storage_class_data(objects):
#     storage_class_data = defaultdict(int)
#     prefix_details = []
    
#     for obj in objects:
#         storage_class = obj['StorageClass']
#         size = obj['Size']
#         storage_class_data[storage_class] += size
#         prefix_details.append({
#             'Prefix': '/'.join(obj['Key'].split('/')[:-1]) + '/',
#             'Object': obj['Key'],
#             'Size': size,
#             'StorageClass': storage_class
#         })
    
#     return storage_class_data, prefix_details

# def write_storage_class_to_excel(storage_class_data, file_name):
#     df = pd.DataFrame(list(storage_class_data.items()), columns=['StorageClass', 'TotalSize (bytes)'])
#     df.to_excel(file_name, index=False)
#     print(f"Storage class data written to {file_name}")

# def write_prefix_details_to_excel(prefix_details, file_name):
#     df = pd.DataFrame(prefix_details, columns=['Prefix', 'Object', 'Size', 'StorageClass'])
#     df.to_excel(file_name, index=False)
#     print(f"Prefix details written to {file_name}")

# def main(bucket_name):
#     prefixes = list_all_prefixes(bucket_name)
#     total_storage_class_data = defaultdict(int)
#     all_prefix_details = []
    
#     for prefix in prefixes:
#         print(f"Prefix: {prefix}")
#         objects = get_objects_data(bucket_name, prefix)
#         for obj in objects:
#             print(f"  Object: {obj['Key']}, Size: {obj['Size']} bytes, Storage Class: {obj['StorageClass']}")
        
#         storage_class_data, prefix_details = calculate_storage_class_data(objects)
#         all_prefix_details.extend(prefix_details)
        
#         for storage_class, size in storage_class_data.items():
#             total_storage_class_data[storage_class] += size
    
#     print("\nTotal data in each storage class:")
#     for storage_class, size in total_storage_class_data.items():
#         print(f"Storage Class: {storage_class}, Total Size: {size} bytes")
    
#     write_storage_class_to_excel(total_storage_class_data, 's3_storage_class_data.xlsx')
#     write_prefix_details_to_excel(all_prefix_details, 's3_prefix_details.xlsx')

# if __name__ == "__main__":
#     bucket_name = 'saikrishna-s3'  # Replace with your S3 bucket name
#     main(bucket_name)

# import boto3
# from collections import defaultdict
# import pandas as pd

# def list_all_prefixes(bucket_name):
#     s3 = boto3.client('s3')
#     prefixes = set()
#     continuation_token = None
    
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/', ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        
#         for prefix in response.get('CommonPrefixes', []):
#             prefixes.add(prefix['Prefix'])
        
#         if response.get('IsTruncated'):
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
    
#     return prefixes

# def get_objects_data(bucket_name, prefix):
#     s3 = boto3.client('s3')
#     objects = []
#     continuation_token = None
    
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
#         objects.extend(response.get('Contents', []))
        
#         if response.get('IsTruncated'):
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
    
#     return objects

# def calculate_storage_class_data(objects):
#     storage_class_data = defaultdict(int)
#     prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
    
#     for obj in objects:
#         storage_class = obj['StorageClass']
#         size = obj['Size']
#         storage_class_data[storage_class] += size
        
#         prefix_path = '/'.join(obj['Key'].split('/')[:-1]) + '/'
#         prefix_details[prefix_path]['TotalSize'] += size
#         prefix_details[prefix_path]['StorageClass'][storage_class] += size
    
#     return storage_class_data, prefix_details

# def write_storage_class_to_excel(storage_class_data, file_name):
#     df = pd.DataFrame(list(storage_class_data.items()), columns=['StorageClass', 'TotalSize (bytes)'])
#     df.to_excel(file_name, index=False)
#     print(f"Storage class data written to {file_name}")

# def write_prefix_details_to_excel(prefix_details, file_name):
#     rows = []
#     for prefix, details in prefix_details.items():
#         for storage_class, size in details['StorageClass'].items():
#             rows.append({
#                 'Prefix': prefix,
#                 'TotalSize': details['TotalSize'],
#                 'StorageClass': storage_class,
#                 'Size': size
#             })
    
#     df = pd.DataFrame(rows, columns=['Prefix', 'TotalSize', 'StorageClass', 'Size'])
#     df.sort_values(by='TotalSize', ascending=False, inplace=True)
#     df.to_excel(file_name, index=False)
#     print(f"Prefix details written to {file_name}")

# def main(bucket_name, top_n):
#     prefixes = list_all_prefixes(bucket_name)
#     total_storage_class_data = defaultdict(int)
#     all_prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
    
#     for prefix in prefixes:
#         print(f"Prefix: {prefix}")
#         objects = get_objects_data(bucket_name, prefix)
#         for obj in objects:
#             print(f"  Object: {obj['Key']}, Size: {obj['Size']} bytes, Storage Class: {obj['StorageClass']}")
        
#         storage_class_data, prefix_details = calculate_storage_class_data(objects)
        
#         for storage_class, size in storage_class_data.items():
#             total_storage_class_data[storage_class] += size
        
#         for prefix, details in prefix_details.items():
#             all_prefix_details[prefix]['TotalSize'] += details['TotalSize']
#             for storage_class, size in details['StorageClass'].items():
#                 all_prefix_details[prefix]['StorageClass'][storage_class] += size
    
#     # Write the total storage class data to Excel
#     write_storage_class_to_excel(total_storage_class_data, 's3_storage_class_data.xlsx')
    
#     # Sort and filter the top N largest sub-prefixes
#     sorted_prefix_details = sorted(all_prefix_details.items(), key=lambda item: item[1]['TotalSize'], reverse=True)
#     top_prefix_details = dict(sorted_prefix_details[:top_n])
    
#     # Write the prefix details to Excel
#     write_prefix_details_to_excel(top_prefix_details, 's3_prefix_details.xlsx')

# if __name__ == "__main__":
#     bucket_name = 'saikrishna-s3'  # Replace with your S3 bucket name
#     top_n = int(input("Enter the number of top largest sub-prefixes to display: "))
#     main(bucket_name, top_n)

import boto3
from collections import defaultdict
import pandas as pd

# Pricing structure for different storage classes in USD per GB per month
# Update these values according to the latest AWS pricing
STORAGE_CLASS_PRICING = {
    'STANDARD': 0.023,
    'INTELLIGENT_TIERING': 0.023,
    'STANDARD_IA': 0.0125,
    'ONEZONE_IA': 0.01,
    'GLACIER': 0.004,
    'DEEP_ARCHIVE': 0.00099
}

def list_all_prefixes(bucket_name):
    s3 = boto3.client('s3')
    prefixes = set()
    continuation_token = None
    
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/', ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        
        for prefix in response.get('CommonPrefixes', []):
            prefixes.add(prefix['Prefix'])
        
        if response.get('IsTruncated'):
            continuation_token = response['NextContinuationToken']
        else:
            break
    
    return prefixes

def get_objects_data(bucket_name, prefix):
    s3 = boto3.client('s3')
    objects = []
    continuation_token = None
    
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        objects.extend(response.get('Contents', []))
        
        if response.get('IsTruncated'):
            continuation_token = response['NextContinuationToken']
        else:
            break
    
    return objects

def calculate_storage_class_data(objects):
    storage_class_data = defaultdict(int)
    prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
    
    for obj in objects:
        storage_class = obj['StorageClass']
        size = obj['Size']
        storage_class_data[storage_class] += size
        
        prefix_path = '/'.join(obj['Key'].split('/')[:-1]) + '/'
        prefix_details[prefix_path]['TotalSize'] += size
        prefix_details[prefix_path]['StorageClass'][storage_class] += size
    
    return storage_class_data, prefix_details

def calculate_cost(storage_class_data):
    storage_class_cost = {}
    for storage_class, size in storage_class_data.items():
        cost_per_gb = STORAGE_CLASS_PRICING.get(storage_class, 0)
        cost = cost_per_gb * (size / (1024**3))  # Convert size from bytes to GB
        storage_class_cost[storage_class] = round(cost, 2)
    return storage_class_cost

def write_storage_class_to_excel(storage_class_data, file_name):
    df = pd.DataFrame(list(storage_class_data.items()), columns=['StorageClass', 'TotalSize (bytes)'])
    df.to_excel(file_name, index=False)
    print(f"Storage class data written to {file_name}")

def write_prefix_details_to_excel(prefix_details, file_name):
    rows = []
    for prefix, details in prefix_details.items():
        cost_data = calculate_cost(details['StorageClass'])
        for storage_class, size in details['StorageClass'].items():
            rows.append({
                'Prefix': prefix,
                'TotalSize': details['TotalSize'],
                'StorageClass': storage_class,
                'Size': size,
                'Cost': cost_data[storage_class]
            })
    
    df = pd.DataFrame(rows, columns=['Prefix', 'TotalSize', 'StorageClass', 'Size', 'Cost'])
    df.sort_values(by='TotalSize', ascending=False, inplace=True)
    df.to_excel(file_name, index=False)
    print(f"Prefix details written to {file_name}")

def main(bucket_name, top_n):
    prefixes = list_all_prefixes(bucket_name)
    total_storage_class_data = defaultdict(int)
    all_prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
    
    for prefix in prefixes:
        print(f"Prefix: {prefix}")
        objects = get_objects_data(bucket_name, prefix)
        for obj in objects:
            print(f"  Object: {obj['Key']}, Size: {obj['Size']} bytes, Storage Class: {obj['StorageClass']}")
        
        storage_class_data, prefix_details = calculate_storage_class_data(objects)
        
        for storage_class, size in storage_class_data.items():
            total_storage_class_data[storage_class] += size
        
        for prefix, details in prefix_details.items():
            all_prefix_details[prefix]['TotalSize'] += details['TotalSize']
            for storage_class, size in details['StorageClass'].items():
                all_prefix_details[prefix]['StorageClass'][storage_class] += size
    
    # Write the total storage class data to Excel
    write_storage_class_to_excel(total_storage_class_data, 's3_storage_class_data.xlsx')
    
    # Sort and filter the top N largest sub-prefixes
    sorted_prefix_details = sorted(all_prefix_details.items(), key=lambda item: item[1]['TotalSize'], reverse=True)
    top_prefix_details = dict(sorted_prefix_details[:top_n])
    
    # Write the prefix details to Excel
    write_prefix_details_to_excel(top_prefix_details, 's3_prefix_details.xlsx')

if __name__ == "__main__":
    bucket_name = 'saikrishna-s3'  # Replace with your S3 bucket name
    top_n = int(input("Enter the number of top largest sub-prefixes to display: "))
    main(bucket_name, top_n)

