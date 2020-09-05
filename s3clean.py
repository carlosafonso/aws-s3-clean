#!/usr/bin/env python
import argparse
import boto3
import re


def filter_buckets(buckets, regex=None):
    if not regex:
        return buckets

    def filter_fn(bucket):
        return re.match(regex, bucket['Name']) is not None
    return list(filter(filter_fn, buckets))


def delete_bucket(bucket):
    print("Emptying bucket...")
    object_deletion_response = bucket.objects.all().delete()
    deleted_objects_count = sum(map(lambda x: len(x['Deleted']), object_deletion_response))
    print("Removed {} objects".format(deleted_objects_count))

    print("Removing bucket...")
    bucket.delete()


def main(filter_regex=None):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    buckets = filter_buckets(s3_client.list_buckets()['Buckets'], filter_regex)

    for bucket in buckets:
        reply = None
        while True:
            reply = input("Delete bucket {}? [y/N] ".format(bucket['Name'])).strip().lower()
            if reply in ['y', 'n', '']:
                break
            print("Enter 'y' or 'n'")

        if reply != 'y':
            continue

        try:
            delete_bucket(s3_resource.Bucket(bucket['Name']))
        except Exception as e:
            print("Error: {}".format(str(e)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deletes S3 buckets, even if they are not empty.')
    parser.add_argument('--filter',
                        '-f',
                        help='A regex to optionally filter bucket names')
    args = parser.parse_args()
    main(args.filter)
