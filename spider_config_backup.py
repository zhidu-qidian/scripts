# coding: utf-8

""" 备份爬虫关键表数据到阿里云 oss """

from datetime import datetime
from urllib import quote
from hashlib import sha256
from StringIO import StringIO
from types import FileType

from bson import json_util
from pymongo import MongoClient
import oss2

MONGODB_HOST_PORT = "内网地址:27017"
MONGODB_PASSWORD = ""


def get_mongodb_database(database, user="third"):
    url = "mongodb://{0}:{1}@{2}/{3}".format(
        user, quote(MONGODB_PASSWORD), MONGODB_HOST_PORT, database
    )
    client = MongoClient(host=url, maxPoolSize=1, minPoolSize=1)
    return client.get_default_database()


class ObjectUploader(object):

    access_key_id = ""
    access_key_secret = ""

    def __init__(self, endpoint, name, domain=None):
        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        self.domain = domain if domain else endpoint
        self.bucket = oss2.Bucket(auth=auth, endpoint=endpoint, bucket_name=name)

    def upload(self, data, name=None, suffix=None, headers=None):
        if name is None:
            if isinstance(data, (FileType, StringIO)):
                data = data.read()
            name = sha256(data).hexdigest()
            if suffix is not None:
                name = "%s.%s" % (name, suffix)
        retry = 3
        for i in range(retry):
            try:
                self.bucket.put_object(name, data, headers=headers)
            except Exception:
                if i == retry-1:
                    raise
        return "%s/%s" % (self.domain, name)

    def download(self, name):
        result = self.bucket.get_object(key=name)
        data = result.read()
        return data


def main():
    names = ["company", "data_category_one", "data_category_two", "data_forms", "qidian_map",
             "source_status", "spider_advertisements", "spider_channels", "spider_configs",
             "spider_sites", "timerules", "region_map"]
    thirdparty = get_mongodb_database("thirdparty", "third")
    endpoint = "OSS地址"
    bucket = "spider-backup"
    domain = "OSS-domain"
    client = ObjectUploader(endpoint=endpoint, name=bucket, domain=domain)
    now = datetime.now().strftime("%Y-%m-%d.")
    for name in names:
        print "start backup %s" % name
        key = now+name+".bak.json"
        col = thirdparty[name]
        docs = list(col.find())
        data = json_util.dumps(docs)
        url = client.upload(data, key)
        print url
        #
        # data = client.download(key)
        # docs = json_util.loads(data)
        # col = thirdparty["test--"+name]
        # result = col.insert_many(docs)
        # print "insert %s count %s" % (name, len(result.inserted_ids))


if __name__ == "__main__":
    main()
