# coding: utf-8

""" 移除 patianxia, thirdparty 老的数据, 节省磁盘空间 """

from datetime import datetime, timedelta
from urllib import quote

from pymongo import MongoClient

MONGODB_HOST_PORT = "内网地址:27017"
MONGODB_PASSWORD = ""


def get_mongodb_database(database, user="third"):
    url = "mongodb://{0}:{1}@{2}/{3}".format(
        user, quote(MONGODB_PASSWORD), MONGODB_HOST_PORT, database
    )
    client = MongoClient(host=url, maxPoolSize=1, minPoolSize=1)
    return client.get_default_database()


def delete(query, col):
    n = 0
    while 1:
        items = col.find(query, projection={"_id": 1}).limit(1000)
        ids = [item["_id"] for item in items]
        result = col.delete_many({"_id": {"$in": ids}})
        if result.deleted_count == 0:
            break
        else:
            n += result.deleted_count
        if n % 5000 == 0:
            print datetime.now(), n


def main():
    patianxia = get_mongodb_database("patianxia", "spider")
    thirdparty = get_mongodb_database("thirdparty", "third")
    v2_request = patianxia["v2_requests"]
    v1_request = thirdparty["v1_request"]
    t = datetime.utcnow() - timedelta(days=7)
    delete({"insert": {"$lt": t}}, v2_request)
    t = datetime.utcnow() - timedelta(days=60)
    delete({"time": {"$lt": t}}, v1_request)


if __name__ == "__main__":
    main()
