# coding:utf-8


"""
统计每天的数据量详情
"""

import logging
from urlparse import urljoin
import requests
from types import DictType, ListType, GeneratorType
from datetime import timedelta
from datetime import datetime
from urllib import quote
from pymongo import MongoClient

DEBUG = True

NEW_USER = ""
NEW_PASSWORD = quote("")
if DEBUG:
    NEW_HOST_PORT = "公网IP:27017"
else:
    NEW_HOST_PORT = "内网IP:27017"
NEW_DATABASE = "thirdparty"
NEW_MONGO_URL = "mongodb://{0}:{1}@{2}/{3}".format(NEW_USER, NEW_PASSWORD, NEW_HOST_PORT, NEW_DATABASE)
MONGO_URL = NEW_MONGO_URL
client_t = MongoClient(host=MONGO_URL, maxPoolSize=1, minPoolSize=1)


class Stat(object):
    def __init__(self):
        self.db_t = client_t.get_default_database()
        self.forms = ["news", "joke", "video", "atlas", "picture"]
        self.store_goal = "day_stat"

    def get_time_info(self, date_time=None):
        if date_time:
            now_bj = date_time
        else:
            now_bj = datetime.now()
        today_from_bj = datetime(year=now_bj.year,
                                 month=now_bj.month,
                                 day=now_bj.day)
        yestoday_from_bj = today_from_bj - timedelta(hours=24)
        yestoday_from_utc = yestoday_from_bj - timedelta(hours=8)
        yestoday = yestoday_from_bj.strftime("%Y-%m-%d")
        return yestoday_from_utc, yestoday

    def upload(self, col, doc):
        try:
            self.db_t[col].insert(doc)
        except Exception as e:
            logging.warning(e)

    def show(self, doc):
        print doc

    def stat(self, t_from):
        return list()

    def run(self, date_time=None):
        t_from, yestoday = self.get_time_info(date_time)
        result = self.stat(t_from=t_from)

        if isinstance(result, DictType):
            result["day"] = yestoday
            result_doc = result
        elif isinstance(result, GeneratorType) or isinstance(result, ListType):
            result_doc = list()
            for i in result:
                i["day"] = yestoday
                result_doc.append(i)
        else:
            logging.error("Unkown Error")
            return
        self.upload(self.store_goal, result_doc)


class StatByForm(Stat):
    def __init__(self):
        super(StatByForm, self).__init__()
        self.store_goal = "day_stat_form"

    def stat(self, t_from):
        t_to = t_from + timedelta(hours=24)
        doc = dict()
        doc["data"] = {}
        for form in self.forms:
            condition = {"time": {"$gt": t_from, "$lt": t_to}}
            count_num = self.db_t["v1_" + form].count(condition)
            doc["data"][form] = count_num
        return doc


class StatBySite(Stat):
    def __init__(self):
        super(StatBySite, self).__init__()
        self.store_goal = "网址/scores"
        self.group_id = 7
        self.subject_id = 1

    def upload(self, url, doc):
        report_list = list()
        if isinstance(doc, DictType):
            report_list.append(doc)
        elif isinstance(doc, ListType):
            report_list = doc
        else:
            logging.warning("Unknown type")
            return
        for report in report_list:
            params = dict()
            params["score_name"] = report["name"]
            params["score"] = str(report["total"])
            params["date"] = report["day"]
            params["group_id"] = self.group_id
            params["subject_id"] = self.subject_id
            self.post(self.store_goal, params)

    def post(self, url, params):
        try:
            resp = requests.post(url, data=params)
            content = resp.content
        except Exception as e:
            logging.warning(e)
            content = ""
        return content

    def stat(self, t_from):
        t_to = t_from + timedelta(hours=24)
        sites = list(self.db_t.spider_sites.find())
        site_docs = list()
        for num, site in enumerate(sites):
            site_doc = dict()
            sid = str(site["_id"])
            site_doc["name"] = site["name"]
            site_doc["total"] = 0
            condition = {
                "time": {"$gt": t_from, "$lt": t_to},
                "site": sid
            }
            for form in self.forms:
                count_num = self.db_t["v1_" + form].count(condition)
                site_doc["total"] += count_num
            site_docs.append(site_doc)
        return site_docs


def main():
    logging.info("Start:Stat-Form")
    form_stat = StatByForm()
    form_stat.run()
    logging.info("End:Stat-Form")
    logging.info("Start:Site-Form")
    site_stat = StatBySite()
    site_stat.run()
    logging.info("End:Site-Form")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        filename="day_stat.log",
                        filemode="a+")
    main()
