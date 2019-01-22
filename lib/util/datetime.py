import datetime
import re

yesterday_date = datetime.date.today() - datetime.timedelta(days=1)

class timestamp_re:
    yesterday = r"%d%02d%02d" % (yesterday_date.year, yesterday_date.month, yesterday_date.day)
    date = r"(19|20)\d\d[01]\d[0123]\d"
    time = r"(0[0-9]|1[0-9]|2[0-3])[0-5][0-9]"
    date_time = date + r"_" + time

    def fullmatch_yesterday(str):
        return re.fullmatch(timestamp_re.yesterday, str)

    def search_date(str):
        return re.search(timestamp_re.date, str)

    def search_date_time(str):
        return re.search(timestamp_re.date_time, str)


class timestamp_parser:
    strpformat = '%Y-%m-%dT%H:%M:%S'
    duration_re = re.compile(r'((?P<hours>\d+?)\shours?)?((?P<minutes>\d+?)\sminutes?)?((?P<seconds>\d+?)\sseconds?)?')

    def parse_datetime(string):
        string = string[0:19] # drop timezone info

        return datetime.datetime.strptime(string, timestamp_parser.strpformat) 


    def parse_duration(string):
        parts = timestamp_parser.duration_re.match(string)

        if not parts:
            return

        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.items():
            if param:
                time_params[name] = int(param)
        
        return datetime.timedelta(**time_params)

    def to_str(datetime):
        return datetime.strftime("%Y-%m-%dT%H:%M:%S")


class timestamp_range_generator():
    def __init__(self, days=18):
        # create datetime range FROM 18-days-ago at 00:00:00 TO today at 23:59:59
        self.start = datetime.datetime.today() - datetime.timedelta(days=days)
        self.start = self.start.replace(hour=0, minute=0, second=0, microsecond=0)

        self.end = datetime.datetime.today()
        self.end = self.end.replace(hour=23, minute=59, second=59, microsecond=999999)


        self.start_timestamp = self.start.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.end_timestamp = self.end.strftime("%Y-%m-%dT%H:%M:%SZ")

        self.date_stamp = self.end.strftime("%Y-%m-%d")
        self.datetime_stamp = self.end_timestamp