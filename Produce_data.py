import re
import datetime
from pyspark.sql import Row

# 定义函数解析日志行

APACHE_LOG_PATTERN = r'^(\S+) (\S+) (\S+) [([\w:/]+\s[+\-]\d{4})] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)'
month_map = {'May': 5}

def parse_apache_time(s):
    return datetime.datetime(int(s[7:11]), month_map[s[3:6]],
                             int(s[0:2]), int(s[12:14]),
                             int(s[15:17]), int(s[18:20]))

def parse_apache_logline(logline):
    match = re.search(APACHE_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = int(match.group(9))
    return (Row(host=match.group(1),
                client_ident=match.group(2),
                user_id=match.group(3),
                date_time=match.group(4),
                method=match.group(5),
                endpoint=match.group(6),
                protocol=match.group(7),
                response=int(match.group(8)),
                content_size=size
                ), 1)

