from datetime import datetime

ISO_DATE_MASK = '%Y-%m-%d %H:%M:%S,%f'


def parseIsoDate(dateStr: str):
    return datetime.strptime(dateStr, ISO_DATE_MASK)


def formatIsoDate(ts: datetime):
    return ts.strftime(ISO_DATE_MASK)

