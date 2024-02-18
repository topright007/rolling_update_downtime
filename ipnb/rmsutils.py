from datetime import datetime

ISO_DATE_MASK = '%Y-%m-%d %H:%M:%S,%f'
ISO_DATE_MASK_NO_MS = '%Y-%m-%d %H:%M:%S'


def parseIsoDate(dateStr: str):
    if len(dateStr) == 19:
        return datetime.strptime(dateStr, ISO_DATE_MASK_NO_MS)
    return datetime.strptime(dateStr, ISO_DATE_MASK).timestamp()


def formatIsoDate(ts: datetime | float):
    toFormat = ts
    if type(ts) == float:
        toFormat = datetime.fromtimestamp(ts)
    return toFormat.strftime(ISO_DATE_MASK)
