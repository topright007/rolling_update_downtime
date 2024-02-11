from datetime import datetime

ISO_DATE_MASK = '%Y-%m-%d %H:%M:%S,%f'
ISO_DATE_MASK_NO_MS = '%Y-%m-%d %H:%M:%S'


def parseIsoDate(dateStr: str):
    if len(dateStr) == 19:
        return datetime.strptime(dateStr, ISO_DATE_MASK_NO_MS)
    return datetime.strptime(dateStr, ISO_DATE_MASK)


def formatIsoDate(ts: datetime):
    return ts.strftime(ISO_DATE_MASK)

