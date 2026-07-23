"""Independent HTML table extractor for operating_revenue early files.
Uses stdlib html.parser only (no external deps). Mirrors what the raw file
contains WITHOUT calling the Scala reader. Big5-HKSCS decode like the reader.
"""
import sys
from html.parser import HTMLParser


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.cur = None
        self.cell = None
        self.buf = []

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.cur = []
        elif tag in ("td", "th") and self.cur is not None:
            self.cell = []

    def handle_endtag(self, tag):
        if tag == "tr" and self.cur is not None:
            self.rows.append(self.cur)
            self.cur = None
        elif tag in ("td", "th") and self.cell is not None:
            text = "".join(self.cell).strip()
            self.cur.append(text)
            self.cell = None

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)


def parse(path, enc="big5-hkscs"):
    raw = open(path, "rb").read()
    txt = raw.decode(enc, "replace")
    p = TableParser()
    p.feed(txt)
    return p.rows


if __name__ == "__main__":
    path = sys.argv[1]
    rows = parse(path)
    import collections
    cc = collections.Counter(len(r) for r in rows)
    print("file:", path)
    print("n rows:", len(rows), "cell-count dist:", dict(cc))
    print("--- first 14 rows ---")
    for r in rows[:14]:
        print(len(r), r)
    print("--- sample 10-col data rows ---")
    shown = 0
    for r in rows:
        if len(r) == 10 and r and r[0] != "公司 代號":
            print(r)
            shown += 1
            if shown >= 4:
                break
