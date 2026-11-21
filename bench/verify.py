import sys, csv, hashlib
from pathlib import Path
def digest(rows):
    h=hashlib.sha256()
    for row in sorted(rows):
        h.update(('\t'.join(row)+'\n').encode())
    return h.hexdigest()
for name in ('user_daily','campaign_daily'):
    if len(sys.argv)==1:
        with open('dev-answer/'+name+'.csv',newline='',encoding='utf8') as f:
            reader=csv.reader(f)
            next(reader)
            print(name,digest(reader))
    else:
        import polars as pl
        frame=pl.read_parquet(str(Path(sys.argv[1])/name/'*.parquet'))
        actual=digest(tuple('' if x is None else str(x) for x in r) for r in frame.rows())
        print(name,frame.height,actual)
        if len(sys.argv)>2: assert actual in sys.argv[2:]
