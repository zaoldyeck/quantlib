import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from research import db
def main():
    con = db.connect()
    df = con.sql("DESCRIBE raw_quarterly").df()
    print(df.to_markdown())
    
    # Let's also check a sample of data
    df_sample = con.sql("SELECT * FROM raw_quarterly WHERE company_code = '2059' ORDER BY year DESC, quarter DESC LIMIT 2").df()
    print(df_sample.to_markdown())

if __name__ == "__main__":
    main()
