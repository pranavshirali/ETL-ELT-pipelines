#!/usr/bin/env python
import snowflake.connector

ctx = snowflake.connector.connect(
    user='pranavshirali',
    password='PranavShirali@123',
    account='kfxfkqq-pt64643'
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()
