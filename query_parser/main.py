from moz_sql_parser import parse
import json
import sqlparse
import re

query = """WITH trans AS ( SELECT account_id, date, type, operation, amount, balance, k_symbol, bank, account, trans_id FROM `id-bi-staging.playground_dev.public_dataset_financial_trans` WHERE date >= '1998-12-01'), account AS ( SELECT district_id, frequency, date, account_id FROM `id-bi-staging.playground_dev.public_dataset_financial_account`), district AS ( SELECT A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, district_id FROM `id-bi-staging.playground_dev.public_dataset_financial_district`), disp AS ( SELECT client_id, account_id, type, disp_id FROM `id-bi-staging.playground_dev.public_dataset_financial_disp`), client AS ( SELECT gender, birth_date, district_id, client_id FROM `id-bi-staging.playground_dev.public_dataset_financial_client`) SELECT trans.date, trans.type AS trans_type, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account, account.frequency, account.account_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16, district.district_id, disp.type, disp.type as disp_type, client.gender, client.birth_date FROM trans JOIN account ON trans.account_id = account.account_id JOIN district ON account.district_id = district.district_id JOIN disp ON disp.account_id = account.account_id JOIN client ON disp.client_id = client.client_id"""

findall = re.findall("\(.*?\)", query)
bad_chars = [')', '(']
for a in findall:
    # b = filter(lambda i: i not in bad_chars, a)
    b = ''.join(i for i in a if not i in bad_chars)
    print(b)

findb = re.findall("\).*?\(", query)
bad_chars = [')', '(', ',']
for a in findb:
    b = ''.join(i for i in a if not i in bad_chars)
    b = b.replace(' AS', '')
    print(b)

findc = query.split("(")
b = findc[0].replace('WITH ', '').replace(' AS', '')
print(b)

findd = query.split(")")
print(findd[-1])
# dumps = json.dumps(parse(query))
# print(dumps)

# print(sqlparse.parse(query))
# parse_ = json.dumps(sqlparse.parse(query))
# print(parse_)
# print(type(parse_[0].tokens))
# for a in parse_[0].tokens:
#     print(a)
