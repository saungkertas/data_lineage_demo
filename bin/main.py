import re
import json


def parse2(query):
    query_split = query.split('`')
    for element in query_split:
        if ' ' in element:
            query_split.remove(element)
    return query_split


def parse(query):
    tables = {}
    tables_array = []
    combinations = {}
    combinations_array = []
    payload = {}

    # Stores list of tables made with 'WITH' sql statement
    withDict = []
    STRIP_REGEX = '(#.*)|(((\/\*)+?[\w\W]+?(\*\/)+))'
    if '/*' in query and '*/' in query:
        query = re.sub(STRIP_REGEX, ' ', query)

    # Special edge cases
    query = query.replace("\n", " ")
    query = query.replace("\r", " ")
    query = query.replace("\t", " ")
    query = query.replace("bi-gojek.", "")
    query = query.replace("`bi-gojek`.", "")

    # Parsing is done
    WITH_REGEX = '(?:^|\W)with(?:$|\W)\s*(\S+)'
    pattern = re.compile(WITH_REGEX, re.UNICODE)
    for match in pattern.findall(query):
        withDict.append(match.replace("`", ""))

    querylist = []

    # Special edge cases
    if 'from`' in query:
        query = query.replace("from`", "from `")

    querylist = query.split("from ")

    for q in querylist:
        # Special edge cases
        q = q.replace(u'\xa0', u' ')
        if 'from' in q:
            q = q.lstrip('from')
        if ' ' in q:
            q = q.lstrip(' ')
        try:
            match = q[0: q.index(' ')]
        except:
            match = q
        temp = []

        if '--' not in match and match not in withDict and '.' in match:
            if '`' in match:
                match = match.replace("`", "")
            if ',' in match:
                match = match[0: match.index(',')]
            if '.' in match and '(' not in match:
                check_with_table = match[0: match.index(".")]
                if check_with_table not in withDict:
                    # Special cases for legacy sql
                    match = match.replace("[", "")
                    match = match.replace("]", "")
                    match = match.replace("id-bi-staging:", "")
                    # Special edge cases
                    match = match.replace(")", "")
                    match = match.replace("id-bi-staging.", "")
                    match = match.replace("`", "")
                    temp.append(match)
                    tables[match] = tables.get(match, 0) + 1
        JOIN_REGEX = '(?:^|\W)join(?:$|\W)\s*(\S+)(?:$|\W)'
        join_pattern = re.compile(JOIN_REGEX, re.UNICODE)
        for join_match in join_pattern.findall(q):
            if '(' not in join_match and ')' not in join_match and '--' not in join_match and join_match not in withDict and '.' in join_match:
                if '`' in join_match:
                    last_occurence = join_match.rfind('`')
                    join_match = join_match[0:last_occurence]
                join_match = join_match.replace("`", "")
                if ',' in join_match:
                    join_match = join_match[0: join_match.index(',')]
                # Special cases for legacy sql
                join_match = join_match.replace("[", "")
                join_match = join_match.replace("]", "")
                join_match = join_match.replace("bi-gojek:", "")
                # Special edge cases
                join_match = join_match.replace("bi-gojek.", "")
                temp.append(join_match)
                tables[join_match] = tables.get(join_match, 0) + 1
        if len(temp) > 1:
            temp.sort()
            key = '+'.join(temp)
            combinations[key] = combinations.get(key, 0) + 1
    for k, v in combinations.items():
        if not all([k, v]):
            del combinations[k]
    # last_code = row["code"]  # Final version of code variable
    # last_message = row["message"]  # Final version of message variable

    for key, value in tables.items():
        # Check if there is error in name of tables
        if 'bi-gojek:' in key or 'bi-gojek.' in key or '(' in key or ')' in key or ',' in key or '`' in key or '[' in key or ']' in key:
            last_code = 400
            last_message = "Error in query OR edge case not considered yet"
        tables_array.append({'tableName': key, 'tableFreq': value})
    for key, value in combinations.items():
        # Check if there is error in join tables' name
        if 'bi-gojek:' in key or 'bi-gojek.' in key or '(' in key or ')' in key or ',' in key or '`' in key or '[' in key or ']' in key:
            last_code = 400
            last_message = "Error in query OR edge case not considered yet"
        combinations_array.append({'combName': key, 'combFreq': value})

    # Check if parsed tables is still empty after parsing
    if not tables_array:
        # If empty, give out code and message to check the query or if there is an edge case not considered yet
        last_code = 400
        last_message = "Error in query OR edge case not considered yet"

    return (json.dumps(tables_array))


if __name__ == '__main__':
    query = "WITH trans AS ( SELECT account_id, date, type, operation, amount, balance, k_symbol, bank, account, trans_id FROM `id-bi-staging.playground_dev.public_dataset_financial_trans` WHERE date >= '1998-12-01'), account AS ( SELECT district_id, frequency, date, account_id FROM `id-bi-staging.playground_dev.public_dataset_financial_account`), district AS ( SELECT A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, district_id FROM `id-bi-staging.playground_dev.public_dataset_financial_district`), disp AS ( SELECT client_id, account_id, type, disp_id FROM `id-bi-staging.playground_dev.public_dataset_financial_disp`), client AS ( SELECT gender, birth_date, district_id, client_id FROM `id-bi-staging.playground_dev.public_dataset_financial_client`) SELECT trans.date, trans.type AS trans_type, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account, account.frequency, account.account_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16, district.district_id, disp.type, disp.type as disp_type, client.gender, client.birth_date FROM trans JOIN account ON trans.account_id = account.account_id JOIN district ON account.district_id = district.district_id JOIN disp ON disp.account_id = account.account_id JOIN client ON disp.client_id = client.client_id"
    query2 = "with __dbt__CTE__trans as ( select * from `id-bi-staging.playground_dev.public_dataset_financial_trans` where date >= '1998-12-01'), __dbt__CTE__account as ( select * from `id-bi-staging.playground_dev.public_dataset_financial_account` ), __dbt__CTE__district as ( select * from `id-bi-staging.playground_dev.public_dataset_financial_district` ), __dbt__CTE__disp as ( select * from `id-bi-staging.playground_dev.public_dataset_financial_disp` ), __dbt__CTE__client as ( select * from `id-bi-staging.playground_dev.public_dataset_financial_client` ),trans as (select * from __dbt__CTE__trans), account as (select * from __dbt__CTE__account), district as (select * from __dbt__CTE__district), disp as (select * from __dbt__CTE__disp), client as (select * from __dbt__CTE__client) select trans.date, trans.type as type_trans, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account,account.frequency, district.*, disp.type, client.* from trans join account on trans.account_id = account._account_id_ join district on account.district_id = district._district_id_ join disp on disp.account_id = account._account_id_ join client on disp.client_id = client._client_id_"
    s = parse2(query)
    print(s)
    # for a in json.loads(s):
    #     print(a)
