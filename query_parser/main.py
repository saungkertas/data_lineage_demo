import re


def main():
    query = """WITH trans AS ( SELECT trans.account_id AS account_id, trans.date AS date, type, operation, amount, balance, k_symbol, bank, account, trans_id, district_id, frequency FROM `id-bi-staging.playground_dev.public_dataset_financial_trans` AS trans JOIN `id-bi-staging.playground_dev.public_dataset_financial_account` AS account ON account.account_id = trans.account_id WHERE trans.date >= '1998-12-01'), district AS ( SELECT A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, district_id FROM `id-bi-staging.playground_dev.public_dataset_financial_district`), disp AS ( SELECT client_id, account_id, type, disp_id FROM `id-bi-staging.playground_dev.public_dataset_financial_disp`), client AS ( SELECT gender, birth_date, district_id, client_id FROM `id-bi-staging.playground_dev.public_dataset_financial_client`) SELECT trans.date, trans.type AS trans_type, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account, account.frequency, account.account_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16, district.district_id, disp.type, disp.type AS disp_type, client.gender, client.birth_date FROM trans JOIN district ON district.district_id = account.district_id JOIN disp ON account.account_id = disp.account_id JOIN client ON client.client_id = disp.client_id"""
    # query = """WITH trans AS ( SELECT account_id, date, type, operation, amount, balance, k_symbol, bank, account, trans_id FROM `id-bi-staging.playground_dev.public_dataset_financial_trans` WHERE date >= '1998-12-01'), account AS ( SELECT district_id, frequency, date, account_id FROM `id-bi-staging.playground_dev.public_dataset_financial_account`), district AS ( SELECT A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, district_id FROM `id-bi-staging.playground_dev.public_dataset_financial_district`), disp AS ( SELECT client_id, account_id, type, disp_id FROM `id-bi-staging.playground_dev.public_dataset_financial_disp`), client AS ( SELECT gender, birth_date, district_id, client_id FROM `id-bi-staging.playground_dev.public_dataset_financial_client`) SELECT trans.date, trans.type AS trans_type, trans.operation, trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account, account.frequency, account.account_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16, district.district_id, disp.type, disp.type as disp_type, client.gender, client.birth_date FROM trans JOIN account ON account.account_id = trans.account_id JOIN district ON district.district_id = account.district_id JOIN disp ON account.account_id = disp.account_id JOIN client ON client.client_id = disp.client_id """
    key = assign_key(query)
    value = assign_value(query)

    i = 0
    kv = {}
    for k in key:
        kv[k] = value[i]
        i = i + 1

    get_join = {}
    for key in kv:
        get_join = parse_join(kv[key])

    for key in kv:

        print(key)
        print('-')
        columns = get_columns(str(kv[key]))
        for column in columns:
            pk = False
            fk = False
            rk = False
            fk_word = ''
            for a in get_join:
                if remove_space(key) == remove_space(a.split('.')[0]) and remove_space(column) == remove_space(
                        a.split('.')[1]):
                    pk = True
                elif remove_space(key) != remove_space(a.split('.')[0]) and remove_space(column) == remove_space(
                        a.split('.')[1]):
                    fk = True
                    fk_word = a
                elif remove_space(column) == a:
                    rk = True
                    rk_word = a

            if pk:
                print(column + ' PK')
            elif fk:
                print(column + ' FK >- ' + fk_word)
            elif rk:
                print(column + ' FK >- ' + rk_word)
            else:
                print(column)

        print()


def assign_value(query):
    value = []
    cte_values = re.findall("\(.*?\)", query)
    bad_chars = [')', '(']
    for cte_value in cte_values:
        clean_cte_value = ''.join(i for i in cte_value if not i in bad_chars)
        value.append(clean_cte_value)
    last_cte_value = query.split(")")
    value.append(last_cte_value[-1])
    return value


def assign_key(query):
    key = []
    first_cte_key = query.split("(")
    clean_key = first_cte_key[0].replace('WITH ', '').replace(' AS', '').replace(' ', '')
    key.append(clean_key)

    cte_keys = re.findall("\).*?\(", query)
    bad_chars = [')', '(', ',']
    for cte_key in cte_keys:
        clean_key = ''.join(i for i in cte_key if not i in bad_chars)
        clean_key = clean_key.replace(' AS', '').replace(' ', '')
        key.append(clean_key)
    key.append("final_query")

    return key


def get_columns(cte_value):
    column_list = []
    split_columns = re.search(r'SELECT(.*?)FROM', cte_value).group(1)
    column_split_by_comma = split_columns.split(',')

    for column in column_split_by_comma:
        column_split_by_alias = column.split('AS')
        if len(column_split_by_alias) > 1:
            column_list.append(remove_space(column_split_by_alias[1]))
        else:
            column_list.append(remove_space(column_split_by_alias[0]))
    return column_list


def parse_join(cte_value):
    kv = {}
    if 'JOIN' in cte_value:
        after_from = cte_value.split('FROM')
        columns = after_from[1].split('JOIN')
        for column in columns:
            xx = column.split('ON ')
            if len(xx) > 1:
                yy = xx[1].split('=')
                kv[remove_space(yy[0])] = remove_space(yy[1])
        return kv

    else:
        return None  # dumps = json.dumps(parse(query))  # print(dumps)


def remove_space(word):
    return word.replace(' ', '')


if __name__ == '__main__':
    main()
