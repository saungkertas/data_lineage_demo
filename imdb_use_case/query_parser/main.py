import re


def main():
    query = read_file('sql/query.sql')
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


def read_file(filename):
    f = open(filename, "r")
    content = f.read()
    return content


if __name__ == '__main__':
    main()
