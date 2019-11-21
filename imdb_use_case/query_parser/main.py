import re


def main():
    query = """WITH movies AS( SELECT movie_id,movie_name,movie_year,movie_rank FROM `id-bi-staging.playground_dev.public_dataset_imdb_movies`), actors AS ( SELECT actor_id,actor_first_name, actor_last_name, actor_gender FROM `id-bi-staging.playground_dev.public_dataset_imdb_actors`), directors AS( SELECT director_id, director_first_name, director_last_name FROM `id-bi-staging.playground_dev.public_dataset_imdb_directors`), directors_genres AS( SELECT director_id, genre, prob FROM `id-bi-staging.playground_dev.public_dataset_imdb_directors_genres`), movies_directors AS( SELECT director_id, movie_id FROM `id-bi-staging.playground_dev.public_dataset_imdb_movies_directors`), movies_genres AS( SELECT movie_id, genre FROM `id-bi-staging.playground_dev.public_dataset_imdb_movies_genres`), roles AS( SELECT actor_id, movie_id, role FROM `id-bi-staging.playground_dev.public_dataset_imdb_roles`) SELECT movies.movie_id, movies.movie_name, movies.movie_year, movies.movie_rank, movies_genres.genre, roles.role, directors.director_id, actors.actor_id, actors.actor_first_name, actors.actor_last_name, actors.actor_gender FROM movies JOIN movies_directors ON movies.movie_id = movies_directors.movie_id JOIN movies_genres ON movies.movie_id = movies_genres.movie_id JOIN roles ON movies.movie_id = roles.movie_id JOIN directors ON directors.director_id = movies_directors.director_id JOIN actors ON roles.actor_id = actors.actor_id"""
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
