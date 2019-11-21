import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name)
                                        >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name)
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


class LogContents(beam.DoFn):
    """This DoFn class logs the content of that which it receives """

    def process(self, input_element):
        logging.info("Contents: {}".format(input_element))
        logging.info("Contents type: {}".format(type(input_element)))
        return


def main():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)
    movies_pipeline_name = 'movies_data'
    movies_data = get_pipeline('playground_dev', p, 'Get Movies Table', 'id-bi-staging',
                               'public_dataset_imdb_movies')

    movies_directors_pipeline_name = 'movies_directors'
    movies_directors_data = get_pipeline('playground_dev', p, 'Get Movies Directors Table', 'id-bi-staging',
                                         'public_dataset_imdb_movies_directors')

    movies_genres_pipeline_name = 'movies_genres'
    movies_genres_data = get_pipeline('playground_dev', p, 'Get Movies Genres Table', 'id-bi-staging',
                                      'public_dataset_imdb_movies_genres')

    roles_pipeline_name = 'roles_data'
    roles_data = get_pipeline('playground_dev', p, 'Get Roles Table', 'id-bi-staging',
                              'public_dataset_imdb_roles')

    actors_pipeline_name = 'dispatch_data'
    actors_data = get_pipeline('playground_dev', p, 'Get Actors Table', 'id-bi-staging',
                               'public_dataset_imdb_actors')

    directors_pipeline_name = 'directors_data'
    directors_data = get_pipeline('playground_dev', p, 'Get Directors Table', 'id-bi-staging',
                                  'public_dataset_imdb_directors')

    directors_genres_pipeline_name = 'directors_genre_data'
    directors_genres_data = get_pipeline('playground_dev', p, 'Get Directors Genre Table', 'id-bi-staging',
                                         'public_dataset_imdb_directors_genres')

    join_movies_movies_directors_key = 'movie_id'
    join_movies_movies_directors_pipeline_title = 'Join Movie and Movie Directors'
    join_movies_movies_directors_data = do_join(movies_data, movies_pipeline_name,
                                                join_movies_movies_directors_pipeline_title,
                                                movies_directors_data, join_movies_movies_directors_key,
                                                movies_directors_pipeline_name)

    join_movies_genres_title = 'Log Join Movies Genres'
    join_movies_genres_pipeline_title = 'Join with Genres'
    join_movies_genres_key = 'movie_id'

    join_movies_genres_data = do_join(join_movies_movies_directors_data,
                                      join_movies_genres_title, join_movies_genres_pipeline_title, movies_genres_data,
                                      join_movies_genres_key,
                                      movies_genres_pipeline_name)

    join_roles_pipeline_title = 'Join with Roles'
    join_roles_key = 'movie_id'

    join_roles_data = do_join(join_movies_genres_data, join_movies_genres_pipeline_title,
                              join_roles_pipeline_title, roles_data, join_roles_key,
                              roles_pipeline_name)

    join_actors_pipeline_title = 'Join with Actors'
    join_actors_key = 'actor_id'

    join_actors_data = do_join(join_roles_data, join_roles_pipeline_title,
                               join_actors_pipeline_title,
                               actors_data, join_actors_key, actors_pipeline_name)

    join_directors_pipeline_title = 'Join with Directors'
    join_directors_key = 'director_id'

    join_directors_data = do_join(join_actors_data, join_actors_pipeline_title,
                                  join_directors_pipeline_title,
                                  directors_data, join_directors_key, directors_pipeline_name)

    join_directors_genres_pipeline_title = 'Join with Director Genres'
    join_directors_genres_key = 'director_id'

    do_join(join_directors_data, join_directors_pipeline_title,
            join_directors_genres_pipeline_title,
            directors_genres_data, join_directors_genres_key, directors_genres_pipeline_name)

    result = p.run()
    result.wait_until_finish()


def do_join(destination_data, destination_pipeline_name, join_pipeline_title,
            source_data, source_key, source_pipeline_name):
    pipeline_dictionary = {source_pipeline_name: source_data, destination_pipeline_name: destination_data}
    join_account_transaction = (
        pipeline_dictionary | join_pipeline_title.format(source_pipeline_name, destination_pipeline_name,
                                                         source_key) >> LeftJoin(
            source_pipeline_name, source_data, destination_pipeline_name,
            destination_data, source_key) | join_pipeline_title >> beam.ParDo(LogContents()))
    return join_account_transaction


def get_pipeline(dataset_name, p, pipeline_title, project_id, table_name):
    transaction_data = p | pipeline_title >> beam.io.Read(
        beam.io.BigQuerySource(table=table_name,
                               dataset=dataset_name,
                               project=project_id, validate=False,
                               coder=None, use_standard_sql=True,
                               flatten_results=True, kms_key=None))
    return transaction_data


if __name__ == '__main__':
    main()
