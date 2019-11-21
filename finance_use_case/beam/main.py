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
    transaction_pipeline_name = 'transaction_data'
    transaction_data = get_pipeline('playground_dev', p, 'Get Transaction Table', 'id-bi-staging',
                                    'public_dataset_financial_trans')

    account_pipeline_name = 'account_data'
    account_data = get_pipeline('playground_dev', p, 'Get Account Table', 'id-bi-staging',
                                'public_dataset_financial_account')

    district_pipeline_name = 'district_data'
    district_data = get_pipeline('playground_dev', p, 'Get District Table', 'id-bi-staging',
                                 'public_dataset_financial_district')

    dispatch_pipeline_name = 'dispatch_data'
    dispatch_data = get_pipeline('playground_dev', p, 'Get Dispatch Table', 'id-bi-staging',
                                 'public_dataset_financial_disp')

    client_pipeline_name = 'dispatch_data'
    client_data = get_pipeline('playground_dev', p, 'Get Client Table', 'id-bi-staging',
                               'public_dataset_financial_client')

    join_transaction_account_key = 'account_id'

    # join_account_transaction_pipeline_name = 'Joined Data'  #
    join_account_transaction_pipeline_title = 'Join transaction and account'
    join_account_transaction_log_title = 'Log Join Account-Transaction'
    join_account_transaction_data = do_join(account_data, account_pipeline_name, join_account_transaction_log_title,
                                            join_account_transaction_pipeline_title,
                                            transaction_data, join_transaction_account_key, transaction_pipeline_name)

    join_district_log_title = 'Log Join District'
    join_district_pipeline_title = 'Join with District'
    join_district_key = 'district_id'

    join_district = do_join(join_account_transaction_data, join_account_transaction_log_title,
                            join_district_log_title, join_district_pipeline_title, district_data, join_district_key,
                            district_pipeline_name)

    join_dispatch_log_title = 'Log Join Dispatch'
    join_dispatch_pipeline_title = 'Join with Dispatch'
    join_dispatch_key = 'account_id'

    join_dispatch = do_join(join_district, join_district_pipeline_title,
                            join_dispatch_log_title, join_dispatch_pipeline_title, dispatch_data, join_dispatch_key,
                            dispatch_pipeline_name)

    join_client_log_title = 'Log Join Client'
    join_client_pipeline_title = 'Join with Client'
    join_client_key = 'client_id'

    do_join(join_dispatch, join_dispatch_pipeline_title,
            join_client_log_title, join_client_pipeline_title, client_data, join_client_key,
            client_pipeline_name)

    result = p.run()
    result.wait_until_finish()


def do_join(destination_data, destination_pipeline_name, join_log_title, join_pipeline_title,
            source_data, source_key, source_pipeline_name):
    pipeline_dictionary = {source_pipeline_name: source_data, destination_pipeline_name: destination_data}
    join_account_transaction = (
        pipeline_dictionary | join_pipeline_title.format(source_pipeline_name, destination_pipeline_name,
                                                         source_key) >> LeftJoin(
            source_pipeline_name, source_data, destination_pipeline_name,
            destination_data, source_key) | join_log_title >> beam.ParDo(LogContents()))
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
