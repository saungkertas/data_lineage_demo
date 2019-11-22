import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from join import LeftJoin
from log_contents import LogContents
from unnest_co_grouped import UnnestCoGrouped


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
