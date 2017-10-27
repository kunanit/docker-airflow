from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


class PostgresToS3Transfer(BaseOperator):
    """
    Dumps a database table to a CSV file on S3
    """
    @apply_defaults
    def __init__(
        self, 
        table, 
        s3_bucket, 
        s3_key, 
        schema=None,
        postgres_conn_id='postgres_default',
        s3_conn_id='s3_default', 
        output_encoding='utf-8',
        *args, **kwargs):

        super(PostgresToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.output_encoding=output_encoding

    def execute(self, context):
        source_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        destination_hook = S3Hook(s3_conn_id=self.s3_conn_id)

        df = source_hook.get_pandas_df("SELECT * FROM {}".format(self.table))

        # not suitable for large files
        destination_hook.load_string(
            df.to_csv(None, index=False),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )
