from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    copy_cvs_sql = \
        """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    copy_json_sql = \
        """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        aws_credentials_id='',
        table='',
        s3_bucket='',
        s3_key='',
        delimiter=',',
        ignore_headers=1,
        load_mode='',
        data_format='',
        *args,
        **kwargs
        ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Map params here

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.load_mode = load_mode
        self.data_format = data_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator has been implemented')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_mode == 'clean':
            self.log.info('Clearing data from destination Redshifttable'
                          )
            redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)

        if self.data_format == 'json':
            formatted_sql = \
                StageToRedshiftOperator.copy_json_sql.format(self.table,
                    s3_path, credentials.access_key,
                    credentials.secret_key, 'auto')
        else:
            formatted_sql = StageToRedshiftOperator.copy_cvs_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter,
                )
        redshift.run(formatted_sql)