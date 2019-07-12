from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        insert into {} 
            {} 
            
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table=table

    def execute(self, context):
        self.log.info('LoadFactOperator has been implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("load fact table")
        redshift.run(LoadFactOperator.insert_sql.format(self.table,self.sql))
        
        
