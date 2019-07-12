from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
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
                 load_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table=table
        self.load_mode=load_mode
        

    def execute(self, context):
        self.log.info('LoadDimensionOperator has benn implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.load_mode=="clean" :
            self.log.info("Clearing data from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("load {} table".format(self.table))
        redshift.run(LoadDimensionOperator.insert_sql.format(self.table,self.sql))
