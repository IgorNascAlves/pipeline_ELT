import sys
sys.path.append("airflow_pipeline")


import json
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join

from airflow.models import DAG, BaseOperator, TaskInstance
from hooks.twitter_hook import TwitterHook


class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    def __init__(self, query, file_path,conn_id = None,start_time = None,end_time = None, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.query = query
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(query=self.query, conn_id=self.conn_id, start_time=self.start_time, end_time=self.end_time)
        # for pg in hook.run():
        #     print(json.dumps(pg, indent=4, sort_keys=True))
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                # json.dumps(pg, indent=4, sort_keys=True)
                output_file.write("\n")

if __name__ == "__main__":

    hoje = datetime.now()

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = hoje.strftime(TIMESTAMP_FORMAT)
    start_time = (hoje + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)

    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(query="NBABrasil", task_id="test_run",
                             file_path=join(
                                            "datalake/twitter_nbabrasil",
                                            f"extract_date={hoje.date()}",
                                            f"AluraOnline_{hoje.strftime('%Y%m%d')}.json"
                                        ),
                            start_time=start_time,
                            end_time=end_time
                            )
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)
        # ti = TaskInstance(task=to)
        # ti.run(test_mode=True)