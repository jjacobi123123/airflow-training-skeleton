import json
from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.launch_hook import LaunchHook


class LaunchToGcsOperator(BaseOperator):

    template_fields = ('start_date_str', 'end_date_str', 'file_name')

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, bucket, file_name, start_date_str, end_date_str, google_cloud_storage_conn_id='google_cloud_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.bucket = bucket
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str
        self.file_name = file_name

    def execute(self, context):
        data = json.dumps(self._retrieve_launches())
        tmp_file_handle = NamedTemporaryFile('w+', delete=True)
        tmp_file_handle.write(data)
        tmp_file_handle.flush()
        self._upload_to_gcs([tmp_file_handle.name])
        tmp_file_handle.close()


    def _retrieve_launches(self):
        hook = LaunchHook()
        return hook.fetch(self.start_date_str, self.end_date_str)


    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        for tmp_file in files_to_upload:
            hook.upload(bucket=self.bucket, filename=tmp_file,
                        object=self.file_name,
                        mime_type='application/json',
                        gzip=False)