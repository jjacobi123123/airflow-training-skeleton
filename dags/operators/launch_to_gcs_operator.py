from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.launch_hook import LaunchHook


class LaunchToGcsOperator(BaseOperator):

    template_fields = ('_myvar1',)

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, bucket, file_name, start_date, end_date, google_cloud_storage_conn_id='google_cloud_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.bucket = bucket
        self.start_date = start_date
        self.end_date = end_date
        self.file_name = file_name

    def execute(self, context):
        data = self._retrieve_launches()
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handle.write(data)
        tmp_file_handle.close()
        self._upload_to_gcs([tmp_file_handle.name])


    def _retrieve_launches(self):
        hook = LaunchHook()
        return hook.fetch(self.start_date, self.end_date)


    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
        for tmp_file in files_to_upload:
            hook.upload(self.bucket, tmp_file,
                        self.file_name,
                        mime_type='application/json',
                        gzip=False)