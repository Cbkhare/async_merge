import logging

from asyncworker.tasks import merge_s3_files, download_file
from asyncworker.boto3_connector import Boto3ConnectorLogs
from celery.result import AsyncResult
from pyramid.view import view_config
from pyramid.response import Response
import json

logger = logging.getLogger(__name__)


@view_config(route_name="initiate", renderer="json", request_method="POST")
def initiate(request):
    """This function is called when a POST request is made to /initiate.

    According to the assignment the expected POST input is:
    {
        "start_date": "<date in ISO8601 format>",
        "end_date": "<date in ISO8601 format>"
    }
    For example:
    {
        "start_date": "2019-08-18",
        "end_date": "2019-08-25"
    }

    The function should initiate the merging of files on S3 with names between
    the given dates. The actual merging should be offloaded to the async
    executor service.

    The return data is a download ID that the /download endpoint digests:
    {
        "download_id": "<id>"
    }
    For example:
    {
        "download_id": "b0952099-3536-4ea0-a613-98509f4087cd"
    }
    """
    logger.info("Initiate called")
    request = json.loads(request)
    param = request.get('body', None)
    if not param:
        return False
    start, end = param.get('start'), param.get('end')
    task_result = merge_s3_files.delay(start, end)
    logger.info("Task result is %s", task_result.get())
    return {"route": "initiate", "taskId": task_result}


@view_config(route_name="download", renderer="json")
def download(request):
    """This function is called when a GET request is made to /download.

    According to the assignment this endpoint accepts the download ID as a URL
    parameter and returns the merged file for download if the merging is done.
    If the merging is not done yet, the appropriate HTTP code is returned, so
    the calling client can continue polling.
    """
    try:
        logger.info("Download called")
        request = json.loads(request)
        param = request.get('body', None)
        if not param:
            return False
        download_id = param.get("downloadID", None)
        if not download_id:
            return False
        res = AsyncResult(download_id)
        if res.ready():
            status = download_file.delay(download_id)
    except Exception as exp:
        raise exp
    return {"route": "download"}
