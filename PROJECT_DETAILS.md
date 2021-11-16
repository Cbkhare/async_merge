# Coding Excercise

You are given an AWS S3 bucket with hundreds of files (log files, effectively).
Each file contains hundreds of lines, sorted alphabetically. Your task is to
implement an efficient and scalable soution to merge a subset of these files
(in a given date-range) into one sorted file and store it in S3.
In order to do this you should implement a REST service that provides endpoints
to initiate the merging of files and to download the file once the merging is
done. The actual merging of the files should happen in an asyncronous executor,
separate from the REST service.

All filenames are in the ISO8601 date format, postfixed with a serial number,
for example `2019-08-17_1`, `2019-08-17_2`, `2019-08-18_1`, etc.

We should be able to run the whole project with `docker-compose up` to test
your solution (see below the explanation about the skeleton we provide).

## Expected REST API specification

We would like you to implement the following endpoints.

### `/initiate` - `POST`

Input model:

    {
        "start_date": "<Date, ISO8601 format>",
        "end_date": "<Date, ISO8601 format>"
    }

Example input:

    {
        "start_date": "2019-08-19",
        "end_date": "2019-08-25"
    }

Calling this endpoint should initiate the merging of files contained in the S3
bucket, by off-loading the task to the asyncronous executor.
The endpoint should return a download ID, by which we can poll for or download
the single merged and sorted file, with the contents of all the files with
names within the given date range.

Output model:

    {
        "download_id": "<ID>"
    }

Example output:

    {
        "download_id": "b0952099-3536-4ea0-a613-98509f4087cd"
    }

Make sure the input of the `/initiate` endpoint is properly validated and no
malformed data is accepted. In case of errors or wrong input, return the
appropriate HTTP code.

### `/download/<download ID>` - `GET`

This endpoint should accept a single parameter, the download ID (generated and
returned by the `/initiate` endpoint).

There should be 2 kinds of responses, since merging multiple large files could
take a relatively long time:
 * The merged and sorted file - in case the merging finished and the merged
 file is uploaded to S3
 * Appropriate HTTP response indicating that the merging and uploading is not
 finished yet - in case the merging is still ongoing in the asyncronous
 executor

## Skeleton project

Since this is quite a complex task, we provide you a "skeleton" project, which
already contains a dummy REST service (including the required endpoints), a
dummy asyncronous executor along with the Redis database necessary to run it
and an AWS S3 compatible service (MinIO). Redis and MinIO are readily available
Docker images and the dummy REST and asyncronous executor are Dockerized by us,
all of them assembled using a Docker-compose configuration for easy execution
on your own computer.

### Contents of the skeleton project

 * `webapp` - A Python REST API application, built with the
 [Pyramid](https://docs.pylonsproject.org/projects/pyramid/en/1.10-branch/)
 web framework
 * `asyncworker` - A Python asyncronous executor application, built with the
 [Celery](http://docs.celeryproject.org/en/latest/) distributed tasks
 framework
 * `Dockerfile.webapp` - [Docker](https://www.docker.com/) container definition
 of the `webapp` REST service
 * `Dockerfile.asyncworker` - [Docker](https://www.docker.com/) container
 definition of the `asyncworker` asyncronous executor service
 * `docker-compose.yml` - [Docker-compose](https://docs.docker.com/compose/)
 configuration to define and run every component of the system in Docker
 containers on your computer
 * `log-generator` - a directory containing a file generator Python script
 and its requirements to create test files for your solution

### MinIO - AWS S3 compatible service

Part of the `docker-compose.yml` definition is to start an AWS S3 compatible
service, [MinIO](https://hub.docker.com/r/minio/minio/), so you can run
everything on your local machine. You will need to interact with this from
the asyncronous executor to download and upload files.
To do this you'll need the access key `minio` and the secret key `minio123`.
The default endpoint is `http://localhost:9000` from the Docker host machine
(your computer). MinIO provides a web interface, which can be useful when
debugging, also available on the same endpoint.
However, within the Docker-compose network (thus from the `webapp` and
`asyncworker` services) the endpoint is `http://minio:9000`.

All files put in a `miniodata` directory in the project root (same level as
`docker-compose.yml`), will be available in the MinIO S3 compatible service, as
if it was previously uploaded to S3.

Since MinIO is API compatible with S3, the same libraries can be used to
interact with it as with AWS S3.

### Tools needed

 * Python 3.7
 * Docker
 * Docker-compose

### Running the project

You can (re)build all Docker containers by running the `docker-compose build`
command.

You can run all the components at once by running the `docker-compose up`
command.

After running `docker-compose up`, the REST service is available at
`localhost:6543`.

## Creating files

In order for you to be able to easily create files to test we have provided
a script to generate an arbitrary number of test files. Script can be found
under log-generator directory. To generate small test data set please execute
following: `cd log-generator && pip install -r requirements.txt && python log-generator.py --path ../miniodata/logs --days 5 --lines 500 --files 3`

This command will generate files for 5 days, each day having 3 corresponding
files (15 in total) and each file with 500 lines. The files will end up in
the `logs` S3 bucket of the MinIO service. You should only handle files in your
solution that are located in this bucket.

## Recommended Python libraries

The following are a few Python libraries that can help with the implementation,
if you know of better tools or are more familiar with different libraries, do
not hesitate to use those instead.

 * [Colander](https://docs.pylonsproject.org/projects/colander/en/latest/)
 for REST input validation
 * [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
 for interacting with the AWS S3 compatible MinIO service
 * [pytest](https://docs.pytest.org/en/latest/) for automated testing

## Your implementation

Since the project scaffolding is ready-made you can solve the task by modifying
only the following files:
 * `webapp/views/default.py` - `initiate` and `download` functions for their
 respective endpoints
 * `asyncworker/tasks.py` - `merge_s3_files` Celery-task-function to interact
 with S3 and merge files

However if you think the overall project structure or the structure of the
Python applications (`webapp` and `asyncworker`) can be improved, please do so.

Also, if for any reason you’re not comfortable with Pyramid or Celery,
feel free to solve this assignment with another web framework and/or task
scheduler.

**We’ve deliberately left the specific implementation details out - we want to
give you space to be creative with your approach to the problem, and impress us
with your skills and experience.**
