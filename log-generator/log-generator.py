import argparse
import random

from datetime import date, timedelta
from os import makedirs
from faker import Faker

fake = Faker()

DATE_FORMAT = "%Y-%m-%d"
LOG_LINE_TEMPLATE = "{}:{} {} {} {}\n"
IP_ADDRESSES = [fake.ipv4() for i in range(100)]
USER_AGENTS = [
    fake.firefox(),
    fake.chrome(),
    fake.safari(),
    fake.internet_explorer(),
    fake.opera(),
]
RESOURCES = ["/list", "/", "/admin", "/explore", "/search/", "/posts/"]


def create_logline(date):
    uri = random.choice(RESOURCES)
    uagent = random.choice(USER_AGENTS)
    time = fake.time()
    ip = random.choice(IP_ADDRESSES)

    return LOG_LINE_TEMPLATE.format(date, time, ip, uagent, uri)


def generate_logs_for_day(date, num_lines):
    log_lines = [create_logline(date) for x in range(num_lines)]
    log_lines.sort()
    return log_lines


def write_logs_to_file(file_name, log_lines):
    file_handler = open(file_name, "w")
    file_handler.writelines(log_lines)
    file_handler.close()


def create_bucket(path):
    makedirs(path, exist_ok=True)
    return path


def create_fake_date(days_before_today):
    day = date.today()
    if days_before_today:
        day -= timedelta(days=days_before_today)
    return day.strftime(DATE_FORMAT)


def get_args(argv=None):
    parser = argparse.ArgumentParser(
        __file__, description="Fake Log Generator"
    )
    parser.add_argument(
        "--path",
        "-p",
        dest="path",
        help="Directory to output log files to."
             "We suggest to generate them under './miniodata/logs'",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--lines",
        "-l",
        dest="num_lines",
        help="Number of lines to generate",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--days",
        "-d",
        dest="num_days",
        help="Number of days you want to generate logs for",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--files",
        "-f",
        dest="num_files",
        help="Number of files per day to generate",
        type=int,
        default=3,
    )

    return parser.parse_args()


def main():
    args = get_args()
    path = args.path
    num_files = args.num_files
    num_lines = args.num_lines
    num_days = args.num_days

    bucket = create_bucket(path)

    for i in range(num_days):
        day = create_fake_date(i)
        for i in range(num_files):
            log_lines = generate_logs_for_day(day, num_lines)
            out_file_name = "{}/{}_{}.log".format(bucket, day, i)
            write_logs_to_file(out_file_name, log_lines)


if __name__ == "__main__":
    main()
