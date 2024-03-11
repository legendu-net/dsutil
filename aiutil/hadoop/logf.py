"""Script for fetch and filtering Spark application logs.
"""

from pathlib import Path
import re
from argparse import ArgumentParser, Namespace
import subprocess as sp
from loguru import logger
from .log import LogFilter

YARN = "/apache/hadoop/bin/yarn"


def filter_(args):
    """Filter the a log file.

    :param args: A Namespace object containing command-line options.
    """
    logf = LogFilter(
        log_file=args.log_file,
        context_size=args.context_size,
        keywords=args.keywords,
        patterns=args.patterns,
        output=args.output,
        threshold=args.threshold,
        dump_by_keyword=args.dump_by_keyword,
    )
    logf.filter()


def _format_app_id(app_id: str):
    app_id = app_id.lower()
    # support old Hive job id of the format job_123456789_123456
    app_id = re.sub("^job_", "application_", app_id)
    # support job id of the format _123456789_123456
    app_id = re.sub("^_", "application_", app_id)
    # support job id of the format 123456789_123456
    if not app_id.startswith("application_"):
        app_id = "application_" + app_id
    return app_id


def fetch(args):
    """Fetch and filter the log of a Spark/Hadoop application.

    :param args: A Namespace object containing command-line options.
    """
    app_id = _format_app_id(args.app_id)
    output = args.output if args.output else app_id
    cmd = [YARN, "logs", "-size_limit_mb", "-1", "-applicationId", app_id]
    if args.user:
        cmd = cmd + ["-appOwner", args.user]
    logger.info(f"Fetching log of the application {app_id} ...")
    with open(output, "w", encoding="utf-8") as fout:
        sp.run(cmd, stdout=fout, check=True)
    args.log_file = output
    filter_(args)


def status(args):
    """Get status of a Spark application.

    :param args: A Namespace object containing command-line options.
    """
    if args.app_id:
        cmd = ["yarn", "application", "-status", args.app_id]
        sp.run(cmd, check=True)
        return
    report = """Application Report : 
        Application-Id : {APP_ID}
        Application-Name : {APP_NAME}
        Application-Type : {APP_TYPE}
        User : {USER}
        Queue : {QUEUE}
        Application Priority : {PRIORITY}
        Start-Time : {START_TIME}
        Finish-Time : {FINISH_TIME}
        Progress : {PROGRESS}
        State : {STATE}
        Final-State : {STATUS}
        Tracking-URL : {URL}
        RPC Port : {PORT}
        AM Host : {HOST}
        Aggregate Resource Allocation : {RESOURCE}
        Log Aggregation Status : {LOG_STATUS}
        Diagnostics : 
        Unmanaged Application : {UNMANAGED}
        Application Node Label Expression : {APP_NODE_LABEL}
        AM container Node Label Expression : {CON_NODE_LABEL}
    """
    with args.log_file.open() as fin:
        for line in fin:
            if "{APP_ID}" in report:
                match = re.search(r"(application_\d+_\d+)", line)
                if match:
                    report = report.replace("{APP_ID}", match.group())
            if "{APP_NAME}" in report:
                match = re.search(r"--primary-py-file (.*) ", line)
                if match:
                    report = report.replace("{APP_NAME}", match.group())
            if "{USER}" in report:
                match = re.search(r"local/usercache/(.*)/", line)
                if match:
                    report = report.replace("{USER}", match.group())
            if "{STATUS}" in report:
                match = re.search(r"Final app status: (.*),", line)
                if match:
                    report = report.replace("{STATUS}", match.group())
    print(report)


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.

    :param args: The arguments to parse.
        If None, the arguments from command-line are parsed.
    :param namespace: An inital Namespace object.
    :return: A namespace object containing parsed options.
    """
    parser = ArgumentParser(description="Spark/Hadoop log utils.")
    subparsers = parser.add_subparsers(help="Sub commands.")
    _subparser_fetch(subparsers)
    _subparser_filter(subparsers)
    _subparser_status(subparsers)
    return parser.parse_args(args=args, namespace=namespace)


def _subparser_status(subparsers):
    subparser_status = subparsers.add_parser(
        "status", help="filter key information from a Spark/Hive application log."
    )
    mutex_group = subparser_status.add_mutually_exclusive_group(required=True)
    mutex_group.add_argument(
        "-i", "--id", "--app-id", dest="app_id", type=str, help="An application ID."
    )
    mutex_group.add_argument(
        "-l", "-f", "--log-file", dest="log_file", type=Path, help="An application ID."
    )
    subparser_status.set_defaults(func=status)


def _option_filter(subparser) -> None:
    subparser.add_argument(
        "-k",
        "--keywords",
        nargs="+",
        dest="keywords",
        default=LogFilter.KEYWORDS,
        help="user-defined keywords to search for in the log file",
    )
    subparser.add_argument(
        "-i",
        "--ignore-patterns",
        nargs="+",
        dest="patterns",
        default=LogFilter.PATTERNS,
        help="regular expression patterns (date/time and ip by default) to ignore in dedup of filtered lines.",
    )
    subparser.add_argument(
        "-c",
        "--context-size",
        type=int,
        dest="context_size",
        default=3,
        help="number of lines (3 by default) to print before and after the suspicious line.",
    )
    subparser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="",
        help="path of the output file (containing filtered lines).",
    )
    subparser.add_argument(
        "-t",
        "--threshold",
        dest="threshold",
        type=float,
        default=0.7,
        help="make pattern matching case-sensitive.",
    )
    subparser.add_argument(
        "-d",
        "--dump-by-keyword",
        dest="dump_by_keyword",
        action="store_true",
        help="dump error lines by keywords.",
    )


def _subparser_fetch(subparsers):
    subparser_fetch = subparsers.add_parser(
        "fetch", help="fetch the log of a Spark/Hive application."
    )
    subparser_fetch.add_argument(
        "app_id", help="the ID of the Spark/Hive application whose log is to fetch."
    )
    subparser_fetch.add_argument(
        "-u",
        "--user",
        dest="user",
        default=None,
        help="the name of the Spark/Hive application owner.",
    )
    subparser_fetch.add_argument(
        "-m",
        "--b-marketing-ep-infr",
        dest="user",
        action="store_const",
        const="b_marketing_ep_infr",
        help="Fetch log using the acount b_marketing_ep_infr.",
    )
    _option_filter(subparser_fetch)
    subparser_fetch.set_defaults(func=fetch)


def _subparser_filter(subparsers):
    subparser_filter = subparsers.add_parser(
        "filter", help="filter key information from a Spark/Hive application log."
    )
    subparser_filter.add_argument(
        "log_file", type=str, help="path of the log file to process"
    )
    _option_filter(subparser_filter)
    subparser_filter.set_defaults(func=filter_)


def main(args: Namespace | None = None):
    """The main function for script usage."""
    if args is None:
        args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
