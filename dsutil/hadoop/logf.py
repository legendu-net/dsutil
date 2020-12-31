"""Script for fetch and filtering Spark application logs.
"""
import re
import argparse
import subprocess as sp
from .log import LogFilter
YARN = '/apache/hadoop/bin/yarn'


def filter_(args):
    """Filter the a log file.
    """
    logf = LogFilter(
        log_file=args.log_file,
        context_size=args.context_size,
        keywords=args.keywords,
        patterns=args.patterns,
        case_sensitive=args.case_sensitive
    )
    logf.filter()


def _format_app_id(app_id: str):
    app_id = app_id.lower()
    # support old Hive job id of the format job_123456789_123456
    app_id = re.sub('^job_', 'application_', app_id)
    # support job id of the format _123456789_123456
    app_id = re.sub('^_', 'application_', app_id)
    # support job id of the format 123456789_123456
    if not app_id.startswith('application_'):
        app_id = 'application_' + app_id
    return app_id


def fetch(args):
    """Fetch and filter the log of a Spark/Hadoop application.
    """
    app_id = _format_app_id(args.app_id)
    output = args.output if args.output else app_id
    cmd = [YARN, 'logs', '-applicationId', app_id]
    if args.user:
        cmd = cmd + ['-appOwner', args.user]
    with open(output, 'w', encoding='utf-8') as fout:
        sp.run(cmd, stdout=fout, check=True)
    LogFilter(log_file=output).filter()


def parse_args(args=None, namespace=None):
    """Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Spark/Hadoop log utils.')
    subparsers = parser.add_subparsers(help='Sub commands.')
    _subparser_fetch(subparsers)
    _subparser_filter(subparsers)
    return parser.parse_args(args=args, namespace=namespace)


def _subparser_fetch(subparsers):
    parser_fetch = subparsers.add_parser(
        'fetch', help='fetch the log of a Spark/Hive application.'
    )
    parser_fetch.add_argument(
        'app_id', help='the ID of the Spark/Hive application whose log is to fetch.'
    )
    parser_fetch.add_argument(
        '-o',
        '--output',
        dest='output',
        help='the ID of the Spark/Hive application whose log is to fetch.'
    )
    parser_fetch.add_argument(
        '-u',
        '--user',
        dest='user',
        default=None,
        help='the name of the Spark/Hive application owner.'
    )
    parser_fetch.add_argument(
        '-m',
        '--b-marketing-ep-infr',
        dest='user',
        action='store_const',
        const='b_marketing_ep_infr',
        help='Fetch log using the acount b_marketing_ep_infr.'
    )
    parser_fetch.set_defaults(func=fetch)


def _subparser_filter(subparsers):
    parser_filter = subparsers.add_parser(
        'filter', help='filter key information from a Spark/Hive application log.'
    )
    parser_filter.add_argument(
        'log_file', type=str, help='path of the log file to process'
    )
    parser_filter.add_argument(
        '-k',
        '--keywords',
        nargs='+',
        dest='keywords',
        default=LogFilter.KEYWORDS,
        help='user-defined keywords to search for in the log file'
    )
    parser_filter.add_argument(
        '-i',
        '--ignore-patterns',
        nargs='+',
        dest='patterns',
        default=LogFilter.PATTERNS,
        help=
        'regular expression patterns (date/time and ip by default) to ignore in dedup of filtered lines.'
    )
    parser_filter.add_argument(
        '-c',
        '--context-size',
        type=int,
        dest='context_size',
        default=3,
        help=
        'number of lines (3 by default) to print before and after the suspicious line.'
    )
    parser_filter.add_argument(
        '-o',
        '--output-file',
        dest='output_file',
        help='path of the output file (containing filtered lines).'
    )
    parser_filter.add_argument(
        '-C',
        '--case-sensitive',
        dest='case_sensitive',
        action="store_true",
        help='make pattern matching case-sensitive.'
    )
    parser_filter.set_defaults(func=filter_)


def main():
    """The main function for script usage.
    """
    args = parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
