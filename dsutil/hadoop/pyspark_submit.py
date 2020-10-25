#!/usr/bin/env python3
# encoding: utf-8
"""A module makes it easy to run Scala/Python Spark job.
"""
import os
import sys
from typing import Union, List, Dict, Callable
from argparse import Namespace, ArgumentParser
from pathlib import Path
import subprocess as sp
import re
import datetime
import yaml
from loguru import logger
import notifiers


class SparkSubmit:
    """A class for submitting Spark jobs.
    """
    def __init__(self, email: Union[Dict, None] = None, level: str = "INFO"):
        """Initialize a SparkSubmit instance.
        :param level: The logging level for loguru.
        """
        # set up loguru with the right logging level
        try:
            logger.remove(0)
        except:
            pass
        logger.add(sys.stdout, level=level)
        self._spark_submit_log = {}
        self.email = email

    def _spark_log_filter_helper_keyword(
        self, line: str, keyword: str, mutual_exclusive: List[str],
        time_delta: datetime.timedelta
    ) -> bool:
        if keyword not in line:
            return False
        now = datetime.datetime.now()
        for kwd in mutual_exclusive:
            if kwd != keyword:
                self._spark_submit_log[kwd] = now - time_delta * 2
        if keyword not in self._spark_submit_log:
            self._spark_submit_log[keyword] = now
            return True
        if now - self._spark_submit_log[keyword] >= time_delta:
            self._spark_submit_log[keyword] = now
            return True
        return False

    def _spark_log_filter_helper_keywords(
        self, line: str, keywords: List[str], mutual_exclusive: bool,
        time_delta: datetime.timedelta
    ) -> bool:
        mutual_exclusive = keywords if mutual_exclusive else ()
        for keyword in keywords:
            if self._spark_log_filter_helper_keyword(
                line=line,
                keyword=keyword,
                mutual_exclusive=mutual_exclusive,
                time_delta=time_delta
            ):
                return True
        return False

    def _spark_log_filter(self, line: str) -> bool:
        line = line.strip().lower()
        if self._spark_log_filter_helper_keywords(
            line=line,
            keywords=["warn client", "uploading"],
            mutual_exclusive=False,
            time_delta=datetime.timedelta(seconds=0)
        ):
            return True
        if self._spark_log_filter_helper_keywords(
            line=line,
            keywords=["queue: ", "tracking url: "],
            mutual_exclusive=False,
            time_delta=datetime.timedelta(days=1)
        ):
            return True
        if self._spark_log_filter_helper_keywords(
            line=line,
            keywords=["exception", "user class threw", "caused by"],
            mutual_exclusive=False,
            time_delta=datetime.timedelta(seconds=1)
        ):
            return True
        if self._spark_log_filter_helper_keywords(
            line=line,
            keywords=["state: accepted", "state: running", "state: finished"],
            mutual_exclusive=True,
            time_delta=datetime.timedelta(minutes=10)
        ):
            return True
        if self._spark_log_filter_helper_keywords(
            line=line,
            keywords=[
                "final status: undefined", "final status: succeeded",
                "final status: failed"
            ],
            mutual_exclusive=True,
            time_delta=datetime.timedelta(minutes=3)
        ):
            return True
        return False

    @staticmethod
    def _print_filter(line: str, log_filter: Union[Callable, None] = None) -> bool:
        if not line:
            return False
        if log_filter is None:
            print(line)
            return True
        if log_filter(line):
            print(line)
            return True
        return False

    def submit(self, cmd: str):
        """Submit a Spark job.
        """
        logger.info("Submitting Spark job...\n{}", cmd)
        stdout = []
        self._spark_submit_log.clear()
        process = sp.Popen(cmd, shell=True, stderr=sp.PIPE)
        while True:
            if process.poll() is None:
                line = process.stderr.readline().decode().rstrip()  # pytype: disable=attribute-error
                if self._print_filter(line, self._spark_log_filter):
                    stdout.append(line)
            else:
                for line in process.stderr.readlines():  # pytype: disable=attribute-error
                    line = line.decode().rstrip()
                    if self._print_filter(line, self._spark_log_filter):
                        stdout.append(line)
                break
        # status
        status = self._final_status(stdout)
        app_id = self._app_id(stdout)
        if status:
            subject = f"Spark Application {app_id} {status}"
        else:
            subject = "Spark Application Submission Failed"
        if self.email:
            notifiers.get_notifier("email").notify(
                from_=self.email["from"],
                to=self.email["to"],
                subject=subject,
                message=cmd + "\n".join(stdout),
                host=self.email["host"],
                username="",
                password="",
                attachments=self._attachments(cmd),
            )
        if status == "FAILED":
            self._notify_log(app_id, subject)

    def _notify_log(self, app_id, subject):
        sp.run(f"logf fetch {app_id}", shell=True, check=True)
        notifiers.get_notifier("email").notify(
            from_=self.email["from"],
            to=self.email["to"],
            subject="Re: " + subject,
            message=Path(app_id + "_s").read_text(),
            host=self.email["host"],
            username="",
            password="",
        )

    @staticmethod
    def _attachments(cmd: str):
        """Identify attachments to send with the email.
        """
        return cmd.strip().split("\n")[-1].split(" ")[:1]

    @staticmethod
    def _app_id(stdout: List[str]):
        """Parse the application ID.
        """
        for line in reversed(stdout):
            match = re.search(r"(application_\d+_\d+)", line)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def _final_status(stdout: List[str]):
        """Parse the final status of the Spark application.
        """
        for line in reversed(stdout):
            if "final status: " in line:
                return line.split(": ")[-1]
        return ""


def _files(config: Dict) -> str:
    """Get a list of valid configuration files to use with the option --files.
    """
    files = []
    for key, paths in config["files"].items():
        for path in paths:
            if path.startswith("file://") and os.path.isfile(path[7:]):
                files.append(path)
                break
            if path.startswith("viewfs://") or path.startswith("hdfs://"):
                process = sp.run(
                    f"/apache/hadoop/bin/hdfs dfs -test -f {path}",
                    shell=True,
                    check=False
                )
                if process.returncode == 0:
                    files.append(path)
                    break
        else:
            logger.warning(
                "None of the specified configuration file for {} exists.\n    ", key,
                "\n".join("    " + path for path in paths)
            )
    return ",".join(files)


def submit(args: Namespace):
    """Submit the Spark job.
    """
    with open(args.config, "r") as fin:
        config = yaml.load(fin, Loader=yaml.FullLoader)
    config["files"] = _files(config)
    if "jars" not in config:
        config["jars"] = ()
    opts = (
        "files", "master", "deploy-mode", "queue", "num-executors", "executor-memory",
        "driver-memory", "executor-cores", "archives"
    )
    lines = [config["spark-submit"]] + [
        f"--{opt} {config[opt]}" for opt in opts if opt in config
    ] + [f"--conf {k}={v}" for k, v in config["conf"].items()
        ] + [f"--jars {jar}" for jar in config["jars"]] + args.cmd
    for idx in range(1, len(lines)):
        lines[idx] = " " * 4 + lines[idx]
    SparkSubmit(email=config["email"]).submit(" \\\n".join(lines) + "\n")


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.
    """
    parser = ArgumentParser(description="Submit Spark application.")
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        required=True,
        help="The configuration file to use."
    )
    parser.add_argument(
        dest="cmd", nargs="+", help="The command to submit to Spark to run."
    )
    args = parser.parse_args(args=args, namespace=namespace)
    return args


def main():
    """Define a main function.
    """
    args = parse_args()
    submit(args)


if __name__ == "__main__":
    main()
