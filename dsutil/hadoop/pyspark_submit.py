#!/usr/bin/env python3
# encoding: utf-8
"""A module makes it easy to run Scala/Python Spark job.
"""
from typing import Union, List, Dict, Callable, Any
import os
import sys
from argparse import Namespace, ArgumentParser
from pathlib import Path
import shutil
import subprocess as sp
import re
import time
import datetime
import yaml
from loguru import logger
import notifiers


class SparkSubmit:
    """A class for submitting Spark jobs.
    """
    def __init__(self, email: Union[Dict, None] = None, level: str = "INFO"):
        """Initialize a SparkSubmit instance.

        :param email: A dict object containing email information ("from", "to" and "host").
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

    def submit(self, cmd: str, attachments: Union[None, List[str]] = None) -> bool:
        """Submit a Spark job.

        :param cmd: The Python script command to run.
        :param attachments: Attachments to send with the notification email.
        :return: True if the Spark application succeeds and False otherwise.
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
            param = {
                "from_": self.email["from"],
                "to": self.email["to"],
                "subject": subject,
                "message": cmd + "\n".join(stdout),
                "host": self.email["host"],
                "username": "",
                "password": "",
            }
            if attachments:
                if isinstance(attachments, str):
                    attachments = [attachments]
                if not isinstance(attachments, list):
                    attachments = list(attachments)
                param["attachments"] = attachments
            notifiers.get_notifier("email").notify(**param)
        if status == "FAILED":
            if self.email:
                self._notify_log(app_id, "Re: " + subject)
            return False
        return True

    def _notify_log(self, app_id, subject):
        logger.info("Waiting for 300 seconds for the log to be available...")
        time.sleep(300)
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
    def _app_id(stdout: List[str]) -> str:
        """Parse the application ID.

        :param stdout: Standard output as a list of strings.
        :return: The application ID of the Spark application.
        """
        for line in reversed(stdout):
            match = re.search(r"(application_\d+_\d+)", line)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def _final_status(stdout: List[str]) -> str:
        """Parse the final status of the Spark application.

        :param stdout: Standard output as a list of strings.
        :return: The final status (SUCCEED or FAILED) of the Spark application.
        """
        for line in reversed(stdout):
            if "final status: " in line:
                return line.split(": ")[-1]
        return ""


def _files(config: Dict) -> str:
    """Get a list of valid configuration files to use with the option --files.

    :param config: A dict object containing configurations.
    :return: A string containing Spark configuration files separated by comma.
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


def _submit_local(args, config: Dict[str, Any]) -> bool:
    spark_submit = config.get("spark-submit-local", "")
    if not spark_submit:
        return True
    if not os.path.isfile(spark_submit):
        raise ValueError(f"{spark_submit} does not exist!")
    lines = [
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        spark_submit
    ]
    if config["jars"]:
        lines.append(f"--jars {config['jars']}")
    lines.append("--conf spark.yarn.maxAppAttempts=1")
    python = shutil.which("python3")
    lines.append("--conf spark.pyspark.driver.python=" + config["conf"].get("spark.pyspark.driver.python", python))
    lines.append("--conf spark.pyspark.python=" + config["conf"].get("spark.pyspark.python", python))
    lines.extend(args.cmd)
    for idx in range(2, len(lines)):
        lines[idx] = " " * 4 + lines[idx]
    return SparkSubmit().submit(" \\\n".join(lines) + "\n", args.cmd[:1])


def _submit_cluster(args, config: Dict[str, Any]) -> bool:
    spark_submit = config.get("spark-submit", ""):
    if not spark_submit:
        logger.warning("The filed spark-submit is not defined!")
        return True
    if not os.path.isfile(spark_submit):
        raise ValueError(f"{spark_submit} does not exist!")
    opts = (
        "files", "master", "deploy-mode", "queue", "num-executors", "executor-memory",
        "driver-memory", "executor-cores", "archives"
    )
    lines = [config["spark-submit"]] + [
        f"--{opt} {config[opt]}" for opt in opts if opt in config
    ] + [f"--conf {k}={v}" for k, v in config["conf"].items()]
    if config["jars"]:
        lines.append(f"--jars {config['jars']}")
    lines.extend(args.cmd)
    for idx in range(1, len(lines)):
        lines[idx] = " " * 4 + lines[idx]
    return SparkSubmit(email=config["email"]
                      ).submit(" \\\n".join(lines) + "\n", args.cmd[:1])


def submit(args: Namespace) -> None:
    """Submit the Spark job.

    :param args: A Namespace object containing command-line options.
    """
    with open(args.config, "r") as fin:
        config = yaml.load(fin, Loader=yaml.FullLoader)
    if "files" not in config:
        config["files"] = {}
    config["files"] = _files(config)
    if "jars" not in config:
        config["jars"] = ""
    if isinstance(config["jars"], (list, tuple)):
        config["jars"] = ",".join(config["jars"])
    if _submit_local(args, config):
        _submit_cluster(args, config)


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.

    :param args: Arguments to parse. If None, arguments from command line is used.
    :param namespace: An initial Namespace object to use.
    :return: A Namespace object containing command-line options.
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
