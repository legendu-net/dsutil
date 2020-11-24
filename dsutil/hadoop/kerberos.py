#!/usr/bin/env python3
"""Make it easier to authenticate users' personal accounts on Hadoop.
If an user specifiy a password when authenticating,
the password is encrypted and saved into a profile that is readable/writable only by the user.
If an user authenticate without specifying password,
the saved password is used so that users do not have to type in password to authenticate every time.
"""
from typing import Union, Dict, Any
import os
from pathlib import Path
import yaml
import base64
from argparse import ArgumentParser
import subprocess as sp
import getpass
import time
import datetime
import socket
from loguru import logger
import notifiers
PROFILE = Path.home() / ".kinit_profile"
HOST = socket.gethostname()
HOST_IP = socket.gethostbyname(HOST)
USER = getpass.getuser()
PID = os.getpid()


class ExceptionNoPassword(Exception):
    """Exception due to no password specified.
    """
    def __init__(self) -> None:
        super().__init__()
        self.value = "No password is specified."

    def __str__(self):
        return repr(self.value)


def save_passwd(passwd: str) -> None:
    """Encrypt and save the password into a profile that is readable/writable only by the user.
    """
    bytes_ = passwd.encode("ascii")
    encode = base64.b64encode(bytes_).decode()
    with open(PROFILE, "w") as fout:
        fout.write(encode)
    os.chmod(PROFILE, 0o600)


def read_passwd() -> str:
    """Read in the saved password.
    """
    os.chmod(PROFILE, 0o600)
    if not os.path.isfile(PROFILE):
        return ""
    with open(PROFILE, "r") as fin:
        return base64.b64decode(fin.read()).decode()


def _warn_passwd_expiration(process, email: Dict[str, str]):
    lines = process.stdout.decode().split("\n")
    msg = "\n".join(
        line for line in lines
        if line.strip().startswith("Warning: Your password will expire")
    )
    if msg and email:
        subject = "Your Hadoop cluster password is expiring!"
        notifiers.get_notifier("email").notify(
            from_=email["from"],
            to=email["to"],
            subject=subject,
            message=msg,
            host=email["host"],
            username="",
            password="",
        )


def authenticate(password: str, email: Dict[str, str]) -> None:
    """Authenticate using the shell command /usr/bin/kinit.
    """
    SUBJECT = "kinit: authentication {}"
    MSG = f'kinit ({PID}): authentication on {HOST} ({HOST_IP}) {"{}"} at {datetime.datetime.now()}'
    try:
        process = sp.run(
            ["/usr/bin/kinit"],
            input=password.encode(),
            check=True,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )
        subject = SUBJECT.format("succeeded")
        msg = MSG.format("succeeded")
        logger.info(msg)
        if email:
            notifiers.get_notifier("email").notify(
                from_=email["from"],
                to=email["to"],
                subject=subject,
                message=msg,
                host=email["host"],
                username="",
                password="",
            )
        _warn_passwd_expiration(process, email)
    except sp.CalledProcessError:
        subject = SUBJECT.format("failed")
        msg = MSG.format("failed")
        logger.warning(msg)
        if email:
            notifiers.get_notifier("email").notify(
                from_=email["from"],
                to=email["to"],
                subject=subject,
                message=msg,
                host=email["host"],
                username="",
                password="",
            )


def parse_args(args=None, namespace=None):
    """Parse command-line arguments for the script.
    """
    parser = ArgumentParser(description="Easy kinit authentication.")
    parser.add_argument(
        "-p", "--password", dest="password", default="", help="the user's password."
    )
    parser.add_argument(
        "-m",
        "--minute",
        dest="minute",
        type=int,
        default=None,
        help="Run the script as a deamon with the specified frequency (in minutes)."
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        type=Path,
        default=None,
        help="The path to a configure file which contains email information."
    )
    return parser.parse_args(args, namespace)


def _read_config(config: Union[Path, str, None]) -> Dict[str, Any]:
    if not config:
        return {}
    if isinstance(config, str):
        config = Path(config)
    with config.open("r") as fin:
        return yaml.load(fin, Loader=yaml.FullLoader)


def main() -> None:
    """Authenticate the user using either supplied or saved password.
    """
    args = parse_args()
    if args.password:
        save_passwd(args.password)
    password = read_passwd()
    if not password:
        raise ExceptionNoPassword()
    config = _read_config(args.config)
    authenticate(password, config["email"])
    if args.minute:
        while True:
            authenticate(read_passwd(), config["email"])
            time.sleep(args.minute * 60)


if __name__ == "__main__":
    main()
