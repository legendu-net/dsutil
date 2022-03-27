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
import socket
import datetime
import getpass
import time
import subprocess as sp
import base64
from argparse import ArgumentParser, Namespace
import yaml
from loguru import logger
import notifiers

HOME = Path.home()
PROFILE = HOME / ".pykinit_profile"
HOST = socket.gethostname()
HOST_IP = socket.gethostbyname(HOST)
USER = getpass.getuser()
PID = os.getpid()


def save_passwd(passwd: str) -> None:
    """Encrypt and save the password into a profile that is readable/writable only by the user.

    :param passwd: The password of the user (to save).
    """
    bytes_ = passwd.encode("ascii")
    encode = base64.b64encode(bytes_).decode()
    with open(PROFILE, "w", encoding="utf-8") as fout:
        fout.write(encode)
    os.chmod(PROFILE, 0o600)


def read_passwd() -> str:
    """Read in the saved password.

    :return: Password in the profile file or empty string if the file does not exist.
    """
    if os.path.isfile(PROFILE):
        os.chmod(PROFILE, 0o600)
        with open(PROFILE, "r", encoding="utf-8") as fin:
            return base64.b64decode(fin.read()).decode()
    passwd = input("Please enter password for kinit: ")
    save_passwd(passwd)
    return passwd


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


def authenticate(password: str, email: Dict[str, str], user: str = "") -> None:
    """Authenticate using the shell command /usr/bin/kinit.

    :param password: The password of the user.
    :param email: A dict containing email information ("from", "to" and "host").
    :param user: User name. If empty, the current user name is used.
    """
    SUBJECT = "kinit: authentication {}"
    MSG = f'kinit ({PID}): authentication on {HOST} ({HOST_IP}) {"{}"} at {datetime.datetime.now()}'
    try:
        process = sp.run(
            ["/usr/bin/kinit", user if user else USER],
            input=password.encode(),
            check=True,
            capture_output=True
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


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments for the script.

    :param args: The arguments to parse. 
    If None, the command-line arguments are parsed.
    :param namespace: An initial Namespace object.
    :return: A Namespace object containing parsed command-line options.
    """
    parser = ArgumentParser(description="Easy kinit authentication.")
    parser.add_argument(
        "-u",
        "--user",
        dest="user",
        default="",
        help="The name of the user to authenticate."
    )
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
        default=HOME / ".pykinit.yaml",
        help="The path to a configure file which contains email information."
    )
    return parser.parse_args(args, namespace)


def _read_config(config: Union[Path, str]) -> Dict[str, Any]:
    if isinstance(config, str):
        config = Path(config)
    if not config.is_file():
        return {"email": {}}
    with config.open("r", encoding="utf-8") as fin:
        return yaml.load(fin, Loader=yaml.FullLoader)


def main() -> None:
    """Authenticate the user using either supplied or saved password.

    :raises ExceptionNoPassword: If no password is provided or found.
    """
    args = parse_args()
    if args.password:
        save_passwd(args.password)
    password = read_passwd()
    config = _read_config(args.config)
    if args.minute:
        while True:
            authenticate(read_passwd(), config["email"])
            time.sleep(args.minute * 60)
    else:
        authenticate(password, config["email"], args.user)


if __name__ == "__main__":
    main()
