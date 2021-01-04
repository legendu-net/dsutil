"""DockerHub
"""
from typing import List
import requests


class DockerHub():
    """A class for manipulating Docker Hub.
    """
    def __init__(self, user: str, password: str = "", token: str = ""):
        self.user = user
        self._token = self.token(password) if password else token

    def tags(self, image: str) -> List[str]:
        """Get tags of a Docker image on Docker Hub.

        :param image: The name of a Docker image, e.g., "jupyterhub-ds".
        :return: A list of tags belonging to the Docker image.
        """
        user = self.user
        if "/" in image:
            user, image = image.split("/")
        url = f"https://hub.docker.com/v2/repositories/{user}/{image}/tags/"
        res = requests.get(url)
        return res.json()["results"]

    def token(self, password: str) -> None:
        """Generate a token of the account.

        :param password: The password of the user.
        """
        res = requests.post(
            url="https://hub.docker.com/v2/users/login/",
            data={
                "username": self.user,
                "password": password
            },
        )
        self._token = res.json()["token"]

    def delete_tag(self, image: str, tag: str = "") -> str:
        """Delete a tag of the specified Docker image.

        :param image: The name of a docker image (without tag). 
        :param tag: The tag of the Docker image (to delete).
        :return: The removed tag of the Docker image. 
            An empty string is returned if no tag is removed.
        """
        user = self.user
        if "/" in image:
            user, image = image.split("/")
        if ":" in image:
            image, tag = image.split(":")
        if not tag:
            return ""
        url = f"https://hub.docker.com/v2/repositories/{user}/{image}/tags/{tag}/"
        res = requests.delete(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"JWT {self._token}"
            }
        )
        return tag if res else ""
