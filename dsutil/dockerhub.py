import requests


def tags(image: str, user: str = "dclong"):
    if "/" in image:
        user, image = image.split("/")
    url = f"https://hub.docker.com/v2/repositories/{user}/{image}/tags/"
    res = requests.get(url)
    return res.json()["results"]


def token(*, password: str, user: str = "dclong"):
    res = requests.post(
        url="https://hub.docker.com/v2/users/login/",
        data={
            "username": user,
            "password": password
        },
    )
    return res.json()["token"]


def delete_tag(token: str, image: str, tag: str = "", user: str = "dclong"):
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
            "Authorization": f"JWT {token}"
        }
    )
    if res:
        return tag
