"""Computer vision related utils.
"""
from typing import Iterable
from pathlib import Path
from tqdm import tqdm, trange
import numpy as np
import pandas as pd
from PIL import Image
import skimage
import cv2


def video_to_image(
    file: str,
    step: int,
    bbox: tuple[int, int, int, int] | None = None,
    output: str = "frame_{:0>7}.png",
):
    """Extract images from a video file.

    :param file: The path to video file.
    :param bbox: A bounding box.
        If specified, crop images using the bounding box.
    :param output_dir: The directory to save extracted images.
    :param step: Keep 1 image every step frames.
    """
    Path(output.format(0)).parent.mkdir(parents=True, exist_ok=True)
    vidcap = cv2.VideoCapture(file)
    total = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    for idx in trange(total):
        success, arr = vidcap.read()
        if not success:
            break
        if idx % step == 0:
            img = Image.fromarray(np.flip(arr, 2))
            if bbox:
                img = img.crop(bbox)
            img.save(output.format(idx))
    vidcap.release()


def resize_image(
    paths: str | Path | Iterable[Path],
    desdir: str | Path | None,
    size: tuple[int, int],
) -> None:
    """Resize images to a given size.

    :param paths: The paths to images to be resized.
    :param desdir: The directory to save resized images.
        Notice that both '.' and '""' stand for the current directory.
        If None is specified, then the orginal image is overwritten.
    :param size: The new size of images.
    """
    if isinstance(desdir, str):
        desdir = Path(desdir)
    if isinstance(desdir, Path):
        desdir.mkdir(exist_ok=True)
    if isinstance(paths, str):
        paths = Path(paths)
    if isinstance(paths, Path):
        img = Image.open(paths)
        if img.size != size:
            img.resize(size).save(desdir / paths.name if desdir else paths)
        return
    if not hasattr(paths, "__len__"):
        paths = tuple(paths)
    for path in tqdm(paths):
        resize_image(paths=path, desdir=desdir, size=size)


def _is_approx_close(x: float, y: float, threshold: float = 0.4) -> bool:
    """Helper function of is_approx_close.
    Check whether the 2 values x and y are relative close.

    :param x: An non negative value.
    :param y: Another non negative value.
    :param threshold: The maximum ratio difference from 1 to be considered as close.
    :return: True if the 2 values are considered close and False otherwise.
    """
    if x < y:
        x, y = y, x
    return (x + 0.01) / (y + 0.01) <= 1 + threshold


def is_approx_close(red: int, green: int, blue: int, threshold: float = 0.4) -> bool:
    """Check whether the 3 channels have approximately close values.

    :param red: The red channel.
    :param green: The green channel.
    :param blue: The blue channel.
    :param threshold: The threshold (absolute deviation from 1)
        to consider a ratio (of 2 channels) to be close to 1.
    :return: True if the RGB values are approximately close to each other.
    """
    return (
        _is_approx_close(red, green, threshold=threshold)
        and _is_approx_close(red, blue, threshold=threshold)
        and _is_approx_close(green, blue, threshold=threshold)
    )


def deshade_arr_1(arr: np.ndarray, threshold: float = 0.4) -> np.ndarray:
    """Deshade a poker card (i.e., get rid of the shading effec on a poker card)
        by checking whether the 3 channels have relative close values.

    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param threshold: The threshold (absolute deviation from 1)
        to consider a ratio (of 2 channels) to be close to 1.
    :return: A new numpy ndarray with shading effect removed.
    """
    arr = arr.copy()
    nrow, ncol, _ = arr.shape
    for i in range(nrow):
        for j in range(ncol):
            r = arr[i, j, 0]
            g = arr[i, j, 1]
            b = arr[i, j, 2]
            if is_approx_close(r, g, b, threshold=threshold):
                arr[i, j, :] = (255, 255, 255)
    return arr


def deshade_arr_2(arr: np.ndarray, cutoff: float = 30) -> np.ndarray:
    """Deshade a poker card (i.e., get rid of the shading effec on a poker card)
        by checking whether the 3 channels all have values larger than a threshold.

    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff,
        then it is considered as shading effect.
    :return: A new numpy ndarray with shading effect removed.
    """
    arr = arr.copy()
    nrow, ncol, _ = arr.shape
    for i in range(nrow):
        for j in range(ncol):
            r = arr[i, j, 0]
            g = arr[i, j, 1]
            b = arr[i, j, 2]
            # if (r + g + b) / 3 >= cutoff and max(r, g, b) <= 150:
            if min(r, g, b) >= cutoff:
                arr[i, j, :] = (255, 255, 255)
    return arr


def deshade_arr_3(
    arr: np.ndarray, threshold: float = 0.4, cutoff: float = 30
) -> np.ndarray:
    """Deshade a poker card (i.e., get rid of the shading effect on a poker card)
        by combining methods in deshade_arr_1 and deshade_arr_2.

    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param threshold: The threshold (absolute deviation from 1)
        to consider a ratio (of 2 channels) to be close to 1.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff,
        then it is considered as shading effect.
    :return: A new numpy ndarray with shading effect removed.
    """
    arr = arr.copy()
    nrow, ncol, _ = arr.shape
    for i in range(nrow):
        for j in range(ncol):
            r = arr[i, j, 0]
            g = arr[i, j, 1]
            b = arr[i, j, 2]
            if min(r, g, b) >= cutoff and is_approx_close(r, g, b, threshold=threshold):
                arr[i, j, :] = (255, 255, 255)
    return arr


def deshade_1(img, threshold=0.4) -> Image.Image:
    """Deshade an image (i.e., get rid of the shading effec on an image.)
        by checking whether the 3 channels have relative close values.

    :param img: An image to deshade.
    :param threshold: The threshold (absolute deviation from 1)
        to consider a ratio (of 2 channels) to be close to 1.
    :return: The new image with shading effect removed.
    """
    arr = np.array(img)
    arr = deshade_arr_1(arr, threshold=threshold)
    return Image.fromarray(arr)


def deshade_2(img, cutoff=30) -> Image.Image:
    """Deshade an image (i.e., get rid of the shading effec on an image)
        by checking whether the 3 channels all have values larger than a threshold.

    :param img: An image to deshade.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff,
        then it is considered as shading effect.
    :return: The new image with shading effect removed.
    """
    arr = np.array(img)
    arr = deshade_arr_2(arr, cutoff=cutoff)
    return Image.fromarray(arr)


def deshade_3(img, threshold=0.4, cutoff=30) -> Image.Image:
    """Deshade an image (i.e., get rid of the shading effect on an image)
        by combining methods in deshade_arr_1 and deshade_arr_2.

    :param img: An image to deshade.
    :param threshold: The threshold (absolute deviation from 1)
        to consider a ratio (of 2 channels) to be close to 1.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff,
        then it is considered as shading effect.
    :return: The new image with shading effect removed.
    """
    arr = np.array(img)
    arr = deshade_arr_3(arr, threshold=threshold, cutoff=cutoff)
    return Image.fromarray(arr)


def add_frames(
    arr: np.ndarray | Image.Image,
    bboxes: list[tuple[int, int, int, int]],
    rgb: tuple[int, int, int] = (255, 0, 0),
) -> np.ndarray:
    """Add (highlighting) frames into an image.

    :param arr: A PIL image or its numpy array representation.
    :param bboxes: A list of bounding boxes.
    :param rgb: The RGB color (defaults to (255, 0, 0)) of the (highlighting) frame.
    :return: A numpy array representation of the altered image.
    """
    if isinstance(arr, Image.Image):
        arr = np.array(arr)
    for x1, y1, x2, y2 in bboxes:
        if x2 is None:
            x2 = x1
        if y2 is None:
            y2 = y1
        arr[y1, x1:x2, :] = rgb
        arr[y2, x1:x2, :] = rgb
        arr[y1:y2, x1, :] = rgb
        arr[y1:y2, x2, :] = rgb
    return arr


def duplicate_image(
    path: str | Path,
    copies: int,
    des_dir: str | Path | None = None,
    noise_amount: float = 0.05,
):
    """Duplicate an image with some noises added.

    :param path: The path to the image to be duplicated.
    :param copies: The number of copies to duplicate.
    :param noise_amount: Proportion of image pixels to replace with noise on range [0, 1].
    """
    if isinstance(path, str):
        path = Path(path)
    if isinstance(des_dir, str):
        des_dir = Path(des_dir)
    if des_dir is None:
        des_dir = path.parent
    des_dir.mkdir(parents=True, exist_ok=True)
    for i in range(copies):
        file_i = des_dir / f"{path.stem}_copy{i}.png"
        noise = skimage.util.random_noise(
            np.array(Image.open(path)), mode="s&p", amount=noise_amount
        )
        Image.fromarray(np.array(noise * 255, dtype=np.uint8)).save(file_i)


def structural_similarity(im1, im2) -> float:
    """Extend skimage.metrics.structural_similarity
        to calculate the similarity of (any) two images.

    :param im1: A PIL image.
    :param im2: Another PIL image.
    """
    size = im1.size
    if im2.size != size:
        im2 = im2.resize(size)
    return skimage.metrics.structural_similarity(
        np.array(im1), np.array(im2), multichannel=True
    )


def calc_image_similarities(img: Image.Image | str | Path, dir_: str | Path):
    """Calculate the similarities between an image and all images in a directory.

    :param img: A PIL image or the path to an image file.
    :param dir_: A directory containing images.
    """
    if isinstance(img, (str, Path)):
        img = Image.open(img)
    if isinstance(dir_, str):
        dir_ = Path(dir_)
    paths = list(dir_.glob("*.png"))
    sims = [structural_similarity(img, Image.open(p)) for p in tqdm(paths)]
    return pd.DataFrame(
        {
            "path": paths,
            "similarity": sims,
        }
    )
