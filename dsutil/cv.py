"""Computer vision related utils.
"""
from __future__ import annotations
from typing import Union, Iterable
from pathlib import Path
from tqdm import tqdm
import numpy as np
from PIL import Image
import cv2


def video_to_image(video, frames_per_image: int = 60, output: str = "frame_{:0>7}.png"):
    """Extract images from a video.

    :param video: The path to a video file.
    :param frames_per_image: Extract 1 image every frames_per_image.
    :param output: The pattern of the output files for the extracted images.
    """
    vidcap = cv2.VideoCapture(video)
    count = 0
    while True:
        success, image = vidcap.read()
        if not success:
            break
        if count % frames_per_image == 0:
            cv2.imwrite(output.format(count), image)
        count += 1


def resize_image(
    paths: Union[str, Path, Iterable[Path]], desdir: Union[str, Path, None],
    size: tuple[int]
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
    return _is_approx_close(red, green, threshold=threshold) and \
        _is_approx_close(red, blue, threshold=threshold) and \
        _is_approx_close(green, blue, threshold=threshold)


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
            #if (r + g + b) / 3 >= cutoff and max(r, g, b) <= 150:
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


def highlight_frame(
    rgb: tuple[int, int, int],
    shape: tuple[int, int],
    thickness: int = 3
) -> Image.Image:
    """Generate a rectangle frame with the specified color and thickness.

    :param rgb: The color in RGB (as a tuple) to use for the frame.
    :param shape: The shape of the frame.
    :param thickness: The thickness of the frame.
    :return: A PIL image presenting the frame.
    """
    nrow = shape[0]
    ncol = shape[1]
    arr = np.zeros((nrow, ncol, 3), np.uint8)
    arr[0:thickness, :, 0] = rgb[0]
    arr[0:thickness, :, 1] = rgb[1]
    arr[0:thickness, :, 2] = rgb[2]
    arr[(nrow - thickness):nrow, :, 0] = rgb[0]
    arr[(nrow - thickness):nrow, :, 1] = rgb[1]
    arr[(nrow - thickness):nrow, :, 2] = rgb[2]
    arr[:, 0:thickness, 0] = rgb[0]
    arr[:, 0:thickness, 1] = rgb[1]
    arr[:, 0:thickness, 2] = rgb[2]
    arr[:, (ncol - thickness):ncol, 0] = rgb[0]
    arr[:, (ncol - thickness):ncol, 1] = rgb[1]
    arr[:, (ncol - thickness):ncol, 2] = rgb[2]
    return Image.fromarray(arr)


def frame_image(
    img: Image.Image, rgb: tuple[int, int, int], thickness: int = 3
) -> Image.Image:
    """Add a highlight frame to an image.

    :param img: A PIL image.
    :param rgb: The color in RGB (as a tuple) to use for the frame.
    :param thickness: The thickness of the frame.
    :return: A new image with the frame added.
    """
    shape = img.size
    shape = (shape[1], shape[0])
    frame = highlight_frame(rgb, shape=shape, thickness=thickness)
    mask = highlight_frame((255, 255, 255), shape=shape,
                           thickness=thickness).convert("1")
    img = img.copy()
    img.paste(frame, mask=mask)
    return img


def add_frames(
    arr: Union[np.ndarray, Image.Image],
    bboxes: list[tuple[int, int, int, int]],
    rgb: tuple[int, int, int] = (255, 0, 0)
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
