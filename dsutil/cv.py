from typing import Union, Tuple
from pathlib import Path
from tqdm import tqdm
import numpy as np
from PIL import Image
import cv2


def video_to_image(
    video, frames_per_image: int = 60, output: str = "frame_{:0>7}.png"
):
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


def resize_image(path: Union[str, Path], size: Tuple[int]) -> None:
    """Resize an image or images in a directory to the given size.
    :param path: The path to an image or a directory containing images to be resized.
    :param size: The new size of images.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        files = list(path.glob("*.png"))
        for file in tqdm(files):
            resize_image(file, size)
        return
    img = Image.open(path)
    if img.size != size:
        img.resize(size).save(path)


def _is_approx_close(x, y, threshold=0.4):
    """Helper function of is_approx_close.
        Check whether the 2 values x and y are relative close. 
    """
    ratio = (x + 0.01) / (y + 0.01)
    return 1 - threshold <= ratio <= 1 + threshold


def is_approx_close(red: int, green: int, blue: int, threshold: float = 0.4):
    """Check whether the 3 channels have approximately close values.
    :param red: The red channel.
    :param green: The green channel.
    :param blue: The blue channel.
    :param threshold: The threshold (absolute deviation from 1) 
        to consider a ratio (of 2 channels) to be close to 1.
    """
    return _is_approx_close(red, green, threshold=threshold) and \
        _is_approx_close(red, blue, threshold=threshold) and \
        _is_approx_close(green, blue, threshold=threshold)


def deshade_arr_1(arr, threshold=0.4):
    """Deshade a poker card (i.e., get rid of the shading effec on a poker card)
        by checking whether the 3 channels have relative close values.
    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param threshold: The threshold (absolute deviation from 1) 
        to consider a ratio (of 2 channels) to be close to 1.
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


def deshade_arr_2(arr, cutoff=30):
    """Deshade a poker card (i.e., get rid of the shading effec on a poker card)
        by checking whether the 3 channels all have values larger than a threshold.
    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff, 
        then it is considered as shading effect. 
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


def deshade_arr_3(arr, threshold=0.4, cutoff=30):
    """Deshade a poker card (i.e., get rid of the shading effec on a poker card)
        by combining methods in deshade_arr_1 and deshade_arr_2.
    :param arr: A numpy.ndarray representation of the image to be deshaded.
    :param threshold: The threshold (absolute deviation from 1) 
        to consider a ratio (of 2 channels) to be close to 1.
    :param cutoff: The cutoff value of 3 channels.
        If the 3 channels all have value no less than this cutoff, 
        then it is considered as shading effect. 
    """
    arr = arr.copy()
    nrow, ncol, _ = arr.shape
    for i in range(nrow):
        for j in range(ncol):
            r = arr[i, j, 0]
            g = arr[i, j, 1]
            b = arr[i, j, 2]
            if min(r, g, b) >= cutoff and \
                is_approx_close(r, g, b, threshold=threshold):
                arr[i, j, :] = (255, 255, 255)
    return arr


def deshade_1(img, threshold=0.4):
    arr = np.array(img)
    arr = deshade_arr_1(arr, threshold=threshold)
    return Image.fromarray(arr)


def deshade_2(img, cutoff=30):
    arr = np.array(img)
    arr = deshade_arr_2(arr, cutoff=cutoff)
    return Image.fromarray(arr)


def deshade_3(img, threshold=0.4, cutoff=30):
    arr = np.array(img)
    arr = deshade_arr_3(arr, threshold=threshold, cutoff=cutoff)
    return Image.fromarray(arr)
    

def highlight_frame(rgb: Tuple[int], shape: Tuple[int], thickness: int = 3):
    """Generate a rectangle frame with the specified color and thickness.
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


def frame_image(img, rgb, thickness: int = 3):
    """Add a highlight frame to an image.
    """
    shape = img.size
    shape = (shape[1], shape[0])
    frame = highlight_frame(rgb, shape=shape, thickness=thickness)
    mask = highlight_frame((255, 255, 255), shape=shape,
                           thickness=thickness).convert("1")
    img = img.copy()
    img.paste(frame, mask=mask)
    return img
