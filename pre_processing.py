from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
import os
import cv2
import numpy as np
import time


@task
def read_image(file_path):
    time.sleep(2)
    img = cv2.imread(file_path)
    return img


@task
def resize_image(img, target_size):
    time.sleep(3)
    resized_img = cv2.resize(img, target_size)
    return resized_img


@task
def convert_to_grayscale(img):
    time.sleep(3)
    grayscale_img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return grayscale_img


@task
def normalize_image(img):
    time.sleep(2)
    normalized_img = img / 255.0
    return normalized_img


@task
def apply_edge_detection(img):
    time.sleep(5)
    edges = cv2.Canny(np.uint8(img * 255), 100, 200)
    return edges


@task
def list_image_files(directory):
    time.sleep(3)
    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".jpg") or f.endswith(".png")
    ]


@task
def create_summary_artifact(
    processed_images_count, edge_detection_applied, target_size
):
    markdown_report = f"""# Image Processing Summary

## Total Images Processed

- **Number of Images:** {processed_images_count}

## Processing Details

- **Edge Detection Applied:** {'Yes' if edge_detection_applied else 'No'}
- **Image Resizing:** To {target_size[0]}x{target_size[1]} pixels
- **Grayscale Conversion:** Applied
- **Normalization:** Pixel values scaled to 0-1

## Conclusion

The image preprocessing flow has successfully processed {processed_images_count} images.
"""
    create_markdown_artifact(
        key="image-processing-summary",
        markdown=markdown_report,
        description="Summary of Image Processing Flow",
    )


@flow(log_prints=True)
def image_preprocessing_flow(
    image_directory, target_size=(128, 128), use_edge_detection=True
):
    image_files = list_image_files(image_directory)
    processed_images_count = len(image_files)
    processed_images = []

    for file_path in image_files:
        img = read_image(file_path)
        resized_img = resize_image(img, target_size)
        grayscale_img = convert_to_grayscale(resized_img)
        normalized_img = normalize_image(grayscale_img)

        if use_edge_detection:
            edge_img = apply_edge_detection(normalized_img)
            processed_images.append(edge_img)
        else:
            processed_images.append(normalized_img)

    create_summary_artifact(processed_images_count, use_edge_detection, target_size)

    return processed_images


if __name__ == "__main__":
    image_directory = "images"
    processed_images = image_preprocessing_flow(image_directory)
