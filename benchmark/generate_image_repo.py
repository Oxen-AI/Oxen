import os
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import argparse
from tqdm import tqdm
import pandas as pd

def generate_noise_images(num_images, output_dir, num_dirs, image_size, show_index=False):
    print(f"Generating {num_images} images with {num_dirs} directories in {output_dir}")
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Make the images sub dir
    images_dir = os.path.join(output_dir, "images")
    os.makedirs(images_dir, exist_ok=True)

    # Make the subdirs
    for i in tqdm(range(num_dirs)):
        os.makedirs(os.path.join(images_dir, f"split_{i}"), exist_ok=True)

    # return all the image paths
    image_paths = []
    for i in tqdm(range(num_images)):
        # Create a subdirectory based on num_dirs
        subdir = os.path.join(images_dir, f"split_{i % num_dirs}")

        # Generate random noise
        noise = np.random.randint(0, 256, (image_size[0], image_size[1], 3), dtype=np.uint8)

        # Create an image from the noise array
        img = Image.fromarray(noise)

        # Add index text if requested
        if show_index:
            draw = ImageDraw.Draw(img)
            # Calculate text size to center it
            text = str(i)
            # Use a font size proportional to image size
            font_size = min(image_size) // 4
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
            except:
                # Fallback to default font if Helvetica is not available
                font = ImageFont.load_default()

            # Get text size for centering
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            # Calculate center position
            x = (image_size[0] - text_width) // 2
            y = (image_size[1] - text_height) // 2

            # Draw text with white color
            draw.text((x, y), text, fill=(255, 255, 255), font=font)

        # Save the image
        path = os.path.join(subdir, f"noise_image_{i}.tiff")
        img.save(path)

        # Get the relative path to the output_dir
        relative_path = os.path.relpath(path, output_dir)
        image_paths.append(relative_path)

    return image_paths

if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_images", type=int, default=10000)
    parser.add_argument("--num_dirs", type=int, default=1000)
    parser.add_argument("--output_dir", type=str, default="noise_images")
    parser.add_argument("--image_size", type=int, nargs=2, default=(128, 128))
    parser.add_argument("--show_index", action="store_true", help="Draw the image index in the center of each image")
    # TODO: Add random sample % as a parameter and use that instead of mod
    args = parser.parse_args()

    image_paths = generate_noise_images(args.num_images, args.output_dir, args.num_dirs, args.image_size, args.show_index)
    print("Image generation complete!")

    # create random labels for each image of cat or dog
    labels = np.random.choice(["cat", "dog"], size=args.num_images)

    # write dataframe
    df = pd.DataFrame({"images": image_paths, "labels": labels})
    df.to_csv(os.path.join(args.output_dir, "images.csv"), index=False)

    with open(os.path.join(args.output_dir, "README.md"), "w") as f:
        f.write(f"# Sample Repo\n\nGenerated {args.num_images} images with {args.num_dirs} directories in {args.output_dir}")    # write a README.md