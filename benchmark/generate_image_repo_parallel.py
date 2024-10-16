import os
import numpy as np
from PIL import Image
import argparse
from tqdm import tqdm
import pandas as pd
from multiprocessing import Pool, cpu_count

def generate_single_image(args):
    i, num_dirs, images_dir, image_size, output_dir = args
    subdir = os.path.join(images_dir, f"split_{i % num_dirs}")
    noise = np.random.randint(0, 256, (image_size[0], image_size[1], 3), dtype=np.uint8)
    img = Image.fromarray(noise)
    path = os.path.join(subdir, f"noise_image_{i}.png")
    img.save(path)
    return os.path.relpath(path, output_dir)

def generate_noise_images(num_images, output_dir, num_dirs, image_size):
    print(f"Generating {num_images} images with {num_dirs} directories in {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    images_dir = os.path.join(output_dir, "images")
    os.makedirs(images_dir, exist_ok=True)

    for i in range(num_dirs):
        os.makedirs(os.path.join(images_dir, f"split_{i}"), exist_ok=True)

    # Prepare arguments for parallel processing
    args_list = [(i, num_dirs, images_dir, image_size, output_dir) for i in range(num_images)]

    # Use multiprocessing to generate images in parallel
    with Pool(processes=cpu_count()) as pool:
        image_paths = list(tqdm(pool.imap(generate_single_image, args_list), total=num_images))

    return image_paths


if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_images", type=int, default=10000)
    parser.add_argument("--num_dirs", type=int, default=1000)
    parser.add_argument("--output_dir", type=str, default="noise_images")
    parser.add_argument("--image_size", type=int, nargs=2, default=(128, 128))
    # TODO: Add random sample % as a parameter and use that instead of mod
    args = parser.parse_args()


    image_paths = generate_noise_images(args.num_images, args.output_dir, args.num_dirs, args.image_size)
    print("Image generation complete!")

    # create random labels for each image of cat or dog
    labels = np.random.choice(["cat", "dog"], size=args.num_images)

    # write dataframe
    df = pd.DataFrame({"images": image_paths, "labels": labels})
    df.to_csv(os.path.join(args.output_dir, "images.csv"), index=False)

    with open(os.path.join(args.output_dir, "README.md"), "w") as f:
        f.write(f"# Sample Repo\n\nGenerated {args.num_images} images with {args.num_dirs} directories in {args.output_dir}")    # write a README.md