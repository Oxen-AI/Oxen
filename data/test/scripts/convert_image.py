import cv2
import sys

# get input and output file from command line
input_file = sys.argv[1]
output_file = sys.argv[2]

# load an image using 'imread' specifying the path to image file
image = cv2.imread(input_file)

# Now we convert it from RGB to format of our choice
image_bgr = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)

# save the image
cv2.imwrite(output_file, image_bgr)