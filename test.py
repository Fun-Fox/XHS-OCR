import os
from dotenv import load_dotenv
load_dotenv()

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ocr_dir = os.getenv("OCR_IMAGES_PATH", os.path.join(root_dir, "images"))

if __name__ == "__main__":
    # OCR 图片目录
    for root, dirs, files in os.walk(ocr_dir):
        # print( root, dirs, files)
        for filename in files:
            print(filename)
    # for filename in os.listdir(ocr_dir):
    #     print(filename)