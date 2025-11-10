# from paddleocr import TextRecognition
# model = TextRecognition(model_name="PP-OCRv5_server_rec")
from paddleocr import PPStructureV3

model = PPStructureV3()
output = model.predict(input=r"D:\PycharmProjects\XHS-OCR\tmp\1.png", batch_size=1)
for res in output:
    res.print()
    res.save_to_img(save_path="./output/")
    res.save_to_json(save_path="./output/res.json")