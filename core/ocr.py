import os

from surya.common.surya.schema import TaskNames
from surya.detection import DetectionPredictor
from surya.foundation import FoundationPredictor
from PIL import Image
from surya.recognition import OCRResult, RecognitionPredictor

# Load models if not already loaded in reload mode
recognition_predictor = RecognitionPredictor(FoundationPredictor())
detection_predictor = DetectionPredictor()


def ocr(
        img: Image.Image,
        skip_text_detection: bool = False,
        recognize_math: bool = True,
        with_bboxes: bool = True,
) -> (Image.Image, OCRResult):
    if skip_text_detection:
        bboxes = [[[0, 0, img.width, img.height]]]
    else:
        bboxes = None

    if with_bboxes:
        tasks = [TaskNames.ocr_with_boxes]
    else:
        tasks = [TaskNames.ocr_without_boxes]

    img_pred = recognition_predictor(
        [img],
        task_names=tasks,
        bboxes=bboxes,
        det_predictor=detection_predictor,
        highres_images=[img],
        math_mode=recognize_math,
        return_words=True,
    )[0]

    # bboxes = [line.bbox for line in img_pred.text_lines]
    # text = [line.text for line in img_pred.text_lines]

    word_boxes = []
    for line in img_pred.text_lines:
        if line.words:
            word_boxes.extend([word.bbox for word in line.words])

    # box_img = img.copy()
    # draw = ImageDraw.Draw(box_img)
    # for word_box in word_boxes:
    #     draw.rectangle(word_box, outline="red", width=2)

    # return  img_pred, box_img
    return  img_pred


import os
from PIL import Image, ImageDraw


def sort_text_lines_by_position(text_lines):
    """
    按照从上到下、从左到右的顺序对文本行进行排序

    Args:
        text_lines: OCRResult.text_lines 列表

    Returns:
        排序后的文本行列表
    """
    # 先按 y 坐标排序（从上到下）
    sorted_lines = sorted(text_lines, key=lambda line: line.bbox[1])

    # 在同一行内按 x 坐标排序（从左到右）
    result = []
    current_y = None
    current_group = []

    for line in sorted_lines:
        if current_y is None or abs(line.bbox[1] - current_y) > 5:  # 设置合理的行间距阈值
            if current_group:
                # 对当前组内的行按 x 坐标排序
                current_group.sort(key=lambda l: l.bbox[0])
                result.extend(current_group)
            current_group = [line]
            current_y = line.bbox[1]
        else:
            current_group.append(line)

    # 处理最后一组
    if current_group:
        current_group.sort(key=lambda l: l.bbox[0])
        result.extend(current_group)

    return result


def print_text_in_order(img_pred):
    """
    按照从上到下、从左到右的顺序打印文本结果

    Args:
        img_pred: OCRResult 对象
    """
    # 获取所有文本行并排序
    sorted_lines = sort_text_lines_by_position(img_pred.text_lines)

    print("=== 按行顺序打印的文本结果 ===")
    for i, line in enumerate(sorted_lines):
        print(f"第{i + 1}行:")
        print(f"  文本: '{line.text}'")
        print(f"  置信度: {line.confidence:.4f}")
        print(f"  边界框: {line.bbox}")

        # 打印字符级信息
        if line.chars:
            chars_info = [char.text for char in line.chars if char.text]
            print(f"  字符: {''.join(chars_info)}")

        # 打印单词级信息
        if line.words:
            words_info = [word.text for word in line.words]
            print(f"  单词: {' '.join(words_info)}")

        print()


if __name__ == "__main__":
    temp_dir = r"D:\PycharmProjects\XHS-OCR\tmp"
    output_dir = os.path.join(os.path.dirname(temp_dir), "ocr_result")
    os.makedirs(output_dir, exist_ok=True)

    # 遍历tmp目录下的所有png文件
    for filename in os.listdir(temp_dir):
        if filename.endswith(".png"):
            file_path = os.path.join(temp_dir, filename)

            try:
                # 打开图像
                img = Image.open(file_path)

                # 执行OCR
                img_pred= ocr(img, with_bboxes=True)

                # 按顺序打印文本结果
                print_text_in_order(img_pred)

                # # 保存带框的图像
                # base_name = os.path.splitext(filename)[0]
                # box_img.save(os.path.join(output_dir, f"{base_name}_box.png"))

            except Exception as e:
                print(f"处理文件 {filename} 时出错: {e}")
