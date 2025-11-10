import torch
from transformers import AutoProcessor, AutoModelForCausalLM
from PIL import Image
import os
import logging
from typing import Optional

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PaddleOCRVLInference:
    """
    PaddleOCR-VL 模型推理类
    """

    def __init__(self, model_path: str):
        """
        初始化模型

        Args:
            model_path: 模型路径
        """
        self.model_path = model_path
        self.processor = None
        self.model = None
        self.device = "cpu"

    def load_model(self):
        """
        加载模型和处理器
        """
        logger.info(f"正在加载模型: {self.model_path}")

        try:
            # 加载处理器
            self.processor = AutoProcessor.from_pretrained(
                self.model_path,
                trust_remote_code=True,
                use_fast=True
            )

            # 尝试加载 float16 模型
            try:
                logger.info("尝试加载 float16 模型...")
                self.model = AutoModelForCausalLM.from_pretrained(
                    self.model_path,
                    trust_remote_code=True,
                    torch_dtype=torch.float16,
                    low_cpu_mem_usage=True
                ).to(self.device)
                logger.info("使用 float16 精度")
            except Exception as e:
                logger.warning(f"float16 加载失败，回退到 float32: {e}")
                self.model = AutoModelForCausalLM.from_pretrained(
                    self.model_path,
                    trust_remote_code=True,
                    torch_dtype=torch.float32,
                    low_cpu_mem_usage=True
                ).to(self.device)
                logger.info("使用 float32 精度")

            # 设置模型为评估模式
            self.model.eval()
            self.model.generation_config.pad_token_id = self.model.config.pad_token_id

            logger.info("模型加载成功")

        except Exception as e:
            logger.error(f"模型加载失败: {e}")
            raise

    def preprocess_image(self, image_path: str) -> Optional[Image.Image]:
        """
        图像预处理

        Args:
            image_path: 图像路径

        Returns:
            处理后的图像对象，如果失败返回 None
        """
        try:
            if not os.path.exists(image_path):
                logger.error(f"图像文件不存在: {image_path}")
                return None

            logger.info(f"正在加载图像: {image_path}")
            image = Image.open(image_path)

            # 确保图像是 RGB 模式
            if image.mode != 'RGB':
                logger.info("将图像转换为 RGB 模式")
                image = image.convert('RGB')

            logger.info(f"图像尺寸: {image.size}")
            return image

        except Exception as e:
            logger.error(f"图像预处理失败: {e}")
            return None

    def generate_text(self, image: Image.Image, prompt: str = "请用中文描述图片内容。") -> str:
        """
        生成文本

        Args:
            image: 输入图像
            prompt: 提示文本

        Returns:
            生成的文本
        """
        try:
            cls_token = "<|begin_of_sentence|>"
            if image:
                prompt = f"{cls_token}User: <|IMAGE_START|><|IMAGE_PLACEHOLDER|><|IMAGE_END|>{prompt}\nAssistant: "
            else:
                prompt = f"{cls_token}User: {prompt}\nAssistant: "

            logger.info("正在处理输入...")
            inputs = self.processor(text=prompt, images=image, return_tensors="pt").to(self.device)

            max_tokens = 131072
            temperature = 0.8

            with torch.no_grad():
                logger.info("开始生成文本...")
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_tokens,
                    temperature=temperature,
                    do_sample=temperature > 0,
                    use_cache=True,
                    pad_token_id=self.processor.tokenizer.pad_token_id,
                    eos_token_id=self.processor.tokenizer.eos_token_id,
                    repetition_penalty=1.2,
                    no_repeat_ngram_size=3,
                    top_p=0.9,
                    top_k=50
                )

            full_output = self.processor.tokenizer.decode(outputs[0], skip_special_tokens=True)

            # 提取生成的文本
            if "Assistant: " in full_output:
                parts = full_output.split("Assistant: ")
                generated = parts[-1].strip() if len(parts) > 1 else full_output
            else:
                generated = full_output.replace(prompt, "").strip()

            logger.info(f"生成文本完成，长度: {len(generated)}")
            return generated

        except Exception as e:
            logger.error(f"文本生成失败: {e}")
            raise

    def run_inference(self, image_path: str, prompt: str = "请用中文描述图片内容。") -> str:
        """
        执行完整的推理流程

        Args:
            image_path: 图像路径
            prompt: 提示文本

        Returns:
            生成的文本
        """
        try:
            # 加载模型（如果尚未加载）
            if self.model is None:
                self.load_model()

            # 预处理图像
            image = self.preprocess_image(image_path)
            if image is None:
                return ""

            # 生成文本
            result = self.generate_text(image, prompt)
            return result

        except Exception as e:
            logger.error(f"推理执行失败: {e}")
            raise


def main():
    """
    主函数
    """
    # 配置参数
    model_path = r"D:\PycharmProjects\XHS-OCR\model\PaddleOCR-VL"
    image_path = r"D:\PycharmProjects\XHS-OCR\images\20251029\test\screenshot-20251104-121257.png"
    prompt = """
    获取关注、粉丝、获赞收藏的数据
    """

    # 创建推理实例
    inference = PaddleOCRVLInference(model_path)

    # 执行推理
    result = inference.run_inference(image_path, prompt)

    # 输出结果
    print(result)


if __name__ == "__main__":
    main()
