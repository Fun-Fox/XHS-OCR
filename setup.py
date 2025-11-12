from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="xhs-ocr-merge",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="XHS OCR数据融合同步工具",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/xhs-ocr-merge",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pymysql",
        "python-dotenv",
        "configparser",
    ],
    entry_points={
        "console_scripts": [
            "xhs-ocr-merge=db.cli:main",
        ],
    },
)