"""Setup configuration for de-intern-2024 project."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="de-intern-2024",
    version="0.1.0",
    author="Data Engineering Intern",
    author_email="intern@example.com",
    description="AWS Data Engineering Internship Project - NYC Taxi Data Pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/de-intern-2024-project",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.34.0",
        "pandas>=2.1.0",
        "psycopg2-binary>=2.9.9",
        "sqlalchemy>=2.0.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.7.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "de-intern=de_intern_2024.cli:main",
        ],
    },
)
