import setuptools

def readme():
    with open("README.md") as f:
        return f.read()


setuptools.setup(
    name="bidnamic",
    version="0.0.1",
    description="Small package to monitor and create csv files",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    url="https://github.com/emilkenguerli/bidnamic.git",
    author="EmilKenguerli",
    author_email="emil.kenguerli@gmail.com",
    license="MIT",
    keywords=["bidnamic"],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        "pandas==1.3.4",
        "watchdog==2.1.6"
    ],
)