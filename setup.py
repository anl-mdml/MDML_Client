import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "mdml_client",
    version = "1.1.108",
    author = "Jakob Elias",
    author_email = "jelias@anl.gov",
    description = "Client to connect to the MDML",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/anl-mdml/MDML_Client",
    packages = setuptools.find_packages(),
    install_requires=[
        "opencv-python",
        "pandas",
        "funcx",
        "paho-mqtt",
        "boto3",
	    "matplotlib",
        "confluent_kafka"
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
