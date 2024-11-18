from setuptools import setup, find_packages

with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name="chestnut-tools",
    version="0.0.1",
    description="chestnut-tools is a package that compliments the work conducted by the ChestnutRidge team. The tools in this package will provide clean functions to interact with the various datasets collected by the scientists associated with the project.",
    author="907Resident",
    author_email="moyo.ajayi.ds@gmail.com",
    license="MIT",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[  # Optional classifiers
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
