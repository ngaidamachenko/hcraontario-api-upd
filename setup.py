from setuptools import setup, find_packages

setup(
    name="hcraontario-api",
    version="0.1.0",
    author="Diego Arrechea",
    author_email="diego.arrechea.job@gmail.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    url="https://github.com/Diego-Arrechea/hcraontario-api",
    license="MIT",
    description="The `hcraontario-api` is designed for seamless interaction with the hcraontario.ca API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=["requests"],
    python_requires=">=3.6",
)
