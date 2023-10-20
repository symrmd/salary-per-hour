from setuptools import setup, find_packages

setup(
    name="metl",
    version="0.1",
    packages=find_packages(include=['models', 'models/*']),
    include_package_data=True,
    entry_points={
        "console_scripts": ["metl = cli.metl:metl"],
    }
)
