from setuptools import setup, find_packages


REQUIRES = ["celery[redis]"]

TESTS_REQUIRE = []

setup(
    name="asyncworker",
    version="1.0",
    description="asyncworker",
    classifiers=["Programming Language :: Python", "Framework :: Celery"],
    author="",
    author_email="",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    extras_require={"test": TESTS_REQUIRE},
    install_requires=REQUIRES,
)
