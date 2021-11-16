from setuptools import setup, find_packages


REQUIRES = ["asyncworker", "pyramid", "pyramid_debugtoolbar", "waitress"]

TESTS_REQUIRE = []

setup(
    name="webapp",
    version="1.0",
    description="webapp",
    classifiers=["Programming Language :: Python", "Framework :: Pyramid"],
    author="",
    author_email="",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    extras_require={"test": TESTS_REQUIRE},
    install_requires=REQUIRES,
    entry_points={"paste.app_factory": ["main = webapp:main"]},
)
