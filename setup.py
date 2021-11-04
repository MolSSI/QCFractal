import setuptools

import versioneer

short_description = "A distributed compute and database platform for quantum chemistry."

try:
    with open("README.md", "r") as handle:
        long_description = handle.read()
except FileNotFoundError:
    long_description = short_description

if __name__ == "__main__":
    setuptools.setup(
        name="qcfractal",
        description=short_description,
        author="The QCArchive Development Team",
        author_email="qcarchive@molssi.org",
        url="https://github.com/molssi/qcfractal",
        license="BSD-3C",
        include_package_data=True,
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        packages=setuptools.find_packages(),
        python_requires=">=3.7",
        install_requires=[
            # Core dependencies
            "numpy",
            "msgpack",
            "flask",
            "flask_jwt_extended",
            "gunicorn",
            "pyyaml",
            "pydantic",
            "pyarrow",
            "bcrypt",
            "sqlalchemy >=1.4",
            "alembic",
            "psycopg2",
            "qcelemental>=0.23",
            "geoip2",
            # QCPortal dependencies
            "requests",
            "tqdm",
            "plotly",
            "pandas",
            "tabulate",
            "h5py",
            "qcengine>=0.20",
        ],
        entry_points={
            "console_scripts": [
                "qcfractal-server=qcfractal.cli.qcfractal_server:main",
                "qcfractal-manager=qcfractal.cli.qcfractal_manager:main",
            ],
            "pytest11": ["qcfractal_testing=qcfractal.testing"],
        },
        extras_require={
            "api_logging": ["geoip2"],
            "docs": [
                "sphinx==1.2.3",  # autodoc was broken in 1.3.1
                "sphinxcontrib-napoleon",
                "sphinx_rtd_theme",
                "numpydoc",
            ],
            "lint": ["black"],
            "tests": ["pytest", "pytest-cov", "codecov", "mypy"],
        },
        tests_require=["pytest", "pytest-cov", "codecov", "mypy"],
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Science/Research",
            "Programming Language :: Python :: 3",
        ],
        zip_safe=True,
        long_description=long_description,
        long_description_content_type="text/markdown",
    )
