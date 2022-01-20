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
            "bcrypt",
            "sqlalchemy >=1.4",
            "alembic",
            "psycopg2",
            "qcelemental>=0.24",
            "qcengine>=0.21",
            "torsiondrive",
            # QCPortal dependencies
            "requests",
            "tqdm",
            "plotly",
            "pandas",
            "tabulate",
            # Only need for python 3.7, but no harm in always including it
            "typing-extensions",
        ],
        entry_points={
            "console_scripts": [
                "qcfractal-server=qcfractal.qcfractal_server_cli:main",
                "qcfractal-manager=qcfractalcompute.qcfractal_manager_cli:main",
            ],
        },
        extras_require={
            "geoip": ["geoip2"],
            "docs": [
                "sphinx",
                "sphinx-automodapi",
                "sphinx_rtd_theme",
                "nbsphinx",
                "ipython",
            ],
            "lint": ["black"],
            "tests": ["pytest", "pytest-cov", "codecov", "mypy", "geoip2"],
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
