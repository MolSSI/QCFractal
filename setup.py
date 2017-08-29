import setuptools

if __name__ == "__main__":
    setuptools.setup(
        name='DatenQM',
        version="alpha",
        description='A MongoDB backend for QCDB',
        author='Daniel Smith',
        author_email='dgasmith@gatech.edu',
        url="https://github.com/psi4/mongo_qcdb",
        license='BSD-3C',
        packages=setuptools.find_packages(),
        install_requires=[
            'numpy>=1.7',
            'pymongo>=3.0',
            'dask>=0.15',
            'distributed>=1.18',
            'matplotlib>=2.0',
            'pymongo>=3.0',
        ],
        extras_require={
            'docs': [
                'sphinx==1.2.3',  # autodoc was broken in 1.3.1
                'sphinxcontrib-napoleon',
                'sphinx_rtd_theme',
                'numpydoc',
            ],
            'tests': [
                'pytest',
                'pytest-cov',
                'pytest-pep8',
                'tox',
            ],
        },

        tests_require=[
            'pytest',
            'pytest-cov',
            'pytest-pep8',
            'tox',
        ],

        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Science/Research',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
        ],
        zip_safe=True,
    )
