import setuptools
import versioneer

if __name__ == "__main__":
    setuptools.setup(
        name='qcfractal',
        description='A high throughput computing and database tool for quantum chemistry.',
        author='Daniel Smith',
        author_email='dgasmith@vt.edu',
        url="https://github.com/molssi/qcfractal",
        license='BSD-3C',
        include_package_data=True,
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        packages=setuptools.find_packages(),
        install_requires=[
            # Base requires
            'bcrypt',
            'cryptography',
            'numpy>=1.7',
            'pandas',
            'pydantic>=0.19',
            'pymongo>=3.0',
            'requests',
            'tornado',

            # Database
            'mongoengine',

            # QCArchive depends
            'qcengine>=0.5.1',
            'qcelemental>=0.2.4',

            # Testing
            'pytest',
        ],
        entry_points={
            "console_scripts": [
                "qcfractal-server=qcfractal.cli.qcfractal_server:main",
                "qcfractal-manager=qcfractal.cli.qcfractal_manager:main",
            ]
        },
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
            ],
        },
        tests_require=[
            'pytest',
            'pytest-cov',
        ],
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Science/Research',
            'Programming Language :: Python :: 3',
        ],
        zip_safe=True,
    )
