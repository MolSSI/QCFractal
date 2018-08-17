import setuptools
import versioneer

if __name__ == "__main__":
    setuptools.setup(
        name='qcfractal',
        description='A high throughput computing and database tool for quantum chemsitry.',
        author='Daniel Smith',
        author_email='dgasmith@vt.edu',
        url="https://github.com/molssi/qcfractal",
        license='BSD-3C',
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        packages=setuptools.find_packages(),
        install_requires=[
            'numpy>=1.7',
            'pymongo>=3.0',
            'requests',
            'tornado',
            'jsonschema',
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
            ],
        },

        tests_require=[
            'pytest',
            'pytest-cov',
        ],

        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Science/Research',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
        ],
        zip_safe=True,
    )
