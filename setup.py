import setuptools
from pathlib import Path

memoryboard_module = [
    setuptools.Extension(
        'extrainterpreters._memoryboard', sources=['src/extrainterpreters/memoryboard.c'],
        include_dirs=[],
        extra_compile_args=['-O3', '-march=native'],
        language='c'
    )
]

setuptools.setup(
    name="extrainterpreters",
    version="0.2-dev1",
    author="João S. O. Bueno",
    author_email="gwidion@gmail.com",
    description="Utilities for concurrent code using subinterpreters",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/jsbueno/extrainterpreters",
    packages=setuptools.find_packages(),
    ext_modules=memoryboard_module,
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",

    ],
    python_requires='>=3.12-alpha',
    tests_require=['pytest'],
    setup_requires=['pytest-runner'],
    install_requires=['Cython', 'wheel'],
)
