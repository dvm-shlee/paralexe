#!/usr/bin/env python
"""
Paralexe (PARALlel EXEcution)
"""
from distutils.core import setup
from setuptools import find_packages
import re, io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('paralexe/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

__author__ = 'SungHo Lee'
__email__ = 'shlee@unc.edu'

setup(name='Paralexe',
      version=__version__,
      description='Parallel Execution',
      author=__author__,
      author_email=__email__,
      url='https://github.com/dvm-shlee/paralexe',
      license='GNLv3',
      packages=find_packages(),
      install_requires=['tqdm',
                        'psutil>=5.7.0',
                        'shleeh>=0.0.4'
                        ],
      # scripts=['',
      #         ],
      classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Framework :: Jupyter',
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'Natural Language :: English',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.7',
      ],
      keywords='Parallel Execution'
      )
