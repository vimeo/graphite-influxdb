# coding: utf-8
from setuptools import setup
from glob import glob

setup(
    name='graphite-influxdb',
    version='0.3',
    url='https://github.com/vimeo/graphite-influxdb',
    license='apache2',
    author='Dieter Plaetinck',
    author_email='dieter@vimeo.com',
    description=('Influxdb backend plugin for graphite-web and graphite-api'),
    long_description=open('README.rst').read(),
    py_modules=('graphite_influxdb',),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: System :: Monitoring',
    ),
    scripts=glob('bin/*.py'),
    install_requires=(
        'influxdb',
    ),
)
