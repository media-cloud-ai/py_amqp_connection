#!/usr/bin/env python

import os
from distutils.core import setup

dir_name = os.path.dirname(__file__)

setup(
    name = 'amqp_connection',
    version = '1.0',
    description = 'Python AMQP connection for worker',
    license='MIT',
    keywords = [
        'AMQP',
        'connection'
    ],
    author = 'Marc-Antoine ARNAUD, Valentin NOEL',
    author_email = 'valent.noel@gmail.com',
    url = 'https://github.com/FTV-Subtil/py_amqp_connection',
    packages = [
        'amqp_connection'
    ],
    package_dir = {
        'amqp_connection': os.path.join(dir_name, 'src')
    },
)
