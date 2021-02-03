import setuptools

setuptools.setup(
    packages=setuptools.find_packages(exclude=["tests"]),
    name='micro-framework',
    url='https://github.com/Mendes11/micro_framework',
    version='2.0.5',
    description='Framework to create CPU or IO bound microservices.',
    long_description= 'file: README.md',
    author = 'Rafael Mendes Pacini Bachiega',
    author_email = 'rafaelmpb11@hotmail.com',
    classifiers =
    ['Intended Audience :: Developers',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9'],
    install_requires=[
        "kombu>=4.6.8,<5", "websockets>=8.1", "websocket-client>=0.57.0",
        "prometheus-client>=0.8.0", "loky>=2.9.0"
    ],
)
