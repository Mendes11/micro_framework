import setuptools

setuptools.setup(
    packages=setuptools.find_packages(exclude=["tests"]),
    name='micro-framework',
    url='https://github.com/Mendes11/micro_framework',
    version='2.0.22',
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
        "kombu>=4.6.8,<=5",
        "websockets>=8.1,<9",
        "websocket-client>=0.57.0,<1",
        "prometheus-client<0.9",
        "loky>=2.9.0,<3",
        "psutil"
    ],
)
