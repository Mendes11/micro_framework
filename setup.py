import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    name='micro-framework',
    url='https://github.com/Mendes11/micro_framework',
    version='1.2.2',
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
    'Programming Language :: Python :: 3.8'],
    install_requires=[
        "kombu>=4.6.8",
    ],
)
