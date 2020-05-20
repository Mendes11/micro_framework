import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    exclude_package_data={"": ["README.md"]},
    name='micro-framework',
    version='0.1',
    description='Framework to create microservices.',
    long_description= 'file: README.rst',
    author = 'Rafael Mendes Pacini Bachiega',
    author_email = 'rafaelmpb11@hotmail.com',
    classifiers =
    ['Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8'],
    install_requires=[
        "kombu==4.6.8",
    ],
)
