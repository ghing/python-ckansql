from distutils.core import setup

setup(
    name='ckansql',
    version='0.1.0dev',
    author='Geoffrey Hing',
    author_email='geoffhing@gmail.com',
    packages=['ckansql','ckansql.test'],
    license='LICENSE.txt',
    description='Python Database API',
    long_description=open('README.txt').read(),
    install_requires=[
        'requests',
    ],
    test_requires=[
        'nose',
    ],
    test_suite='nose.collector',
)
