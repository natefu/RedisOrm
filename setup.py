from setuptools import setup, find_packages

tests_require = [
    'pytest',
]

setup(
    name='redisorm',
    version='1.1',
    description='A searchsorted implementation for pytorch',
    keywords='redisorm',
    author='yi fu',
    author_email='natefuyi@gmail.com',
    packages=find_packages(),
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    }
)

