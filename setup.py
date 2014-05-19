from distutils.core import setup

setup(
    name='SocIntPy',
    version='0.1.0',
    author='Amit Sharma',
    author_email='as2447@cornell.edu',
    packages=['socintpy', 'socintpy.test'],
    scripts=['bin/tes'],
    url='http://pypi.python.org/pypi/SocIntPy/',
    license='LICENSE.txt',
    description='Here is socintpy',
    long_description=open('README.txt').read(),
    install_requires=[
        "matplotlib >= 1.1.1",
        "numpy >= 1.6.2",
        "cython" >= "0.20.1",
    ],
)
