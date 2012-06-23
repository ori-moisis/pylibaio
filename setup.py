from distutils.core import setup, Extension

version = '0.1'

mymodule = \
    Extension('pylibaio',
    sources = ['src/pylibaio.cpp'],
    libraries = ['aio'])

setup(
    name = 'pylibaio',
    version = '0.1',
    description = 'Python libaio bindings (libaio.h)',
    author = 'Ori Moisis',
    author_email = 'ori_moisis@hotmail.com',
    license='BSD',
    ext_modules = [mymodule])
