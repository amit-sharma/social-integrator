from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
setup(
    ext_modules = cythonize("cnetwork_node.pyx")
    #ext_modules = [Extension("cnetwork_node", ["cnetwork_node.pyx"])]
)
