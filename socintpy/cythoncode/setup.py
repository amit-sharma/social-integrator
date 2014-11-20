from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
#from Cython.Distutils import build_ext
import numpy as np

sourcefiles = ["cnetwork_node.pyx"]
extensions = [Extension("cnetwork_node", sourcefiles, language='c++',
        include_dirs=[np.get_include()])]
setup(
    name="MyApp",
    #cmdclass = {'build_ext': build_ext},
    #include_dirs =[np.get_include()],
    ext_modules = cythonize(extensions),
)
