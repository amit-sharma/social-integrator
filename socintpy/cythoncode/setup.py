from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
#from Cython.Distutils import build_ext
import numpy as np

mainfiles = ["cnetwork_node.pyx"]
auxfiles = ["data_types.pyx", "binary_search.pyx"]
extensions = [
    Extension("cnetwork_node", mainfiles, #language='c++',
        include_dirs=[np.get_include()],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp']),    
    Extension("data_types", ["data_types.pyx"],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp']),    
    Extension("binary_search", ["binary_search.pyx"],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp'])    
]
setup(
    name="MyApp",
    #cmdclass = {'build_ext': build_ext},
    #include_dirs =[np.get_include()],
    ext_modules = cythonize(extensions, gdb_debug=True, output_dir="."),
)
