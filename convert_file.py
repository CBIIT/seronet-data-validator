import os
import PyInstaller.__main__

PyInstaller.__main__.run([
    'Data_Validation_v1.py',
#    '--onefile',           #will complie but does not run properly
    '--nowindowed',
    '--noupx',
    '--clean',
    '--paths=C:\\Python_Code\\',
    '--paths=C:\\Users\\breadsp2\\Anaconda3\\Scripts',
    '--paths=C:\\Users\\breadsp2\\Anaconda3\\Lib\\site-packages',
    '--paths=C:\\Users\\breadsp2\\Anaconda3\\Library\\bin\\',
    '--name=Data_Validator',
    '--exclude-module=xlrd',
    '--exclude-module=alabaster',
    '--exclude-module=argon2',
    '--exclude-module=babel',
    '--exclude-module=bcrypt',
    '--exclude-module=bokeh',
    '--exclude-module=brotli',
    '--exclude-module=certifi',
    '--exclude-module=cryptography',
    '--exclude-module=cryptography-3.1.1-py3.8.egg-info'
    '--exclude-module=Cython',
    '--exclude-module=etc',
    '--exclude-module=gevent',
    '--exclude-module=h5py',
    '--exclude-module=IPython',
    '--exclude-module=jedi',
    '--exclude-module=importlib_metadata-2.0.0-py3.8.egg-info',
    '--exclude-module=jsonschema',
    '--exclude-module=jsonschema-3.2.0-py3.8.egg-info',
    '--exclude-module=lib2to3',
    '--exclude-module=llvmlite',
    '--exclude-module=mkl'
    '--exclude-module=lxml',
    '--exclude-module=markupsafe',
    '--exclude-module=matplotlib',
    '--exclude-module=mpl-data',
    '--exclude-module=msgpack',
    '--exclude-module=nacl',
    '--exclude-module=nbconvert',
    '--exclude-module=nbconvert-6.0.7-py3.8.egg-info',
    '--exclude-module=nbformat',
    '--exclude-module=notebook',
    '--exclude-module=numba',
    '--exclude-module=numexpr',
    '--exclude-module=PIL',
    '--exclude-module=psutil',
    '--exclude-module=PyQt5',
    '--exclude-module=scipy',
    '--exclude-module=sqlalchemy',
    '--exclude-module=tables',
    '--exclude-module=tornado',
    '--exclude-module=win32com',
    '--exclude-module=zmq',
    '--exclude-module=zope',
])


file_names = os.listdir("C:\\Python_Code\\dist\\Data_Validator")
for iterZ in file_names:
    if "mkl" in iterZ:
        os.remove("C:\\Python_Code\\dist\\Data_Validator\\" + iterZ)