from distutils.core import setup
setup(name='sswsdk',
      version='1.0',
      packages=['pysswsdk'],
      package_dir={'pysswsdk':'src/pysswsdk'},
      package_data={'pysswsdk':['data/*.json']},       
      description='Python library for utility functions and Redis operatoins',
      author='Chen Liu',
      author_email='liuchen@microsoft.com',
      )
