from setuptools import setup

#Following https://uoftcoders.github.io/studyGroup/lessons/python/packages/lesson/

#Look here https://stackoverflow.com/questions/458550/standard-way-to-embed-version-into-python-package
exec(open('laupy/version.py').read())

petra_requires = ['h5py', 'pandas']
script_requires = ['argparse']

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='laupy',
    url='https://github.com/kulvait/KCT_laupy',
    author='Vojtech Kulvait',
    author_email='vojtech.kulvait@hereon.de',
    # Needed to actually package something
    packages=['laupy'],
    # Needed for dependencies
    install_requires=script_requires,
    entry_points={
    'console_scripts': [
        'listnodes =  laupy.scripts.listMaxwellNodes:main',
        'submitslurm =  laupy.scripts.submitslurm:main',
   ]
    },
    # *strongly* suggested for sharing
    version=__version__,
    # The license can be anything you like
    license='GPL3',
    description='Python package for pipeline orchestration.',
)
