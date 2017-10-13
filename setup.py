from setuptools import setup, find_packages


def get_version(filename):
    '''
    Parse the value of the __version__ var from a Python source file
    without running/importing the file.
    '''
    import re
    version_pattern = r'^ *__version__ *= *[\'"](\d+\.\d+\.\d+)[\'"] *$'
    match = re.search(version_pattern, open(filename).read(), re.MULTILINE)

    assert match, ('No version found in file: {!r} matching pattern: {!r}'
                   .format(filename, version_pattern))

    return match.group(1)


setup(
    name='patrons_datasrc',
    version='0.0.1',
    py_modules=['patronsdatasrc'],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Hal Blackburn',
    author_email='hwtb2@cam.ac.uk',
    install_requires=['django'],
    entry_points={
        'console_scripts': []
    },
    package_data={}
)
