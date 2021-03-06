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
    name='importar',
    license='BSD',
    version='0.0.1',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Hal Blackburn',
    author_email='hwtb2@cam.ac.uk',
    install_requires=['django'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3 :: Only',
        'Operating System :: OS Independent'
    ],
    python_requires='>=3.4, <4'
)
