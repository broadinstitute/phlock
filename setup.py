from distutils.core import setup

setup(
    name='flock',
    version='1.3',
    packages=['flock',],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomery",
    author_email="pgm@lazbox.org",
    url="https://github.com/pgm/flock",
    long_description=open('README.txt').read(),
    entry_points={'console_scripts': [ "phlock = flock:main" ]}
    package_data={'flock': ['*.R']}
)
