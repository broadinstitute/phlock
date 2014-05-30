from distutils.core import setup

setup(
    name='flock',
    version='1.0',
    packages=['flock',],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomer",
    author_email="pgm@lazbox.org",
    url="https://github.com/pgm/flock",
    long_description=open('README.txt').read(),
    scripts=['scripts/flock'],
    package_data={'flock': ['*.R']}
)
