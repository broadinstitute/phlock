from setuptools import setup, find_packages

setup(
    name='flock',
    version='1.3',
    packages=find_packages(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomery",
    author_email="pgm@lazbox.org",
    url="https://github.com/pgm/flock",
    long_description=open('README.txt').read(),
    entry_points={'console_scripts': [
        "phlock = flock.main:main",
        "phlock-wingman = flock.wingman:main",
        "phlock-wingman-sync = flock.wingman_sync:main"
        ]},
    package_data={'flock': ['*.R']}
)
