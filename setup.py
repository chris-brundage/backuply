from setuptools import setup

setup(
    name='backuply',
    version='1.0.2',
    author='Chris Brundage',
    author_email='christopher.m.brundage@gmail.com',
    description='Simple backup client meant for running as a cron.',
    url='https://github.com/chris-brundage/backuply',
    packages=[
        'backuply',
    ],
    classifiers=[
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
    ],
    license='LGPLv2',
    data_files=[
        ('/usr/local/sbin', ['data_files/sbin/backuply']),
    ],
    include_package_data=True,
)
