from setuptools import setup

data_files = [
    ('/usr/local/sbin', ['data_files/sbin/backuply']),
    # ('/etc/sysconfig', ['data_files/etc/sysconfig/backuply']),
    ('/etc', ['data_files/etc/backuply.conf'])
]

setup(
    name='backuply',
    version='2.0.0',
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
    data_files=data_files,
    include_package_data=True,
    install_requires=[
        'PyYAML',
        'libvirt-python',
        'google-api-python-client',
        'httplib2',
        'oauth2client < 4.0.0',
        'backoff',
        'ratelimit',
    ],
)
