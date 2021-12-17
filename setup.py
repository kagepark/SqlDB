#Kage Park
import os
import setuptools
from sys import version_info
import subprocess

# require package
#python -m pip install wheel --user
#python -m pip install --upgrade build --user
#python -m pip install twine --user

def lib_ver():
    my_dir=os.path.dirname(os.path.abspath(__file__))
    if os.path.isdir(os.path.join(my_dir,'.git')):
        gver=subprocess.check_output('''git describe --tags >/dev/null && (git describe --tags | sed "s/^v//g" | sed "s/^V//g") || (git tag V1.0; sleep 1; git describe --tags | sed "s/^v//g" | sed "s/^V//g")''',stderr=subprocess.STDOUT,shell=True)
        if gver:
            if version_info[0] >= 3:
                if isinstance(gver,bytes): gver=gver.decode('latin1')
            else:
                if isinstance(gver,unicode): gver=gver.encode('latin1')
            gver_a=gver.split('\n')[0].split('-')
            if len(gver_a) == 1:
                return gver_a[0]
            else:
                return '.'.join(gver_a[:-1])
    else:
        my_ver=os.path.basename(my_dir)
        ver_a=my_ver.split('-')
        if len(ver_a) == 2:
            return ver_a[1]
    return 1.0

pkg_name='SqlDB'
pkg_desc='SQL Family DB handler'
pkg_git="https://github.com/kagepark/SqlDB"
long_description=''
if os.path.isfile('README.md'):
    with open("README.md", "r") as fh:
        long_description = fh.read()

setuptools.setup(
    name=pkg_name,
    version='{}'.format(lib_ver()),
    author='Kage Park',
    autor_email='kagepark1@gmail.com',
    license="MIT",
    description=pkg_desc,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=pkg_git,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
#    install_requires=["ast"],
)
