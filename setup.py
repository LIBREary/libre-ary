from setuptools import setup, find_packages

with open('libreary/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='libreary',
    version=VERSION,
    description='Distributed Digital Object Archive System',
    long_description='Distributed Digital Object Archive System',
    url='https://github.com/LIBREary/libre-ary',
    author='Ben Glick',
    install_requires=install_requires,
    author_email='glick@glick.cloud',
    license='Apache 2.0',
    download_url='https://github.com/LIBREary/libre-ary#{}'.format(VERSION),
    include_package_data=True,
    packages=find_packages(),
    classifiers=[
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['Archive', 'Distributed Systems']
)
