# LIBREary [![Documentation Status](https://readthedocs.org/projects/libre-ary/badge/?version=master)](https://libre-ary.readthedocs.io/en/master/?badge=master) [![Build Status](https://travis-ci.com/LIBREary/libre-ary.svg?branch=master)](https://travis-ci.com/LIBREary/libre-ary) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/71f7de3f657b4958a39cb2170f841ae5)](https://www.codacy.com/gh/LIBREary/libre-ary?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=LIBREary/libre-ary&amp;utm_campaign=Badge_Grade)


## What is this?

LIBREary is an adaptive and distributed digital archive system. It's meant to provide a resilient and distributed platform for carefully storing many copies of digital objects deemed to be of high importance.

It's designed to be as flexible as possible, with adapters for storage in many different environments across various types of technologies, including the cloud, local storage systems, other network systems, and more.

LIBREary is highly configurable and is able to store, update, retrieve, and ensure integrity of objects using many different storage backends, with configurable integrity checking and copy distribution.

## How does LIBREary work? What's the architecture like?

LIBREary is packaged as a python object, which contains several other objects. To configure LIBREary, first, a user must decide where they want data to be stored. To do this, they decide on a set of adapters to use. They must choose a single "canonical" adapter, which will store the most important copy of each resource in the LIBREary. Then, they choose a set of "levels," each of which consist of one or more adapters and a specified integrity check frequency.

They can begin to ingest digital objects into the LIBREary, which assigns them a unique identifier and stores copies of each to all of the adapters at each level they have been assigned to. Each resource will be checked for digital integrity ad a frequency also determined by the levels it has assigned to it. 

LIBREary is flexible - objects can have their levels changed as necessary, levels can be edited to have more or fewer adapters as needed, and objects can be stored or retireved as often as is needed. It's also fully distributed. Copies of objects can be stored anywhere that is supported via an adapter. The adapter interface is simple and open, so that users can create adapters easily based on their needs.

The LIBREary configuration process is relatively simple, and getting started with LIBREary should only take a few minutes.

An example of the configuration can be found in the `libre-ary/config` directory.

## How do I install LIBREary?

LIBREary is a python package, and can be istalled using python3's setup tools. First, make sure you have python3 installed on your system. Then:

`git clone https://github.com/LIBREary/libre-ary`

`cd libre-ary`

`python3 setup.py install`


LIBREary is now `pip` installable! Try `pip3 install libreary==0.0.1`!

## How do I interact with LIBRE-ary?

Please see the [quickstart guide](https://github.com/LIBREary/libre-ary/blob/master/QUICKSTART.md) or [our docs](https://libre-ary.readthedocs.io) for usage information.

## How should I cite or use LIBREary?

LIBRE-ary is free and open-source software. It is licensed under the Apache General Public License v3. You are free to use it for whatever purpose you would like, within the terms of that licence. I would ask that you give credit to the authors of LIBRE-ary and link to this GitHub repository in the documentation of any project or implementation using LIBREary. 

If you wish to cite LIBREary in academic work (or other work), please cite the following publication:

LIBRE-ary, an Open-Source, Distributed Digital Archiving System
B Glick, J Mache - The Journal of Computing Sciences in Colleges, 2019

BibTeX:

```
@article{glick2019libre,
  title={LIBRE-ary, an Open-Source, Distributed Digital Archiving System},
  author={Glick, Ben and Mache, Jens},
  journal={The Journal of Computing Sciences in Colleges},
  pages={22},
  year={2019}
}
```
## I have a feature idea or bug report.

Please feel free to open an issue in this repository. There are issue templates for feature requests and bug reports. Please provide as much information as you can, in order to help the developers understand how to reproduce bugs and desired feature behavior.

## Who are you?

The primary developer is [Ben Glick](https://glick.cloud), and he can be reached at <glick@glick.cloud>.

