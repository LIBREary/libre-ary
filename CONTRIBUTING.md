Where to start
--------------

We welcome contributions of any type (e.g., bug fixes, new features, reporting issues, documentation, etc).  If you're looking for a good place to get started you might like to peruse our current Git issues (those marked with [help wanted](https://github.com/benhg/libre-ary/labels/help%20wanted) are a good place to start).  

Coding conventions
------------------

LIBREary code should adhere to Python pep-8.  Install `flake8` and run the following code to identify non-compliant code::

  $ flake8 libreary/

Note: the continuous integration environment will (eventually) validate all pull requests using this command.

Naming conventions
==================

The following convention should be followed: ClassName, ExceptionName, GLOBAL_CONSTANT_NAME, and lowercase_with_underscores for everything else.

Version increments
==================

Libreary follows the ``major.minor[.maintenance[.build]]`` numbering scheme for versions. Once major features  for a specific milestone (minor version) are met, the minor version is incremented and released via PyPI.

Fixes to minor releases are made via maintenance releases.

Documentation
==================

Classes should be documented following the [NumPy/SciPy](https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt)
style. A concise summary is available [here](http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html). User and developer documentation is auto-generated and made available on
[ReadTheDocs](https://LIBRE-ary.readthedocs.io).


Development Process
-------------------

If you are a contributor to LIBREary at large, we recommend forking the repository and submitting pull requests from your fork.
Libreary development follows a common pull request-based workflow similar to GitHub flow (http://scottchacon.com/2011/08/31/github-flow.html). That is:

* every development activity (except very minor changes, which can be discussed in the PR) should have a related GitHub issue
* all development occurs in branches (named with a short descriptive name which includes the associated issue number, for example, `add-globus-transfer-#1`)
* the master branch is always stable
* development branches should include tests for added features
* development branches should be tested after being brought up-to-date with the master (in this way, what is being tested is what is actually going into the code; otherwise unexpected issues from merging may come up)
* branches what have been successfully tested are merged via pull requests (PRs)
* PRs should be used for review and discussion
* PRs should be reviewed in a timely manner, to reduce effort keeping them synced with other changes happening on the master branch

Git commit messages should include a single summary sentence followed by a more explanatory paragraph. Note: all commit messages should reference the GitHub issue to which they relate. A nice discussion on the topic can be found [here](https://chris.beams.io/posts/git-commit/).

Credit and Contributions
----------------------

We want to make sure that all contributors get credit for their contributions.  When you make your first contribution, it should include updating the codemeta.json file to include yourself as a contributor to the project.

Discussion and Support
----------------------

The best way to discuss development activities is via Git issues.
