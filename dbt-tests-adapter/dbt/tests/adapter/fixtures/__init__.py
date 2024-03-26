# These are the fixtures that are used in dbt core functional tests
#
# The main functional test fixture is the 'project' fixture, which combines
# other fixtures, writes out a dbt project in a temporary directory, creates a temp
# schema in the testing database, and returns a `TestProjInfo` object that
# contains information from the other fixtures for convenience.
#
# The models, macros, seeds, snapshots, tests, and analyses fixtures all
# represent directories in a dbt project, and are all dictionaries with
# file name keys and file contents values.
#
# The other commonly used fixture is 'project_config_update'. Other
# occasionally used fixtures are 'profiles_config_update', 'packages',
# and 'selectors'.
#
# Most test cases have fairly small files which are best included in
# the test case file itself as string variables, to make it easy to
# understand what is happening in the test. Files which are used
# in multiple test case files can be included in a common file, such as
# files.py or fixtures.py. Large files, such as seed files, which would
# just clutter the test file can be pulled in from 'data' subdirectories
# in the test directory.
#
# Test logs are written in the 'logs' directory in the root of the repo.
# Every test case writes to a log directory with the same 'prefix' as the
# test's unique schema.
#
# These fixture have "class" scope. Class scope fixtures can be used both
# in classes and in single test functions (which act as classes for this
# purpose). Pytest will collect all classes starting with 'Test', so if
# you have a class that you want to be subclassed, it's generally best to
# not start the class name with 'Test'. All standalone functions starting with
# 'test_' and methods in classes starting with 'test_' (in classes starting
# with 'Test') will be collected.
#
# Please see the pytest docs for further information:
#     https://docs.pytest.org
