.DEFAULT_GOAL := test

test:
	pylint tap_copper --disable missing-docstring,logging-format-interpolation,no-member,no-self-use,arguments-differ,too-few-public-methods
	nosetests
