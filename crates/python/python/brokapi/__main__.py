"""``python -m brokapi new <folder> --broker <broker>``: see :mod:`brokapi.new`."""

import sys

from .new import main

sys.exit(main())
