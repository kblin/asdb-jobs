# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Logging configuration"""

import logging

core_logger = logging.getLogger("asdb-jobs.core")


def setup_logging():
    logging.basicConfig(format="%(levelname)-7s %(asctime)s    %(message)s",
                        level=logging.DEBUG, datefmt="%d/%m %H:%M:%S")