# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""ASDB background job runner errors"""


class ASDBJobsError(ValueError):
    """Base class for all asdb-jobs errors"""


class ControlError(ASDBJobsError):
    """Base class for control-related errors"""


class InvalidControlName(ControlError):
    """Error raised when an invalid control name is used to query controls"""


class JobError(ASDBJobsError):
    """Base class for job-related errors"""

class InvalidJobData(JobError):
    """Error raised if the job data is invalid"""


class InvalidJobId(JobError):
    """Error raised when an invalid JobId is used to query jobs"""


class InvalidJobType(JobError):
    """Error raised on unknown job types"""


class JobConfict(JobError):
    """Error raised when the job has changed in the database unexpectedly"""
