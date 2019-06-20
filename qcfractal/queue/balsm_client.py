"""
Client for interfacing with Balsam
"""

import balsam.launcher.dag as dag
from balsam.core.models import BalsamJob
from pydantic import BaseModel, validator
from qcelemental.util import which


class BalsamClient(BaseModel):
    """
    Configuration interface layer between Balsam and the Fractal Compute Manager interface

    Mock client-like object to hold information with respect to
    """
    name: str = "QCArchive Job"
    walltime: int = 6 * 60
    ranks_per_node: int = 1
    threads_per_core: int = 1
    _qcengine_app_name: str = "qcengine_by_fractal_manager"


    @validator("walltime", pre=True)
    def walltime_str_to_minutes(cls, v):
        if isinstance(v, str):
            #                    s  m  h   d
            converters_to_min = (0, 1, 60, 3600)
            split = tuple([int(x) for x in reversed(v.split(":"))])
            v = 0
            for multiplier, time_block in zip(converters_to_min, split):
                v += multiplier * time_block
        return v

    def __init__(self, fractal_add_engine_app: bool = True, **kwargs):
        """

        Parameters
        ----------
        fractal_add_engine_app
        kwargs : dict
            Additional keyword arguments to add to the
        """
        from balsam import setup as balsam_setup
        from balsam.core.models import ApplicationDefinition
        super().__init__(**kwargs)
        # Using the CLI type invocation of Balsam
        balsam_setup()
        if not ApplicationDefinition.objects.filter(name=self._application).exists():
            # Add app if it does not exist
            if not fractal_add_engine_app:
                raise RuntimeError(f"Unable to find Balsam App '{self._qcen_qcengine_app_namegine_name}' and the "
                                   f"`fractal_add_engine_app` was set to False. QCEngine will not be auto-added to the "
                                   f"Balsam database for safety")
            engine_path = which("qcengine", raise_error=True)
            engine_app = ApplicationDefinition
            engine_app.name = self._qcengine_name
            engine_app.description = "QC Engine as located bt the Fractal Manager"
            app.executable = py_app_path(args.executable)
            app.preprocess = py_app_path(args.preprocess)
            app.postprocess = py_app_path(args.postprocess)
            app.save()
            print(app)
            print("Added app to database")


# ===========================
# Debug and development lines DELETE BEFORE MERGING
from django.db import models
import uuid
from django.contrib.postgres.fields import JSONField

class BalsamJob(models.Model):
    ''' A DB representation of a Balsam Job '''

    job_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False)

    workflow = models.TextField(
        'Workflow Name',
        help_text='Name of the workflow to which this job belongs',
        default='')
    name = models.TextField(
        'Job Name',
        help_text='A name for the job given by the user.',
        default='')
    description = models.TextField(
        'Job Description',
        help_text='A description of the job.',
        default='')
    lock = models.TextField(
        'Process Lock',
        help_text='{hostname}:{PID} set by process that currently owns the job',
        default='',
        db_index=True
    )
    tick = models.DateTimeField(auto_now_add=True)

    parents = models.TextField(
        'IDs of the parent jobs which must complete prior to the start of this job.',
        default='[]')

    input_files = models.TextField(
        'Input File Patterns',
        help_text="Space-delimited filename patterns that will be searched in the parents'" \
                  "working directories. Every matching file will be made available in this" \
                  "job's working directory (symlinks for local Balsam jobs, file transfer for" \
                  "remote Balsam jobs). Default: all files from parent jobs are made available.",
        default='*')
    stage_in_url = models.TextField(
        'External stage in files or folders',
        help_text="A list of URLs for external data to be staged in prior to job processing. Job dataflow from parents to children is NOT handled here; see `input_files` field instead.",
        default='')
    stage_out_files = models.TextField(
        'External stage out files or folders',
        help_text="A string of filename patterns. Matches will be transferred to the stage_out_url. Default: no files are staged out",
        default='')
    stage_out_url = models.TextField(
        'Stage Out URL',
        help_text='The URLs to which designated stage out files are sent.',
        default='')

    wall_time_minutes = models.IntegerField(
        'Job Wall Time in Minutes',
        help_text='The number of minutes the job is expected to take',
        default=1)
    num_nodes = models.IntegerField(
        'Number of Compute Nodes',
        help_text='The number of compute nodes requested for this job.',
        default=1,
        db_index=True)
    coschedule_num_nodes = models.IntegerField(
        'Number of additional compute nodes to reserve alongside this job',
        help_text='''Used by Balsam service only.  If a pilot job runs on one or a
        few nodes, but requires additional worker nodes alongside it,
        use this field to specify the number of additional nodes that will be
        reserved by the service for this job.''',
        default=0)
    ranks_per_node = models.IntegerField(
        'Number of ranks per node',
        help_text='The number of MPI ranks per node to schedule for this job.',
        default=1)
    cpu_affinity = models.TextField(
        'Cray CPU Affinity ("depth" or "none")',
        default="none")
    threads_per_rank = models.IntegerField(
        'Number of threads per MPI rank',
        help_text='The number of OpenMP threads per MPI rank (if applicable)',
        default=1)
    threads_per_core = models.IntegerField(
        'Number of hyperthreads per physical core (if applicable)',
        help_text='Number of hyperthreads per physical core.',
        default=1)
    node_packing_count = models.IntegerField(
        'For serial (non-MPI) jobs only. How many to run concurrently on a node.',
        help_text='Setting this field at 2 means two serial jobs will run at a '
                  'time on a node. This field is ignored for MPI jobs.',
        default=1)
    environ_vars = models.TextField(
        'Environment variables specific to this job',
        help_text="Colon-separated list of envs like VAR1=value1:VAR2=value2",
        default='')

    application = models.TextField(
        'Application to Run',
        help_text='The application to run; located in Applications database',
        default='')
    args = models.TextField(
        'Command-line args to the application exe',
        help_text='Command line arguments used by the Balsam job runner',
        default='')
    user_workdir = models.TextField(
        'Override the Balsam-generated workdir, point to existing location',
        default=''
    )

    wait_for_parents = models.BooleanField(
        'If True, do not process this job until parents are FINISHED',
        default=True)
    post_error_handler = models.BooleanField(
        'Let postprocesser try to handle RUN_ERROR',
        help_text='If true, the postprocessor will be invoked for RUN_ERROR jobs'
                  ' and it is up to the script to handle error and update job state.',
        default=False)
    post_timeout_handler = models.BooleanField(
        'Let postprocesser try to handle RUN_TIMEOUT',
        help_text='If true, the postprocessor will be invoked for RUN_TIMEOUT jobs'
                  ' and it is up to the script to handle timeout and update job state.',
        default=False)
    auto_timeout_retry = models.BooleanField(
        'Automatically restart jobs that have timed out',
        help_text="If True and post_timeout_handler is False, then jobs will "
                  "simply be marked RESTART_READY upon timing out.",
        default=True)

    state = models.TextField(
        'Job State',
        help_text='The current state of the job.',
        default='CREATED',
        validators=[], #[validate_state],
        db_index=True)
    state_history = models.TextField(
        'Job State History',
        help_text="Chronological record of the job's states",
        default="history_line")

    queued_launch = models.ForeignKey(
        'QueuedLaunch',
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    data = JSONField('User Data', help_text="JSON encoded data store for user-defined data", default=dict)

# ===========================


class BalsamClient(object):
    """
    Configuration interface layer between Balsam and the Fractal Compute Manager interface
    """
    pass
