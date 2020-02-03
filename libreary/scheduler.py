import logging
from crontab import CronTab

from libreary import Libreary

logger = logging.getLogger(__name__)

class Scheduler:
    """
    The LIBREary scheduler is responsible for scheduling various kinds of checks of LIBREarys

    It is meant to be interacted with by users. It currently uses cron jobs to schedule access.

    You must use one scheduler per LIBREary.

    This class currently contains the following methods:

    - set_schedule
    - verify_schedule
    - show_schedule
    """

    def __init__(self, config_dir: str):
        try:
        	self.libreary = Libreary(config_dir)
        	self.crontab  = CronTab(user=True)
        except Exception as e:
        	logger.error(f"Could not create Libreary Scheduler. Exception: {e}")

    def set_schedule(self, schedule: dict):
        pass

    def verify_schedule(self, schedule):
        pass

    def build_single_python_command(self, schedule_entry:dict):
    	"""
		Schedule entry format:

		```{json}
		{
			"config_dir": "Path to config_directory",
			"levels_to_check": ["list of levels to check"],
			"other_commands":["line here", "line here"],	 
		}
		```
    	"""
    	base_python_command = f"python3 -c from libreary import Libreary; l = Libreary('{config_dir}');"
    	for level in schedule_entry["levels_to_check"]:
    		base_python_command += f"l.check_level('{level}');"
    	base_python_command += ";".join(schedule_entry["other_commands"])

    def show_schedule():
    	pass
