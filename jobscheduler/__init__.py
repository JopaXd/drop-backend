import threading
import datetime
import logging
import sqlite3
import os
import pickle
import uuid
import queue

#Logging setup.
logger = logging.getLogger("scheduler")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = formatter = logging.Formatter('[%(levelname)s] - %(asctime)s - %(message)s', "%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

class SQLQuery:
	def __init__(self, query, args):
		self.id = str(uuid.uuid4())
		self.query = query
		self.args = args

class QueryResult:
	def __init__(self, query_id, result):
		self.query_id = query_id
		self.result = result

# Thread-safe wrapper of sqlite3
# Probably not done in the best possible way.
# But it works for now.
# Written by me :)

class SqliteWorker(threading.Thread):
	def __init__(self, file_name):
		threading.Thread.__init__(self, name=__name__)
		self.db_conn = sqlite3.connect(file_name, check_same_thread=False)
		self.cursor = self.db_conn.cursor()
		self.commands_queue = queue.Queue()
		self.select_results = []
		self.open = True
		self.start()

	def run(self):
		while True:
			if self.open:
				if not self.commands_queue.empty():
					sql_q = self.commands_queue.get()
					if "select" in sql_q.query.lower():
						try:
							if not sql_q.args is None:
								self.cursor.execute(sql_q.query, sql_q.args)
							else:
								self.cursor.execute(sql_q.query)
							results = self.cursor.fetchall()
							self.select_results.append(QueryResult(sql_q.id, results))
						except Exception as e:
							self.select_results.append(QueryResult(sql_q.id, e))
					else:
						try:
							if not sql_q.args is None:
								self.cursor.execute(sql_q.query, sql_q.args)
							else:
								self.cursor.execute(sql_q.query)
						except Exception as e:
							print(f"Error: {e}")
					self.db_conn.commit()
			else:
				self.db_conn.close()
				break

	def close(self):
		self.open = False

	def execute(self, query, args=None):
		q = SQLQuery(query, args)
		self.commands_queue.put(q)
		if "select" in query.lower():
			while True:
				q_result = next((x for x in self.select_results if x.query_id == q.id), None)
				if not q_result is None:
					break
			return q_result.result

class JobSchedulerException(Exception):
	pass

class Job:
	def __init__(self, function, function_args ,run_time, name):
		self.name = name
		self.function = function
		#This is a list of arguments that will be provided to the function.
		self.function_args = function_args
		#This is the time when the job should be executed.
		self.run_time = run_time
		#None if the job hasn't executed yet, True if it executed sucessfully and False if there was an error.
		self.executed_successfully = None
		#This contains the result of the error if execution failed.
		self.execution_error = None

	@property
	def is_pending(self):
		#If the time is exact or has passed, it is pending to execute.
		if self.run_time <= datetime.datetime.now():
			return True
		else:
			return False

	def __str__(self):
		return self.name

class RepetetiveJob:
	def __init__(self, function, function_args, time_to_repeat, name):
		self.name = name
		self.function = function
		#This is a list of arguments that will be provided to the function.
		self.function_args = function_args
		#Time to repeat the job after (in seconds).
		self.time_to_repeat = time_to_repeat
		#This is the time when the job should be executed.
		self.run_time = datetime.datetime.now() + datetime.timedelta(0, time_to_repeat)
		#None if the job hasn't executed yet, True if it executed sucessfully and False if there was an error.
		self.executed_successfully = None
		#This contains the result of the error if execution failed.
		self.execution_error = None

	@property
	def is_pending(self):
		#If the time is exact or has passed, it is pending to execute.
		if self.run_time <= datetime.datetime.now():
			return True
		else:
			return False

	def __str__(self):
		return self.name

class JobScheduler:
	def __init__(self, **kwargs):
		#If this is true. then the _job_handler thread stops,
		#therefore the application does not get stuck if the user wants to exit it.
		#close_scheduler sets it to true.
		self.is_closed = False
		#Logging
		self.logging = kwargs.get("logging", False)
		#If in_memory is enabled, the tasks a stored in memory, if false, the tasks are stored in an sqlite database.
		#The latter is recommended for production.
		self.in_memory = kwargs.get("in_memory", False)
		if self.in_memory == False:
			#A bit weird but we have to check if there is already a database or not.
			#sqlite3.connect() creates the so some code has to be repeated.
			if not os.path.exists("./jobs.db"):
				self.client = SqliteWorker("./jobs.db")
				#Create all the in_memory alternatives for the database
				self.client.execute("CREATE TABLE scheduled_jobs (id INTEGER PRIMARY KEY, job_name TEXT, job_obj BLOB);")
				self.client.execute("CREATE TABLE cancelled_jobs (id INTEGER PRIMARY KEY, job_name TEXT, job_obj BLOB);")
				self.client.execute("CREATE TABLE executed_jobs (id INTEGER PRIMARY KEY, job_name TEXT, job_obj BLOB);")
			else:
				self.client = SqliteWorker("./jobs.db")
		else:
			#In memory scheduled jobs storage.
			self.scheduled_jobs = []
			#Jobs that were cancelled.
			#In memory.
			self.cancelled_jobs = []
			#Jobs that were executed in history.
			#In memory.
			self.executed_jobs = []
		#Handler thread
		handler_thread = threading.Thread(target=self._job_handler)
		handler_thread.start()

	#Get schueduled jobs.
	def get_scheduled_jobs(self):
		if not self.in_memory:
			query = self.client.execute("SELECT job_obj FROM scheduled_jobs")
			return [pickle.loads(job[0]) for job in query]
		else:
			return self.scheduled_jobs

	#Get executed jobs.
	def get_executed_jobs(self):
		if not self.in_memory:
			query = self.client.execute("SELECT job_obj FROM executed_jobs")
			return [pickle.loads(job[0]) for job in query]
		else:
			return self.executed_jobs

	#Get cancelled jobs.
	def get_cancelled_jobs(self):
		if not self.in_memory:
			query = self.client.execute("SELECT job_obj FROM cancelled_jobs")
			return [pickle.loads(job[0]) for job in query]
		else:
			return self.cancelled_jobs

	#Close the scheduler. (Meant to kill the _job_handler thread)
	def close_scheduler(self):
		self.is_closed = True
		self.client.close()

	#This checks for jobs that are pending to be executed and runs them
	def _run_job(self, job):
		job.function(*job.function_args)

	def _job_handler(self):
		while True:
			if self.is_closed:
				break
			else:
				if self.in_memory:
					for index, job in enumerate(self.scheduled_jobs):
						if job.is_pending:
							try:
								self._run_job(job)
							except Exception as e:
								#Execution of the job failed due to an error in the job function.
								logger.error(f"Failed to execute {job.name} due to {type(e).__name__}!")
								job.executed_successfully = False
								job.execution_error = str(e)
								if isinstance(job, Job):
									self.scheduled_jobs.remove(job)
								elif isinstance(job, RepetetiveJob):
									#Since its a repetetive task, schedule another run.
									job.run_time = datetime.datetime.now() + datetime.timedelta(0, job.time_to_repeat)
									self.scheduled_jobs[index] = job
								#Save to executed_jobs with executed_successfully and execution_error defined.
								self.executed_jobs.append(job)
								continue
							if self.logging:
								logger.debug(f"Successfully executed {job.name}.")
							if isinstance(job, Job):
								self.scheduled_jobs.remove(job)
							elif isinstance(job, RepetetiveJob):
								job.run_time = datetime.datetime.now() + datetime.timedelta(0, job.time_to_repeat)
								self.scheduled_jobs[index] = job
							self.executed_jobs.append(job)
				else:
					scheduled_jobs_data = self.client.execute("SELECT job_obj FROM scheduled_jobs")
					sc_jobs = []
					for row in scheduled_jobs_data:
						sc_jobs.append(pickle.loads(row[0]))
					for job in sc_jobs:
						if job.is_pending:
							try:
								self._run_job(job)
							except Exception as e:
								logger.error(f"Failed to execute {job.name} due to {type(e).__name__}!")
								job.executed_successfully = False
								job.execution_error = str(e)
								if isinstance(job, Job):
									self.client.execute(f"DELETE FROM scheduled_jobs WHERE job_name = '{job.name}'")
								elif isinstance(job, RepetetiveJob):
									#Since its a repetetive task, schedule another run.
									job.run_time = datetime.datetime.now() + datetime.timedelta(0, job.time_to_repeat)
									self.client.execute(f"DELETE FROM scheduled_jobs WHERE job_name = '{job.name}'")
									self.client.execute("INSERT INTO scheduled_jobs(job_name, job_obj) VALUES(?,?)", (job.name, pickle.dumps(job)))
								self.client.execute("INSERT INTO executed_jobs(job_name, job_obj) VALUES(?,?)", (job.name, pickle.dumps(job)))
								
								continue
							if self.logging:
								logger.debug(f"Successfully executed {job.name}.")
							if isinstance(job, Job):
								self.client.execute(f"DELETE FROM scheduled_jobs WHERE job_name = '{job.name}'")
							elif isinstance(job, RepetetiveJob):
								job.run_time = datetime.datetime.now() + datetime.timedelta(0, job.time_to_repeat)
								self.client.execute(f"DELETE FROM scheduled_jobs WHERE job_name = '{job.name}'")
								self.client.execute("INSERT INTO scheduled_jobs(job_name, job_obj) VALUES(?,?)", (job.name, pickle.dumps(job)))
							self.client.execute(f"INSERT INTO executed_jobs(job_name, job_obj) VALUES(?, ?)", (job.name, pickle.dumps(job)))

	def schedule_job(self, job, job_arguments, time, name):
		#job if a function
		#Time is a datetime.datetime object
		if not isinstance(time, datetime.datetime):
			raise JobSchedulerException("Time must be a datetime.datetime object!")
		else:
			if datetime.datetime.now() > time:
				#We cant schedule a job to run in the past, therefore raise an error.
				raise JobSchedulerException("You cannot use a time that has already passed!")
			else:
				new_job = Job(job, job_arguments, time, name)
				if self.in_memory:
					self.scheduled_jobs.append(new_job)
				else:
					self.client.execute("INSERT INTO scheduled_jobs(job_name, job_obj) VALUES(?,?)", (name, pickle.dumps(new_job)))
					
	def seconds(self, sec):
		#Made the function for minutes and hours, figured why not seconds as well.
		return sec

	def minutes(self, min):
		#Return minutes in seconds
		return min * 60

	def hours(self, hrs):
		#Return hours in seconds.
		return hrs * 60 * 60

	def schedule_repetetive_job(self, job, job_arguments, time, name):
		#job if a function
		#Time is a datetime.datetime object	
		new_job = RepetetiveJob(job, job_arguments, time, name)
		if self.in_memory:
			self.scheduled_jobs.append(new_job)
		else:
			self.client.execute("INSERT INTO scheduled_jobs(job_name, job_obj) VALUES(?,?)", (name, pickle.dumps(new_job)))

	def cancel_job(self, job_name):
		if self.in_memory:
			if (job := next((x for x in self.scheduled_jobs if x.name == job_name), None)):
				self.scheduled_jobs.remove(job)
				self.cancelled_jobs.append(job)
			else:
				raise JobSchedulerException(f"No job with the name {job_name} found!")
		else:
			cancelled_job = self.client.execute(f"SELECT job_obj FROM scheduled_jobs WHERE job_name = '{job_name}'")[0][0]
			self.client.execute(f"DELETE FROM scheduled_jobs WHERE job_name = '{job_name}'")
			self.client.execute(f"INSERT INTO cancelled_jobs(job_name, job_obj) VALUES (?,?)", (job_name, cancelled_job))